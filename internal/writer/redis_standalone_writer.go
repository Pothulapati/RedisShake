package writer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

type RedisWriterOptions struct {
	Cluster   bool                   `mapstructure:"cluster" default:"false"`
	Address   string                 `mapstructure:"address" default:""`
	Username  string                 `mapstructure:"username" default:""`
	Password  string                 `mapstructure:"password" default:""`
	Tls       bool                   `mapstructure:"tls" default:"false"`
	TlsConfig client.TlsConfig       `mapstructure:"tls_config" default:"{}"`
	OffReply  bool                   `mapstructure:"off_reply" default:"false"`
	Sentinel  client.SentinelOptions `mapstructure:"sentinel"`
}

type redisClient struct {
	client *client.Redis
	dbId   int // Track DB state per connection
	// Per-connection reply channel
	chWaitReply chan *entry.Entry
	chWaitWg    sync.WaitGroup
}

type redisStandaloneWriter struct {
	address string
	// Pool of connections
	clients []*redisClient
	// Number of connections in the pool
	numConnections int
	// Channels for each connection to maintain order
	cmdChannels []chan *entry.Entry

	offReply bool
	ch       chan *entry.Entry
	chWg     sync.WaitGroup

	stat struct {
		Name              string `json:"name"`
		UnansweredBytes   int64  `json:"unanswered_bytes"`
		UnansweredEntries int64  `json:"unanswered_entries"`
	}
}

// getConnectionIndex returns consistent connection index for a given key
func getConnectionIndex(key string, numConnections int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numConnections
}

func NewRedisStandaloneWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	rw := new(redisStandaloneWriter)
	rw.address = opts.Address
	rw.stat.Name = "writer_" + strings.Replace(opts.Address, ":", "_", -1)

	// Initialize connection pool - use 8 connections
	rw.numConnections = 8
	rw.clients = make([]*redisClient, rw.numConnections)
	rw.cmdChannels = make([]chan *entry.Entry, rw.numConnections)

	for i := 0; i < rw.numConnections; i++ {
		client := client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
		if opts.OffReply {
			client.Send("CLIENT", "REPLY", "OFF")
		}

		rc := &redisClient{
			client: client,
			dbId:   0, // Start with DB 0
		}

		if !opts.OffReply {
			rc.chWaitReply = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit)
			rc.chWaitWg.Add(1)
			go rw.processReply(rc)
		}

		rw.clients[i] = rc
		// Create channel for each connection with standard buffer size
		rw.cmdChannels[i] = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit)
	}

	rw.ch = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit)
	if opts.OffReply {
		log.Infof("turn off the reply of write")
		rw.offReply = true
	}
	return rw
}

func (w *redisStandaloneWriter) Close() {
	close(w.ch)
	w.chWg.Wait()

	// Close all command channels
	for _, ch := range w.cmdChannels {
		close(ch)
	}

	// Close all reply channels and wait for processors
	if !w.offReply {
		for _, c := range w.clients {
			close(c.chWaitReply)
			c.chWaitWg.Wait()
		}
	}

	// Close all Redis connections
	for _, c := range w.clients {
		c.client.Close()
	}
}

func (w *redisStandaloneWriter) StartWrite(ctx context.Context) chan *entry.Entry {
	w.chWg = sync.WaitGroup{}
	w.chWg.Add(w.numConnections)

	// Start the command distributor
	go w.distributeCommands(ctx)

	// Start processors for each connection
	for i := 0; i < w.numConnections; i++ {
		go w.processWrite(ctx, w.clients[i], w.cmdChannels[i])
	}
	return w.ch
}

func (w *redisStandaloneWriter) Write(e *entry.Entry) {
	w.ch <- e
}

// distributeCommands reads from main channel and routes to appropriate connection channel
func (w *redisStandaloneWriter) distributeCommands(ctx context.Context) {
	for {
		select {
		case e, ok := <-w.ch:
			if !ok {
				// Close all command channels when main channel is closed
				for _, ch := range w.cmdChannels {
					close(ch)
				}
				return
			}
			// Parse command to get keys if not already parsed
			if len(e.Keys) == 0 {
				e.Parse()
			}

			var connIndex int
			if len(e.Keys) == 0 {
				// Round-robin for commands without keys (like PING)
				idx := atomic.AddInt64(&w.stat.UnansweredEntries, 1) % int64(w.numConnections)
				connIndex = int(idx)
			} else {
				// Use the first key for routing to maintain order for multi-key operations
				connIndex = getConnectionIndex(e.Keys[0], w.numConnections)
			}

			w.cmdChannels[connIndex] <- e

		case <-ctx.Done():
			// Context cancelled, clean up and exit
			for _, ch := range w.cmdChannels {
				close(ch)
			}
			return
		}
	}
}

func (w *redisStandaloneWriter) processWrite(ctx context.Context, c *redisClient, cmdChan chan *entry.Entry) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// do nothing until channel is closed
		case <-ticker.C:
			c.client.Flush()
		case e, ok := <-cmdChan:
			if !ok {
				// clean up and exit
				c.client.Flush()
				w.chWg.Done()
				return
			}
			// switch db if needed for this connection
			if c.dbId != e.DbId {
				log.Debugf("[%s] switch db to [%d]", w.stat.Name, e.DbId)
				c.client.Send("select", strconv.Itoa(e.DbId))
				if !w.offReply {
					c.chWaitReply <- &entry.Entry{
						Argv:    []string{"select", strconv.Itoa(e.DbId)},
						CmdName: "select",
					}
				}
				c.dbId = e.DbId
			}

			// send using this connection
			bytes := e.Serialize()
			for e.SerializedSize+atomic.LoadInt64(&w.stat.UnansweredBytes) > config.Opt.Advanced.TargetRedisClientMaxQuerybufLen {
				time.Sleep(1 * time.Nanosecond)
			}
			log.Debugf("[%s] send cmd. cmd=[%s]", w.stat.Name, e.String())
			if !w.offReply {
				c.chWaitReply <- e
				atomic.AddInt64(&w.stat.UnansweredBytes, e.SerializedSize)
				atomic.AddInt64(&w.stat.UnansweredEntries, 1)
			}
			c.client.SendBytesBuff(bytes)
		}
	}
}

func (w *redisStandaloneWriter) processReply(c *redisClient) {
	for e := range c.chWaitReply {
		reply, err := c.client.Receive()
		log.Debugf("[%s] receive reply. reply=[%v], cmd=[%s]", w.stat.Name, reply, e.String())

		// It's good to skip the nil error since some write commands will return the null reply. For example,
		// the SET command with NX option will return nil if the key already exists.
		if err != nil && !errors.Is(err, proto.Nil) {
			if err.Error() == "BUSYKEY Target key name already exists." {
				if config.Opt.Advanced.RDBRestoreCommandBehavior == "skip" {
					log.Debugf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				} else if config.Opt.Advanced.RDBRestoreCommandBehavior == "panic" {
					log.Panicf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				}
			} else {
				log.Panicf("[%s] receive reply failed. cmd=[%s], error=[%v]", w.stat.Name, e.String(), err)
			}
		}

		if strings.EqualFold(e.CmdName, "select") { // skip select command
			continue
		}

		atomic.AddInt64(&w.stat.UnansweredBytes, -e.SerializedSize)
		atomic.AddInt64(&w.stat.UnansweredEntries, -1)
	}
	c.chWaitWg.Done()
}

func (w *redisStandaloneWriter) Status() interface{} {
	return w.stat
}

func (w *redisStandaloneWriter) StatusString() string {
	return fmt.Sprintf("[%s]: unanswered_entries=%d", w.stat.Name, atomic.LoadInt64(&w.stat.UnansweredEntries))
}

func (w *redisStandaloneWriter) StatusConsistent() bool {
	return atomic.LoadInt64(&w.stat.UnansweredBytes) == 0 && atomic.LoadInt64(&w.stat.UnansweredEntries) == 0
}
