package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"telemetry-pipeline/internal/stats"
	"telemetry-pipeline/internal/store"
)

type ConsumerConfig struct {
	Brokers     []string
	Group       string
	InTopic     string
	OutTopic    string
	BatchSize   int
	BatchWindow time.Duration
}

type tp struct {
	topic string
	part  int32
}

type partWorker struct {
	in   chan chunk
	done chan struct{}
}

// Consumer runs one batching pipeline per assigned partition. A user's events
// always land on one partition, so per-user ordering holds end to end.
// Offsets are marked only after a batch's alerts are acknowledged
// (at-least-once); see Processor for how redelivered events are handled.
type Consumer struct {
	cfg   ConsumerConfig
	proc  *Processor
	stats *stats.Stats
	log   *slog.Logger
	cl    *kgo.Client
	ctx   context.Context

	mu    sync.Mutex
	parts map[tp]*partWorker
}

func NewConsumer(ctx context.Context, cfg ConsumerConfig, proc *Processor, st *stats.Stats, log *slog.Logger) (*Consumer, error) {
	c := &Consumer{cfg: cfg, proc: proc, stats: st, log: log, ctx: ctx, parts: map[tp]*partWorker{}}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.InTopic),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.assigned),
		kgo.OnPartitionsRevoked(c.revoked),
		kgo.OnPartitionsLost(c.lost),
		kgo.FetchMaxWait(50*time.Millisecond),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

// Run polls until ctx is canceled, then drains workers and commits.
func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			// Leaving the group waits on the rebalance block from this poll.
			c.cl.AllowRebalance()
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.log.Warn("fetch error", "topic", t, "partition", p, "err", err)
		})
		now := time.Now()
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			c.mu.Lock()
			w := c.parts[tp{p.Topic, p.Partition}]
			c.mu.Unlock()
			if w != nil {
				w.in <- chunk{recs: p.Records, at: now}
			}
		})
		c.cl.AllowRebalance()
	}

	c.mu.Lock()
	all := make(map[string][]int32)
	for k := range c.parts {
		all[k.topic] = append(all[k.topic], k.part)
	}
	c.mu.Unlock()
	c.stop(all)
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.cl.CommitMarkedOffsets(cctx); err != nil {
		c.log.Warn("final commit failed", "err", err)
	}
	c.cl.Close()
}

func (c *Consumer) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for topic, parts := range assigned {
		for _, p := range parts {
			w := &partWorker{in: make(chan chunk, 16), done: make(chan struct{})}
			c.parts[tp{topic, p}] = w
			// Each partition gets its own state cache: its users are owned
			// by this worker until the partition is revoked.
			proc := *c.proc
			proc.Store = store.NewCached(c.proc.Store)
			go c.runPartition(w, &proc)
			c.log.Info("partition assigned", "topic", topic, "partition", p)
		}
	}
}

// preparedBatch is a batch between the two stages.
type preparedBatch struct {
	recs []*kgo.Record
	prep *Prepared
}

// runPartition runs a partition's two stages. Stage 1 (Prepare) runs batches
// strictly in order; stage 2 (AI, publish, commit) runs one batch behind it,
// also in order, so batch N+1's Redis work overlaps batch N's AI call.
func (c *Consumer) runPartition(w *partWorker, proc *Processor) {
	defer close(w.done)
	stage2 := make(chan preparedBatch, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for b := range stage2 {
			c.finish(proc, b)
		}
	}()
	batchLoop(w.in, c.cfg.BatchSize, c.cfg.BatchWindow, func(recs []*kgo.Record, at []time.Time) {
		inputs := make([]Input, len(recs))
		for i, r := range recs {
			inputs[i] = Input{Value: r.Value, Received: at[i]}
		}
		var prep *Prepared
		if !c.retry("prepare batch", func() (err error) {
			prep, err = proc.Prepare(c.ctx, inputs)
			return err
		}) {
			return
		}
		stage2 <- preparedBatch{recs: recs, prep: prep}
	})
	close(stage2)
	<-finished
}

// finish scores and decides a prepared batch, publishes its alerts, and marks
// its offsets for commit. On shutdown mid-retry the batch is left uncommitted
// and is redelivered.
func (c *Consumer) finish(proc *Processor, b preparedBatch) {
	alerts, res := proc.Finish(c.ctx, b.prep)
	start := time.Now()
	out := make([]*kgo.Record, len(alerts))
	for i, a := range alerts {
		v, _ := json.Marshal(a)
		out[i] = &kgo.Record{Topic: c.cfg.OutTopic, Key: []byte(a.ViewerUserID), Value: v}
	}
	if len(out) > 0 && !c.retry("produce alerts", func() error {
		return c.cl.ProduceSync(c.ctx, out...).FirstErr()
	}) {
		return
	}
	res.ProduceTime = time.Since(start)
	c.stats.Record(res)
	c.cl.MarkCommitRecords(b.recs...)
}

func (c *Consumer) revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	c.stop(revoked)
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		c.log.Warn("commit on revoke failed", "err", err)
	}
}

func (c *Consumer) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.stop(lost)
}

// stop closes the given partitions' workers and waits for in-flight batches.
func (c *Consumer) stop(parts map[string][]int32) {
	var ws []*partWorker
	c.mu.Lock()
	for topic, ps := range parts {
		for _, p := range ps {
			k := tp{topic, p}
			if w, ok := c.parts[k]; ok {
				delete(c.parts, k)
				close(w.in)
				ws = append(ws, w)
			}
		}
	}
	c.mu.Unlock()
	for _, w := range ws {
		<-w.done
	}
}

func (c *Consumer) retry(what string, f func() error) bool {
	backoff := 50 * time.Millisecond
	for {
		err := f()
		if err == nil {
			return true
		}
		if c.ctx.Err() != nil {
			return false
		}
		c.log.Error(what+" failed, retrying", "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-c.ctx.Done():
			return false
		}
		backoff = min(2*backoff, 5*time.Second)
	}
}
