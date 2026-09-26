// Package engine is the Go evaluation engine (spec §4.3): it turns batches of
// raw events into decisions, and consumes/produces them through Kafka.
package engine

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"sync"
	"time"

	"anti-scraping-pipeline/internal/detect"
	"anti-scraping-pipeline/internal/event"
	"anti-scraping-pipeline/internal/stats"
	"anti-scraping-pipeline/internal/store"
)

// Input is one raw record and the time the engine received it.
type Input struct {
	Value    []byte
	Received time.Time
}

// Processor evaluates batches in two stages so a partition can overlap them:
//
//  1. Prepare: decode, load state, run the rules, save state. Must run in
//     batch order; the next batch's Prepare sees this state.
//  2. Finish: call the AI engine and decide.
//
// Delivery is at-least-once. A redelivered event (one still in its user's
// activity window) reuses its stored velocity result without touching state,
// and is scored and published again, so a crash between publishing alerts and
// committing offsets can repeat an alert but never loses or re-applies one.
type Processor struct {
	Store     store.Store
	Scorer    Scorer // nil runs rules only
	AITimeout time.Duration
	Workers   int
	Now       func() time.Time
}

type item struct {
	ev          event.Telemetry
	received    time.Time
	velocity    bool
	features    []float32
	redelivered bool
}

// Prepared is a batch that has been through stage 1.
type Prepared struct {
	items []*item
	res   stats.BatchResult
}

// Process runs both stages (tests, offline runs).
func (p *Processor) Process(ctx context.Context, batch []Input) ([]event.Alert, stats.BatchResult, error) {
	prep, err := p.Prepare(ctx, batch)
	if err != nil {
		return nil, stats.BatchResult{}, err
	}
	alerts, res := p.Finish(ctx, prep)
	return alerts, res, nil
}

// Prepare is stage 1.
func (p *Processor) Prepare(ctx context.Context, batch []Input) (*Prepared, error) {
	start := time.Now()
	prep := &Prepared{res: stats.BatchResult{Actions: map[string]uint64{}}}
	res := &prep.res

	// Decode, dropping malformed records and duplicates within the batch.
	items := make([]*item, 0, len(batch))
	inBatch := make(map[string]bool, len(batch))
	for _, in := range batch {
		var ev event.Telemetry
		if err := json.Unmarshal(in.Value, &ev); err != nil || ev.EventID == "" || ev.ViewerUserID == "" {
			res.BadRecords++
			continue
		}
		if inBatch[ev.EventID] {
			res.Duplicates++
			continue
		}
		inBatch[ev.EventID] = true
		items = append(items, &item{ev: ev, received: in.Received})
	}
	if len(items) == 0 {
		return prep, nil
	}

	var users []string
	byUser := map[string][]*item{}
	for _, it := range items {
		u := it.ev.ViewerUserID
		if byUser[u] == nil {
			users = append(users, u)
		}
		byUser[u] = append(byUser[u], it)
	}
	states, err := p.Store.Load(ctx, users)
	if err != nil {
		return nil, err
	}

	// Shard users over workers by hash so one user's events are always
	// evaluated by one goroutine, in order.
	workers := max(1, p.Workers)
	shards := make([][]string, workers)
	for u := range byUser {
		s := shardOf(u, workers)
		shards[s] = append(shards[s], u)
	}
	updates := make([][]store.Update, workers)
	var wg sync.WaitGroup
	for w := range shards {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for _, u := range shards[w] {
				st := states[u]
				up := store.Update{User: u}
				for _, it := range byUser[u] {
					r := st.Evaluate(&it.ev)
					it.velocity, it.features, it.redelivered = r.Velocity, r.Features, r.Redelivered
					if !r.Redelivered {
						up.Added = append(up.Added, r.Added)
					}
				}
				up.Session = st.Session
				updates[w] = append(updates[w], up)
			}
		}(w)
	}
	wg.Wait()

	// Persist state before the AI call so the next batch sees it.
	var all []store.Update
	for _, u := range updates {
		all = append(all, u...)
	}
	if err := p.Store.Save(ctx, all); err != nil {
		return nil, err
	}
	for _, it := range items {
		if it.redelivered {
			res.Duplicates++
		}
	}
	prep.items = items
	res.PrepareTime = time.Since(start)
	return prep, nil
}

// Finish is stage 2: score with the AI engine and decide. It never fails; if
// the AI engine is slow or down the batch is decided on the rules alone.
func (p *Processor) Finish(ctx context.Context, prep *Prepared) ([]event.Alert, stats.BatchResult) {
	res := prep.res
	if len(prep.items) == 0 {
		return nil, res
	}
	scores := p.score(ctx, prep.items, &res)

	now := p.now()
	var alerts []event.Alert
	for _, it := range prep.items {
		score, has := scores[it.ev.EventID]
		action := detect.Decide(it.velocity, score, has)
		res.Events++
		res.Actions[string(action)]++
		if it.velocity {
			res.Velocity++
		}
		latency := now.Sub(it.received)
		res.Latencies = append(res.Latencies, latency)
		if action == event.ActionAllow {
			continue
		}
		a := event.Alert{
			EventID:                     it.ev.EventID,
			ViewerUserID:                it.ev.ViewerUserID,
			EventTimestamp:              it.ev.Timestamp,
			EvaluatedAt:                 now.UnixMilli(),
			GeographicVelocityTriggered: it.velocity,
			AIScoreAvailable:            has,
			ActionTaken:                 action,
			ProcessingDurationMs:        float64(latency.Microseconds()) / 1000,
		}
		if has {
			s := score
			a.AnomalyScore = &s
		}
		alerts = append(alerts, a)
	}
	return alerts, res
}

// score calls the AI engine for events with enough history. On timeout or
// error it returns no scores and the batch falls back to the rules alone.
func (p *Processor) score(ctx context.Context, items []*item, res *stats.BatchResult) map[string]float64 {
	if p.Scorer == nil {
		return nil
	}
	var req []event.ScoreItem
	for _, it := range items {
		if it.features != nil {
			req = append(req, event.ScoreItem{EventID: it.ev.EventID, ViewerUserID: it.ev.ViewerUserID, Features: it.features})
		}
	}
	if len(req) == 0 {
		return nil
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, p.AITimeout)
	defer cancel()
	scores, err := p.Scorer.Score(ctx, req)
	res.AITime = time.Since(start)
	if err != nil {
		res.AIFailed = true
		return nil
	}
	res.AIScored += uint64(len(scores))
	return scores
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func shardOf(user string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(user))
	return int(h.Sum32() % uint32(n))
}
