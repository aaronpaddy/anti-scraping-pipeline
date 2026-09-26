// Package stats tracks engine counters and processing latency.
package stats

import (
	"math"
	"sync"
	"time"
)

const (
	bucketWidth = 100 * time.Microsecond
	numBuckets  = 10_000 // up to 1s; slower samples land in the last bucket
)

// Histogram is a fixed-width latency histogram (100µs buckets).
type Histogram struct {
	counts [numBuckets]uint64
	total  uint64
	max    time.Duration
}

func (h *Histogram) Observe(d time.Duration) {
	i := int(d / bucketWidth)
	if i >= numBuckets {
		i = numBuckets - 1
	}
	if i < 0 {
		i = 0
	}
	h.counts[i]++
	h.total++
	h.max = max(h.max, d)
}

// Quantile returns the upper edge of the bucket holding quantile q, in ms.
func (h *Histogram) Quantile(q float64) float64 {
	if h.total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.total)))
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= rank {
			return float64(time.Duration(i+1)*bucketWidth) / float64(time.Millisecond)
		}
	}
	return float64(h.max) / float64(time.Millisecond)
}

// Snapshot is a point-in-time view of the engine's counters.
type Snapshot struct {
	Events       uint64            `json:"events"`
	Duplicates   uint64            `json:"duplicates"`
	BadRecords   uint64            `json:"bad_records"`
	Batches      uint64            `json:"batches"`
	AIScored     uint64            `json:"ai_scored"`
	AIFailures   uint64            `json:"ai_failed_batches"`
	Velocity     uint64            `json:"velocity_triggered"`
	Actions      map[string]uint64 `json:"actions"`
	LatencyP50Ms float64           `json:"latency_p50_ms"`
	LatencyP99Ms float64           `json:"latency_p99_ms"`
	LatencyMaxMs float64           `json:"latency_max_ms"`
	// Average per-batch stage times.
	AvgPrepareMs float64 `json:"avg_prepare_ms"`
	AvgAIMs      float64 `json:"avg_ai_ms"`
	AvgProduceMs float64 `json:"avg_produce_ms"`
}

type stageTimes struct {
	batches, aiCalls     uint64
	prepare, ai, produce time.Duration
}

// Stats is safe for concurrent use. Latency is tracked both since start and for
// the current reporting interval.
type Stats struct {
	mu        sync.Mutex
	snap      Snapshot
	all       Histogram
	interval  Histogram
	stagesAll stageTimes
	stagesInt stageTimes
}

func New() *Stats { return &Stats{snap: Snapshot{Actions: map[string]uint64{}}} }

// BatchResult is what one processed batch adds to the counters.
type BatchResult struct {
	Events, Duplicates, BadRecords, AIScored, Velocity uint64
	AIFailed                                           bool
	Actions                                            map[string]uint64
	Latencies                                          []time.Duration
	PrepareTime, AITime, ProduceTime                   time.Duration
}

func (s *Stats) Record(r BatchResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap.Batches++
	s.snap.Events += r.Events
	s.snap.Duplicates += r.Duplicates
	s.snap.BadRecords += r.BadRecords
	s.snap.AIScored += r.AIScored
	s.snap.Velocity += r.Velocity
	if r.AIFailed {
		s.snap.AIFailures++
	}
	for a, n := range r.Actions {
		s.snap.Actions[a] += n
	}
	for _, d := range r.Latencies {
		s.all.Observe(d)
		s.interval.Observe(d)
	}
	for _, st := range []*stageTimes{&s.stagesAll, &s.stagesInt} {
		st.batches++
		st.prepare += r.PrepareTime
		st.produce += r.ProduceTime
		if r.AITime > 0 {
			st.aiCalls++
			st.ai += r.AITime
		}
	}
}

func avgMs(d time.Duration, n uint64) float64 {
	if n == 0 {
		return 0
	}
	return float64(d) / float64(n) / float64(time.Millisecond)
}

func (s *Stats) snapshot(h *Histogram, st *stageTimes) Snapshot {
	out := s.snap
	out.Actions = make(map[string]uint64, len(s.snap.Actions))
	for k, v := range s.snap.Actions {
		out.Actions[k] = v
	}
	out.LatencyP50Ms = h.Quantile(0.50)
	out.LatencyP99Ms = h.Quantile(0.99)
	out.LatencyMaxMs = float64(h.max) / float64(time.Millisecond)
	out.AvgPrepareMs = avgMs(st.prepare, st.batches)
	out.AvgAIMs = avgMs(st.ai, st.aiCalls)
	out.AvgProduceMs = avgMs(st.produce, st.batches)
	return out
}

// Reset clears all counters and latency, e.g. at the start of a load test.
func (s *Stats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = Snapshot{Actions: map[string]uint64{}}
	s.all, s.interval = Histogram{}, Histogram{}
	s.stagesAll, s.stagesInt = stageTimes{}, stageTimes{}
}

// Total returns counters and latency since start (or the last Reset).
func (s *Stats) Total() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot(&s.all, &s.stagesAll)
}

// Interval returns counters since start and latency since the last call, then
// resets the interval latency.
func (s *Stats) Interval() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.snapshot(&s.interval, &s.stagesInt)
	s.interval = Histogram{}
	s.stagesInt = stageTimes{}
	return out
}
