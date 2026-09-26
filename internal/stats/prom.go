package stats

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics, served on GET /metrics and shown in the Grafana
// dashboard (monitoring/grafana). They mirror the counters in Snapshot.
var (
	promEvents = promauto.NewCounter(prometheus.CounterOpts{
		Name: "engine_events_total", Help: "Events evaluated.",
	})
	promActions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "engine_decisions_total", Help: "Decisions by action.",
	}, []string{"action"})
	promRedelivered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "engine_redelivered_total", Help: "Events seen again after a redelivery or a duplicate in the batch.",
	})
	promBadRecords = promauto.NewCounter(prometheus.CounterOpts{
		Name: "engine_bad_records_total", Help: "Records that failed to decode.",
	})
	promVelocity = promauto.NewCounter(prometheus.CounterOpts{
		Name: "engine_velocity_triggered_total", Help: "Events that tripped the geographic velocity check.",
	})
	promAIScored = promauto.NewCounter(prometheus.CounterOpts{
		Name: "engine_ai_scored_total", Help: "Events scored by the AI engine.",
	})
	promBatches = promauto.NewCounter(prometheus.CounterOpts{
		Name: "engine_batches_total", Help: "Batches processed.",
	})
	promAIFallbacks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "engine_ai_fallback_batches_total", Help: "Batches decided on rules alone because the AI call timed out or failed.",
	})
	promLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "engine_event_latency_seconds",
		Help:    "Time from polling an event to its decision.",
		Buckets: []float64{.005, .01, .015, .02, .025, .03, .04, .05, .075, .1, .15, .25, .5, 1, 2.5, 5},
	})
	promStage = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "engine_stage_seconds",
		Help:    "Per-batch time in each stage.",
		Buckets: []float64{.0005, .001, .002, .004, .006, .008, .01, .015, .02, .03, .05, .1, .25},
	}, []string{"stage"})

	// PartitionsOwned is set by the consumer as partitions are assigned and revoked.
	PartitionsOwned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "engine_partitions_owned", Help: "Kafka partitions this engine currently owns.",
	})
)

func recordProm(r BatchResult) {
	promBatches.Inc()
	promEvents.Add(float64(r.Events))
	promRedelivered.Add(float64(r.Duplicates))
	promBadRecords.Add(float64(r.BadRecords))
	promVelocity.Add(float64(r.Velocity))
	promAIScored.Add(float64(r.AIScored))
	if r.AIFailed {
		promAIFallbacks.Inc()
	}
	for a, n := range r.Actions {
		promActions.WithLabelValues(a).Add(float64(n))
	}
	for _, d := range r.Latencies {
		promLatency.Observe(d.Seconds())
	}
	observeStage("prepare", r.PrepareTime)
	observeStage("ai", r.AITime)
	observeStage("publish", r.ProduceTime)
}

func observeStage(stage string, d time.Duration) {
	if d > 0 {
		promStage.WithLabelValues(stage).Observe(d.Seconds())
	}
}
