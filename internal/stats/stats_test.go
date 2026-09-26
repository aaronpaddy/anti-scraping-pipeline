package stats

import (
	"testing"
	"time"
)

func TestQuantiles(t *testing.T) {
	var h Histogram
	for i := 1; i <= 100; i++ {
		h.Observe(time.Duration(i) * time.Millisecond)
	}
	if p50 := h.Quantile(0.5); p50 < 50 || p50 > 50.2 {
		t.Fatalf("p50 = %v", p50)
	}
	if p99 := h.Quantile(0.99); p99 < 99 || p99 > 99.2 {
		t.Fatalf("p99 = %v", p99)
	}
	h.Observe(5 * time.Second)
	if h.Quantile(1) != 1000 {
		t.Fatalf("overflow bucket = %v", h.Quantile(1))
	}
}

func TestIntervalResets(t *testing.T) {
	s := New()
	s.Record(BatchResult{Events: 2, Latencies: []time.Duration{time.Millisecond, 2 * time.Millisecond}})
	if s.Interval().LatencyMaxMs != 2 {
		t.Fatal("interval should see latencies")
	}
	if got := s.Interval(); got.LatencyMaxMs != 0 || got.Events != 2 {
		t.Fatalf("after reset: %+v", got)
	}
	if s.Total().LatencyMaxMs != 2 {
		t.Fatal("total should keep latencies")
	}
}
