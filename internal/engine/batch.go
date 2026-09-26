package engine

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// chunk is the records one poll returned for a partition.
type chunk struct {
	recs []*kgo.Record
	at   time.Time
}

// batchLoop groups incoming records into batches of at most size, flushing a
// partial batch once window has passed since its first record. It flushes what
// is left and returns when in is closed.
func batchLoop(in <-chan chunk, size int, window time.Duration, flush func([]*kgo.Record, []time.Time)) {
	var recs []*kgo.Record
	var at []time.Time
	timer := time.NewTimer(window)
	timer.Stop()
	emit := func() {
		timer.Stop()
		for len(recs) > 0 {
			n := min(size, len(recs))
			flush(recs[:n:n], at[:n:n])
			recs, at = recs[n:], at[n:]
		}
		recs, at = nil, nil
	}
	add := func(c chunk) {
		if len(recs) == 0 {
			timer.Reset(window)
		}
		for _, r := range c.recs {
			recs = append(recs, r)
			at = append(at, c.at)
			if len(recs) == size {
				flush(recs, at)
				recs, at = nil, nil
				timer.Reset(window)
			}
		}
		if len(recs) == 0 {
			timer.Stop()
		}
	}
	for {
		select {
		case c, ok := <-in:
			if !ok {
				emit()
				return
			}
			add(c)
		case <-timer.C:
			emit()
		}
	}
}
