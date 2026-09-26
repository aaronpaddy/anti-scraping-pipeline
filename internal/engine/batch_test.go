package engine

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func recs(n int, start int64) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := range out {
		out[i] = &kgo.Record{Offset: start + int64(i)}
	}
	return out
}

func TestBatchLoopSplitsBySize(t *testing.T) {
	in := make(chan chunk, 4)
	var sizes []int
	var offsets []int64
	done := make(chan struct{})
	go func() {
		batchLoop(in, 100, time.Hour, func(r []*kgo.Record, _ []time.Time) {
			sizes = append(sizes, len(r))
			for _, x := range r {
				offsets = append(offsets, x.Offset)
			}
		})
		close(done)
	}()
	in <- chunk{recs: recs(250, 0), at: time.Now()}
	in <- chunk{recs: recs(30, 250), at: time.Now()}
	close(in)
	<-done
	if len(sizes) != 3 || sizes[0] != 100 || sizes[1] != 100 || sizes[2] != 80 {
		t.Fatalf("batch sizes %v", sizes)
	}
	for i, o := range offsets {
		if o != int64(i) {
			t.Fatalf("records out of order at %d", i)
		}
	}
}

func TestBatchLoopFlushesOnWindow(t *testing.T) {
	in := make(chan chunk)
	got := make(chan int, 1)
	go batchLoop(in, 100, 10*time.Millisecond, func(r []*kgo.Record, _ []time.Time) { got <- len(r) })
	start := time.Now()
	in <- chunk{recs: recs(3, 0), at: start}
	select {
	case n := <-got:
		if n != 3 || time.Since(start) < 10*time.Millisecond {
			t.Fatalf("flushed %d after %v", n, time.Since(start))
		}
	case <-time.After(time.Second):
		t.Fatal("partial batch never flushed")
	}
	close(in)
}
