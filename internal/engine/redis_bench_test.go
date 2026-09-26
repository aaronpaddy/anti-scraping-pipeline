package engine

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"telemetry-pipeline/internal/store"
)

// BenchmarkPrepareRedis runs stage 1 against a real Redis:
//
//	BENCH_REDIS_ADDR=localhost:6379 go test -run x -bench PrepareRedis ./internal/engine/
func BenchmarkPrepareRedis(b *testing.B) {
	addr := os.Getenv("BENCH_REDIS_ADDR")
	if addr == "" {
		b.Skip("BENCH_REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("BENCH_REDIS_PASSWORD")})
	st := store.NewRedis(rdb)
	batches := benchBatches(b.N)
	p := &Processor{Store: st, Workers: 4}
	ctx := context.Background()

	var load time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prep, err := p.Prepare(ctx, batches[i])
		if err != nil {
			b.Fatal(err)
		}
		load += prep.res.PrepareTime
	}
	b.ReportMetric(float64(load.Microseconds())/float64(b.N)/1000, "ms/prepare")

	// Time the read half alone on the (now warm) users of the last batch.
	var users []string
	for _, it := range mustPrepare(b, p, batches[b.N-1]).items {
		users = append(users, it.ev.ViewerUserID)
	}
	start := time.Now()
	const reps = 50
	var items int
	for i := 0; i < reps; i++ {
		states, _ := st.Load(ctx, users)
		items = 0
		for _, s := range states {
			items += len(s.Window.Items)
		}
	}
	b.ReportMetric(float64(time.Since(start).Microseconds())/reps/1000, "ms/load")
	b.ReportMetric(float64(items), "window-items/load")
}

func mustPrepare(b *testing.B, p *Processor, in []Input) *Prepared {
	prep, err := p.Prepare(context.Background(), in)
	if err != nil {
		b.Fatal(err)
	}
	return prep
}
