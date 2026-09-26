package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"anti-scraping-pipeline/internal/gen"
	"anti-scraping-pipeline/internal/store"
)

func benchBatches(n int) [][]Input {
	sim := gen.New(gen.Config{Seed: 3, Users: 50000, ScraperPct: 0.05, TeleporterPct: 0.02, StartMs: 1_790_000_000_000})
	out := make([][]Input, n)
	for i := range out {
		for j := 0; j < 100; j++ {
			ev, _ := sim.Next()
			b, _ := json.Marshal(ev)
			out[i] = append(out[i], Input{Value: b, Received: time.Now()})
		}
	}
	return out
}

func benchProcess(b *testing.B, st store.Store) {
	batches := benchBatches(b.N)
	p := &Processor{Store: st, Workers: 4}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := p.Process(context.Background(), batches[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N)/1000, "ms/batch")
}

func BenchmarkProcessMemory(b *testing.B) { benchProcess(b, store.NewMemory()) }

func BenchmarkProcessMiniredis(b *testing.B) {
	mr := miniredis.RunT(b)
	benchProcess(b, store.NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr()})))
}
