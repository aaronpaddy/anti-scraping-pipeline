// Command engine is the Go evaluation engine (spec §4.3).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"anti-scraping-pipeline/internal/cli"
	"anti-scraping-pipeline/internal/engine"
	"anti-scraping-pipeline/internal/event"
	"anti-scraping-pipeline/internal/stats"
	"anti-scraping-pipeline/internal/store"
)

func main() {
	brokers := flag.String("brokers", cli.Env("KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers, comma separated")
	group := flag.String("group", cli.Env("CONSUMER_GROUP", "evaluation-engine"), "consumer group")
	redisAddr := flag.String("redis-addr", cli.Env("REDIS_ADDR", "localhost:6379"), "Redis address")
	redisPass := flag.String("redis-password", cli.Env("REDIS_PASSWORD", "dev_pipeline_pass_99"), "Redis password")
	aiURL := flag.String("ai-url", cli.Env("AI_URL", "http://localhost:8000"), "AI engine base URL; empty runs rules only")
	aiTimeout := flag.Duration("ai-timeout", cli.EnvDuration("AI_TIMEOUT", 20*time.Millisecond), "AI engine timeout per batch")
	batchSize := flag.Int("batch-size", cli.EnvInt("BATCH_SIZE", 100), "max events per batch")
	batchWindow := flag.Duration("batch-window", cli.EnvDuration("BATCH_WINDOW", 10*time.Millisecond), "max wait to fill a batch")
	workers := flag.Int("workers", cli.EnvInt("WORKERS", 4), "worker goroutines per batch")
	statsAddr := flag.String("stats-addr", cli.Env("STATS_ADDR", ":9100"), "address for GET /stats")
	statsEvery := flag.Duration("stats-interval", cli.EnvDuration("STATS_INTERVAL", 10*time.Second), "how often to log stats")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, Password: *redisPass, PoolSize: 64})
	proc := &engine.Processor{Store: store.NewRedis(rdb), AITimeout: *aiTimeout, Workers: *workers}
	if *aiURL != "" {
		proc.Scorer = engine.NewHTTPScorer(*aiURL)
	}
	st := stats.New()

	c, err := engine.NewConsumer(ctx, engine.ConsumerConfig{
		Brokers:     cli.Split(*brokers),
		Group:       *group,
		InTopic:     event.ClickstreamTopic,
		OutTopic:    event.AlertsTopic,
		BatchSize:   *batchSize,
		BatchWindow: *batchWindow,
	}, proc, st, log)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(st.Total())
	})
	mux.HandleFunc("POST /stats/reset", func(w http.ResponseWriter, _ *http.Request) {
		st.Reset()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	srv := &http.Server{Addr: *statsAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Error("stats server", "err", err)
		}
	}()

	go func() {
		t := time.NewTicker(*statsEvery)
		defer t.Stop()
		var last uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := st.Interval()
				log.Info("stats",
					"events_per_sec", float64(s.Events-last)/statsEvery.Seconds(),
					"events", s.Events, "duplicates", s.Duplicates, "actions", s.Actions,
					"ai_failed_batches", s.AIFailures,
					"p50_ms", s.LatencyP50Ms, "p99_ms", s.LatencyP99Ms, "max_ms", s.LatencyMaxMs,
					"avg_prepare_ms", s.AvgPrepareMs, "avg_ai_ms", s.AvgAIMs, "avg_produce_ms", s.AvgProduceMs)
				last = s.Events
			}
		}
	}()

	log.Info("engine started", "brokers", *brokers, "ai", *aiURL, "batch_size", *batchSize, "batch_window", *batchWindow)
	c.Run(ctx)
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
	log.Info("engine stopped", "total", st.Total())
}
