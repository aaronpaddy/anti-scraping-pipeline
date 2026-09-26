// Command generator streams simulated clickstream events into Kafka (spec §4.1).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"anti-scraping-pipeline/internal/cli"
	"anti-scraping-pipeline/internal/event"
	"anti-scraping-pipeline/internal/gen"
)

func main() {
	brokers := flag.String("brokers", cli.Env("KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers, comma separated")
	rate := flag.Int("rate", cli.EnvInt("RATE", 10000), "events per second")
	duration := flag.Duration("duration", cli.EnvDuration("DURATION", time.Minute), "how long to run; 0 runs until interrupted")
	seed := flag.Uint64("seed", uint64(cli.EnvInt("SEED", 42)), "random seed")
	users := flag.Int("users", cli.EnvInt("USERS", 50000), "simulated users")
	scraperPct := flag.Float64("scraper-pct", cli.EnvFloat("SCRAPER_PCT", 0.05), "share of users that are scrapers")
	teleporterPct := flag.Float64("teleporter-pct", cli.EnvFloat("TELEPORTER_PCT", 0.02), "share of users whose location jumps")
	stealthPct := flag.Float64("stealth-pct", cli.EnvFloat("STEALTH_PCT", 0.03), "share of users that are stealth scrapers")
	powerPct := flag.Float64("power-user-pct", cli.EnvFloat("POWER_USER_PCT", 0.05), "share of users that are fast-browsing humans")
	truthPath := flag.String("truth-out", cli.Env("TRUTH_OUT", "data/truth.json"), "where to write each user's persona")
	producers := flag.Int("producers", cli.EnvInt("PRODUCERS", 4), "producer goroutines")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	start := time.Now()
	sim := gen.New(gen.Config{
		Seed: *seed, Users: *users, ScraperPct: *scraperPct, TeleporterPct: *teleporterPct,
		StealthPct: *stealthPct, PowerUserPct: *powerPct, StartMs: start.UnixMilli(),
	})
	if err := gen.WriteTruth(*truthPath, gen.Truth{Seed: *seed, StartedAt: start.UnixMilli(), Users: sim.Personas()}); err != nil {
		log.Error("write truth file", "err", err)
		os.Exit(1)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cli.Split(*brokers)...),
		kgo.DefaultProduceTopic(event.ClickstreamTopic),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.MaxBufferedRecords(100_000),
	)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	// Events are generated on one goroutine (deterministic order) and handed to
	// producer goroutines by user hash, so each user's events stay in order.
	var sent, failed atomic.Uint64
	chans := make([]chan event.Telemetry, *producers)
	var wg sync.WaitGroup
	for i := range chans {
		chans[i] = make(chan event.Telemetry, 1024)
		wg.Add(1)
		go func(in <-chan event.Telemetry) {
			defer wg.Done()
			for ev := range in {
				b, _ := json.Marshal(ev)
				cl.Produce(context.Background(), &kgo.Record{Key: []byte(ev.ViewerUserID), Value: b}, func(_ *kgo.Record, err error) {
					if err != nil {
						failed.Add(1)
						return
					}
					sent.Add(1)
				})
			}
		}(chans[i])
	}

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var last uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n := sent.Load()
				log.Info("progress", "sent", n, "failed", failed.Load(), "events_per_sec", float64(n-last)/5)
				last = n
			}
		}
	}()

	log.Info("generating", "rate", *rate, "users", *users, "seed", *seed, "truth", *truthPath)
	var emitted uint64
	tick := time.NewTicker(time.Millisecond)
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-tick.C:
			target := uint64(time.Since(start).Seconds() * float64(*rate))
			for ; emitted < target; emitted++ {
				ev, _ := sim.Next()
				h := fnv.New32a()
				h.Write([]byte(ev.ViewerUserID))
				chans[h.Sum32()%uint32(len(chans))] <- ev
			}
		}
	}
	tick.Stop()
	for _, c := range chans {
		close(c)
	}
	wg.Wait()
	fctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Flush(fctx); err != nil {
		log.Warn("flush", "err", err)
	}
	elapsed := time.Since(start).Seconds()
	log.Info("done", "sent", sent.Load(), "failed", failed.Load(), "seconds", elapsed, "events_per_sec", float64(sent.Load())/elapsed)
}
