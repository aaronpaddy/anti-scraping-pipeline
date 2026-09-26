package engine

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"telemetry-pipeline/internal/event"
	"telemetry-pipeline/internal/gen"
	"telemetry-pipeline/internal/stats"
	"telemetry-pipeline/internal/store"
)

// aiStub scores regular traffic over mostly distinct profiles (features 2 and 3)
// as a bot, a crude stand-in for the k-NN index.
func aiStub(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req event.ScoreRequest
		json.NewDecoder(r.Body).Decode(&req)
		var resp event.ScoreResponse
		for _, it := range req.Items {
			score := 0.1
			if it.Features[2] < 0.1 && it.Features[3] > 0.9 {
				score = 0.99
			}
			resp.Results = append(resp.Results, event.ScoreResult{EventID: it.EventID, AnomalyScore: score})
		}
		json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func startConsumer(t *testing.T, ctx context.Context, brokers []string, st store.Store, aiURL string) (*stats.Stats, <-chan struct{}) {
	t.Helper()
	sts := stats.New()
	proc := &Processor{Store: st, Scorer: NewHTTPScorer(aiURL), AITimeout: 200 * time.Millisecond, Workers: 4}
	c, err := NewConsumer(ctx, ConsumerConfig{
		Brokers: brokers, Group: "engine-test",
		InTopic: event.ClickstreamTopic, OutTopic: event.AlertsTopic,
		BatchSize: 100, BatchWindow: 10 * time.Millisecond,
	}, proc, sts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	return sts, done
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestConsumerEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end")
	}
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, event.ClickstreamTopic, event.AlertsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()
	mr := miniredis.RunT(t)
	st := store.NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	ai := aiStub(t)

	// Produce simulated traffic.
	sim := gen.New(gen.Config{Seed: 7, Users: 300, ScraperPct: 0.1, TeleporterPct: 0.1, StartMs: 1_790_000_000_000})
	personas := sim.Personas()
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(event.ClickstreamTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	const total = 5000
	var recs []*kgo.Record
	for i := 0; i < total; i++ {
		ev, _ := sim.Next()
		b, _ := json.Marshal(ev)
		recs = append(recs, &kgo.Record{Key: []byte(ev.ViewerUserID), Value: b})
	}
	if err := prod.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sts, done := startConsumer(t, ctx, brokers, st, ai.URL)
	waitFor(t, "all events processed", func() bool { return sts.Total().Events == total })
	cancel()
	<-done

	s := sts.Total()
	if s.Duplicates != 0 || s.BadRecords != 0 || s.AIScored == 0 || s.AIFailures != 0 {
		t.Fatalf("stats: %+v", s)
	}

	// Read the alerts back and check who got them.
	reader, _ := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(event.AlertsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer reader.Close()
	byPersona := map[gen.Persona]map[event.Action]int{}
	var alerts uint64
	for alerts < s.Actions["FLAG_FOR_REVIEW"]+s.Actions["BLOCK_SESSION"] {
		pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
		f := reader.PollFetches(pctx)
		timedOut := pctx.Err() != nil
		pcancel()
		if timedOut {
			t.Fatalf("read %d alerts, want %d", alerts, s.Actions["FLAG_FOR_REVIEW"]+s.Actions["BLOCK_SESSION"])
		}
		f.EachRecord(func(r *kgo.Record) {
			var a event.Alert
			json.Unmarshal(r.Value, &a)
			if string(r.Key) != a.ViewerUserID {
				t.Errorf("alert key %q != user %q", r.Key, a.ViewerUserID)
			}
			p := personas[a.ViewerUserID]
			if byPersona[p] == nil {
				byPersona[p] = map[event.Action]int{}
			}
			byPersona[p][a.ActionTaken]++
			alerts++
		})
	}
	if len(byPersona[gen.Human]) != 0 {
		t.Errorf("humans alerted: %v", byPersona[gen.Human])
	}
	if byPersona[gen.Scraper][event.ActionBlockSession] == 0 || byPersona[gen.Teleporter][event.ActionFlagForReview] == 0 {
		t.Errorf("bots not caught: %v", byPersona)
	}

	// Offsets were committed: a restarted engine in the same group sees nothing.
	ctx2, cancel2 := context.WithCancel(context.Background())
	sts2, done2 := startConsumer(t, ctx2, brokers, st, ai.URL)
	time.Sleep(3 * time.Second)
	cancel2()
	<-done2
	if s2 := sts2.Total(); s2.Events != 0 || s2.Duplicates != 0 {
		t.Fatalf("restarted engine reprocessed events: %+v", s2)
	}
}
