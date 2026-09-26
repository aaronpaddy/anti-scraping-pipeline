package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"telemetry-pipeline/internal/detect"
	"telemetry-pipeline/internal/event"
	"telemetry-pipeline/internal/store"
)

const (
	sfLat, sfLon   = 37.7749, -122.4194
	nycLat, nycLon = 40.7128, -74.0060
)

func ev(id, user string, ts int64, lat, lon float64) event.Telemetry {
	return event.Telemetry{
		EventID: id, ViewerUserID: user, TargetProfileID: "p_" + id, Timestamp: ts,
		ActionType: "PROFILE_VIEW", Location: event.Coordinates{Latitude: lat, Longitude: lon},
	}
}

func inputs(evs ...event.Telemetry) []Input {
	out := make([]Input, len(evs))
	for i, e := range evs {
		b, _ := json.Marshal(e)
		out[i] = Input{Value: b, Received: time.Unix(0, 0)}
	}
	return out
}

type fakeScorer struct {
	score  float64
	delay  time.Duration
	err    error
	calls  int
	before func() // runs at call time
}

func (f *fakeScorer) Score(ctx context.Context, items []event.ScoreItem) (map[string]float64, error) {
	f.calls++
	if f.before != nil {
		f.before()
	}
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]float64{}
	for _, it := range items {
		out[it.EventID] = f.score
	}
	return out, nil
}

func newProc(s Scorer) *Processor {
	return &Processor{
		Store: store.NewMemory(), Scorer: s, AITimeout: 20 * time.Millisecond, Workers: 4,
		Now: func() time.Time { return time.Unix(0, int64(7*time.Millisecond)) },
	}
}

func TestVelocityWithinOneBatch(t *testing.T) {
	p := newProc(nil)
	alerts, res, err := p.Process(context.Background(), inputs(
		ev("a", "u1", 0, sfLat, sfLon),
		ev("b", "u1", 60_000, nycLat, nycLon), // SF -> NYC in a minute
		ev("c", "u2", 0, sfLat, sfLon),
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 1 || alerts[0].EventID != "b" || alerts[0].ActionTaken != event.ActionFlagForReview {
		t.Fatalf("alerts: %+v", alerts)
	}
	a := alerts[0]
	if !a.GeographicVelocityTriggered || a.AIScoreAvailable || a.AnomalyScore != nil || a.ProcessingDurationMs != 7 {
		t.Fatalf("alert fields: %+v", a)
	}
	if res.Events != 3 || res.Velocity != 1 || res.Actions["ALLOW"] != 2 {
		t.Fatalf("counters: %+v", res)
	}
}

func TestVelocityAcrossBatches(t *testing.T) {
	p := newProc(nil)
	ctx := context.Background()
	if _, _, err := p.Process(ctx, inputs(ev("a", "u1", 0, sfLat, sfLon))); err != nil {
		t.Fatal(err)
	}
	alerts, _, _ := p.Process(ctx, inputs(ev("b", "u1", 60_000, nycLat, nycLon)))
	if len(alerts) != 1 {
		t.Fatalf("state from the previous batch was not used: %+v", alerts)
	}
}

func TestDuplicatesAndBadRecords(t *testing.T) {
	p := newProc(nil)
	ctx := context.Background()
	batch := inputs(ev("a", "u1", 0, sfLat, sfLon), ev("a", "u1", 0, sfLat, sfLon))
	batch = append(batch, Input{Value: []byte("not json")}, Input{Value: []byte(`{"event_id":""}`)})
	_, res, _ := p.Process(ctx, batch)
	if res.Events != 1 || res.Duplicates != 1 || res.BadRecords != 2 {
		t.Fatalf("first batch: %+v", res)
	}
	// A redelivered event is re-decided but does not move state, even if its
	// payload differs.
	_, res, _ = p.Process(ctx, inputs(ev("a", "u1", 0, nycLat, nycLon)))
	if res.Events != 1 || res.Duplicates != 1 {
		t.Fatalf("redelivery: %+v", res)
	}
	_, states, _ := loadUser(p, "u1")
	if states.Session.Lat != sfLat || len(states.Window.Items) != 1 {
		t.Fatalf("redelivery changed state: %+v", states)
	}
}

func loadUser(p *Processor, u string) (bool, *detect.UserState, error) {
	states, err := p.Store.Load(context.Background(), []string{u})
	return err == nil, states[u], err
}

// Many users with interleaved events: each user's events must be evaluated in
// order even though users are spread across worker goroutines.
func TestPerUserOrdering(t *testing.T) {
	p := newProc(nil)
	var evs []event.Telemetry
	for step := 0; step < 20; step++ {
		for u := 0; u < 50; u++ {
			evs = append(evs, ev(fmt.Sprintf("e%d_%d", u, step), fmt.Sprint("u", u), int64(step)*1000, sfLat, sfLon))
		}
	}
	if _, _, err := p.Process(context.Background(), inputs(evs...)); err != nil {
		t.Fatal(err)
	}
	states, _ := p.Store.Load(context.Background(), []string{"u0", "u49"})
	for u, st := range states {
		if len(st.Window.Items) != 20 || st.Session.TS != 19_000 {
			t.Fatalf("%s: %d items, last ts %d", u, len(st.Window.Items), st.Session.TS)
		}
		for i, a := range st.Window.Items {
			if a.EventID != fmt.Sprintf("e%s_%d", u[1:], i) {
				t.Fatalf("%s out of order at %d: %s", u, i, a.EventID)
			}
		}
	}
}

func history(user string, n int) []event.Telemetry {
	var evs []event.Telemetry
	for i := 0; i < n; i++ {
		evs = append(evs, ev(fmt.Sprint(user, "_", i), user, int64(i)*1000, sfLat, sfLon))
	}
	return evs
}

func TestAIScoresDriveDecision(t *testing.T) {
	s := &fakeScorer{score: 0.97}
	p := newProc(s)
	alerts, res, _ := p.Process(context.Background(), inputs(history("u1", detect.MinFeatureLen+2)...))
	// The first MinFeatureLen-1 events lack history and are not scored.
	if want := 3; len(alerts) != want || res.AIScored != uint64(want) || s.calls != 1 {
		t.Fatalf("alerts %d scored %d calls %d", len(alerts), res.AIScored, s.calls)
	}
	for _, a := range alerts {
		if a.ActionTaken != event.ActionBlockSession || *a.AnomalyScore != 0.97 {
			t.Fatalf("alert: %+v", a)
		}
	}
}

func TestAITimeoutFallsBackToRules(t *testing.T) {
	for name, s := range map[string]*fakeScorer{
		"timeout": {score: 0.99, delay: time.Second},
		"error":   {err: errors.New("boom")},
	} {
		t.Run(name, func(t *testing.T) {
			p := newProc(s)
			evs := history("u1", detect.MinFeatureLen+2)
			evs = append(evs, ev("jump", "u1", 10_000, nycLat, nycLon))
			start := time.Now()
			alerts, res, err := p.Process(context.Background(), inputs(evs...))
			if err != nil {
				t.Fatal(err)
			}
			if time.Since(start) > 500*time.Millisecond {
				t.Fatal("processor waited past the AI timeout")
			}
			if !res.AIFailed || len(alerts) != 1 || alerts[0].EventID != "jump" ||
				alerts[0].ActionTaken != event.ActionFlagForReview || alerts[0].AIScoreAvailable {
				t.Fatalf("res %+v alerts %+v", res, alerts)
			}
		})
	}
}

func TestStateSavedBeforeAICall(t *testing.T) {
	s := &fakeScorer{score: 0.1}
	p := newProc(s)
	var saved bool
	s.before = func() {
		_, st, _ := loadUser(p, "u1")
		saved = len(st.Window.Items) > 0
	}
	p.Process(context.Background(), inputs(history("u1", detect.MinFeatureLen)...))
	if s.calls != 1 || !saved {
		t.Fatalf("calls %d, state saved before scoring: %v", s.calls, saved)
	}
}

// A batch redelivered after it was fully processed (crash before the offset
// commit) is decided the same way again: the jump is still flagged, nothing
// else is, and state is not applied twice.
func TestRedeliveredBatchRepeatsDecisions(t *testing.T) {
	p := newProc(nil)
	ctx := context.Background()
	batch := inputs(ev("a", "u1", 0, sfLat, sfLon), ev("b", "u1", 60_000, nycLat, nycLon))
	first, _, err := p.Process(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	again, res, err := p.Process(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(again) != 1 || again[0].EventID != "b" || !again[0].GeographicVelocityTriggered {
		t.Fatalf("first %+v\nagain %+v", first, again)
	}
	if res.Duplicates != 2 {
		t.Fatalf("redelivered count: %+v", res)
	}
	_, st, _ := loadUser(p, "u1")
	if len(st.Window.Items) != 2 || st.Session.TS != 60_000 {
		t.Fatalf("state after redelivery: %+v", st)
	}
}
