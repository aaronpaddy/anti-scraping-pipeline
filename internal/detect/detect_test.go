package detect

import (
	"fmt"
	"testing"

	"anti-scraping-pipeline/internal/event"
)

const (
	sfLat, sfLon   = 37.7749, -122.4194
	nycLat, nycLon = 40.7128, -74.0060
	minute         = int64(60_000)
)

func TestSessionObserve(t *testing.T) {
	tests := []struct {
		name      string
		prev      Session
		lat, lon  float64
		ts        int64
		want      bool
		wantTS    int64 // stored timestamp afterwards
		wantMoved bool  // stored location changed
	}{
		{"first event never triggers", Session{}, sfLat, sfLon, 1000, false, 1000, true},
		{"SF to NYC in 3 minutes", Session{sfLat, sfLon, 0, true}, nycLat, nycLon, 3 * minute, true, 3 * minute, true},
		{"SF to NYC in 6 hours is a flight", Session{sfLat, sfLon, 0, true}, nycLat, nycLon, 360 * minute, false, 360 * minute, true},
		{"jitter under 5 miles in 1ms", Session{sfLat, sfLon, 0, true}, sfLat + 0.01, sfLon, 1, false, 1, true},
		{"same millisecond far away", Session{sfLat, sfLon, 5000, true}, nycLat, nycLon, 5000, true, 5000, true},
		{"older event far away triggers but does not overwrite", Session{sfLat, sfLon, 10 * minute, true}, nycLat, nycLon, 9 * minute, true, 10 * minute, false},
		{"older event nearby does not overwrite", Session{sfLat, sfLon, 10 * minute, true}, sfLat, sfLon + 0.01, 9 * minute, false, 10 * minute, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.prev
			if got := s.Observe(tc.lat, tc.lon, tc.ts); got != tc.want {
				t.Fatalf("triggered = %v, want %v", got, tc.want)
			}
			if s.TS != tc.wantTS {
				t.Fatalf("stored ts = %d, want %d", s.TS, tc.wantTS)
			}
			if moved := s.Lat == tc.lat && s.Lon == tc.lon; moved != tc.wantMoved {
				t.Fatalf("location updated = %v, want %v", moved, tc.wantMoved)
			}
		})
	}
}

func TestWindowTrim(t *testing.T) {
	var w Window
	for i := 0; i < 10; i++ {
		w.Add(Activity{EventID: fmt.Sprint(i), TS: int64(i) * minute})
	}
	// Last event at 9 min, so everything before 4 min is dropped.
	if len(w.Items) != 6 || w.Items[0].TS != 4*minute {
		t.Fatalf("time trim: got %d items starting at %d", len(w.Items), w.Items[0].TS)
	}

	w = Window{}
	for i := 0; i < WindowMax+50; i++ {
		w.Add(Activity{TS: int64(i)})
	}
	if len(w.Items) != WindowMax || w.Items[0].TS != 50 {
		t.Fatalf("size trim: got %d items starting at %d", len(w.Items), w.Items[0].TS)
	}
}

func TestWindowOutOfOrderInsert(t *testing.T) {
	var w Window
	for _, ts := range []int64{1000, 3000, 2000, 500} {
		w.Add(Activity{TS: ts})
	}
	for i := 1; i < len(w.Items); i++ {
		if w.Items[i].TS < w.Items[i-1].TS {
			t.Fatalf("window not sorted: %+v", w.Items)
		}
	}
}

func TestEvaluateRedelivery(t *testing.T) {
	var u UserState
	first := event.Telemetry{EventID: "a", Timestamp: 0, Location: event.Coordinates{Latitude: sfLat, Longitude: sfLon}}
	jump := event.Telemetry{EventID: "b", Timestamp: minute, Location: event.Coordinates{Latitude: nycLat, Longitude: nycLon}}
	u.Evaluate(&first)
	if r := u.Evaluate(&jump); !r.Velocity || r.Redelivered || !r.Added.Velocity {
		t.Fatalf("jump: %+v", r)
	}
	// Redelivered in order: each keeps its original result, state unchanged.
	if r := u.Evaluate(&first); r.Velocity || !r.Redelivered {
		t.Fatalf("redelivered first: %+v", r)
	}
	if r := u.Evaluate(&jump); !r.Velocity || !r.Redelivered {
		t.Fatalf("redelivered jump: %+v", r)
	}
	if len(u.Window.Items) != 2 || u.Session.TS != minute {
		t.Fatalf("state changed by redelivery: %+v", u)
	}
}

func TestWindowIgnoresRepeat(t *testing.T) {
	var w Window
	w.Add(Activity{EventID: "a", TS: 1000})
	w.Add(Activity{EventID: "b", TS: 1000})
	w.Add(Activity{EventID: "a", TS: 1000})
	if len(w.Items) != 2 {
		t.Fatalf("repeat was added: %+v", w.Items)
	}
}

func TestFeatures(t *testing.T) {
	var w Window
	for i := 0; i < MinFeatureLen-1; i++ {
		w.Add(Activity{TS: int64(i) * 1000, Target: "p"})
	}
	if _, ok := w.Features(); ok {
		t.Fatal("too little history should not produce features")
	}

	// Scraper: one new profile every second, perfectly regular.
	var bot Window
	for i := 0; i < 100; i++ {
		bot.Add(Activity{TS: int64(i) * 1000, Target: fmt.Sprint("p", i)})
	}
	// Human: irregular gaps, revisiting two profiles.
	var human Window
	ts := int64(0)
	for i, gap := range []int64{4, 30, 9, 55, 2, 18, 70, 12} {
		ts += gap * 1000
		human.Add(Activity{TS: ts, Target: fmt.Sprint("p", i%2)})
	}
	b, _ := bot.Features()
	h, _ := human.Features()
	for i, v := range append(append([]float32{}, b...), h...) {
		if v < 0 || v > 1 {
			t.Fatalf("feature %d out of [0,1]: %v", i%FeatureDims, v)
		}
	}
	if !(b[0] > h[0]) || !(b[1] < h[1]) || !(b[2] < h[2]) || !(b[3] > h[3]) {
		t.Fatalf("bot and human features not separated\nbot   %v\nhuman %v", b, h)
	}
	if b[2] != 0 || b[3] != 1 {
		t.Fatalf("perfectly regular bot: want regularity 0 and distinct 1, got %v", b)
	}
}

func TestDecide(t *testing.T) {
	tests := []struct {
		velocity, hasScore, sustained bool
		score                         float64
		want                          event.Action
	}{
		{false, true, true, 0.97, event.ActionBlockSession},
		{false, true, false, 0.97, event.ActionFlagForReview}, // one high score only flags
		{true, true, true, 0.90, event.ActionBlockSession},
		{true, true, false, 0.90, event.ActionFlagForReview},
		{true, true, true, 0.50, event.ActionFlagForReview},
		{true, false, false, 0, event.ActionFlagForReview},
		{false, true, true, 0.90, event.ActionFlagForReview},
		{false, true, false, 0.85, event.ActionFlagForReview},
		{false, true, true, 0.84, event.ActionAllow},
		{false, false, false, 0, event.ActionAllow},
	}
	for _, tc := range tests {
		if got := Decide(tc.velocity, tc.score, tc.hasScore, tc.sustained); got != tc.want {
			t.Errorf("Decide(velocity=%v, score=%v, has=%v, sustained=%v) = %s, want %s",
				tc.velocity, tc.score, tc.hasScore, tc.sustained, got, tc.want)
		}
	}
}

func TestScoreHistorySustained(t *testing.T) {
	h := NewScoreHistory()
	for i := 0; i < SustainedOf-1; i++ {
		if h.Record("u", int64(i), 0.99) {
			t.Fatalf("sustained after only %d scores", i+1)
		}
	}
	if !h.Record("u", 9, 0.99) {
		t.Fatal("10 high scores should be sustained")
	}
	// Three low scores leave 7 of the last 10 high: not sustained.
	h.Record("u", 10, 0.1)
	h.Record("u", 11, 0.1)
	if !h.Record("u", 12, 0.99) {
		t.Fatal("8 of 10 high should still be sustained")
	}
	if h.Record("u", 13, 0.1) {
		t.Fatal("7 of 10 high should not be sustained")
	}
	// Other users are independent.
	if h.Record("other", 13, 0.99) {
		t.Fatal("a new user's first score cannot be sustained")
	}
}

func TestScoreHistoryEvictsIdle(t *testing.T) {
	h := NewScoreHistory()
	h.Record("idle", 0, 0.5)
	for i := 1; i <= 100_000; i++ {
		h.Record("active", historyIdleMs+int64(i), 0.5)
	}
	if h.Len() != 1 {
		t.Fatalf("tracked users = %d, want 1", h.Len())
	}
}
