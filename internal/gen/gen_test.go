package gen

import (
	"reflect"
	"regexp"
	"testing"

	"anti-scraping-pipeline/internal/detect"
	"anti-scraping-pipeline/internal/event"
)

var cfg = Config{Seed: 7, Users: 3000, ScraperPct: 0.05, TeleporterPct: 0.05, StealthPct: 0.05, PowerUserPct: 0.05, StartMs: 1_790_000_000_000}

func take(s *Simulator, n int) []event.Telemetry {
	out := make([]event.Telemetry, n)
	for i := range out {
		out[i], _ = s.Next()
	}
	return out
}

func TestDeterministic(t *testing.T) {
	a, b := take(New(cfg), 5000), take(New(cfg), 5000)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different streams")
	}
	other := cfg
	other.Seed = 8
	if reflect.DeepEqual(a, take(New(other), 5000)) {
		t.Fatal("different seeds produced the same stream")
	}
}

func TestStreamShape(t *testing.T) {
	uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	s := New(cfg)
	personas := s.Personas()
	counts := map[Persona]int{}
	for _, p := range personas {
		counts[p]++
	}
	if counts[Scraper] == 0 || counts[Teleporter] == 0 || counts[StealthScraper] == 0 || counts[PowerUser] == 0 || counts[Human] < 2000 {
		t.Fatalf("unexpected persona mix: %v", counts)
	}

	seen := map[string]bool{}
	var last int64
	for _, ev := range take(s, 20000) {
		if ev.Timestamp < last {
			t.Fatal("events not in time order")
		}
		last = ev.Timestamp
		if !uuidRe.MatchString(ev.EventID) || seen[ev.EventID] {
			t.Fatalf("bad or duplicate event id %q", ev.EventID)
		}
		seen[ev.EventID] = true
		if _, ok := personas[ev.ViewerUserID]; !ok {
			t.Fatalf("unknown user %s", ev.ViewerUserID)
		}
	}
}

// The simulated personas should look the way the detectors expect: teleporters
// trip the velocity check and nobody else does; scrapers are far more regular
// than humans; and the hard pair (stealth scrapers vs power users) overlaps on
// rate and distinct profiles but differs in regularity.
func TestPersonasAreDetectable(t *testing.T) {
	s := New(cfg)
	personas := s.Personas()
	states := map[string]*detect.UserState{}
	events := map[Persona]int{}
	velocity := map[Persona]int{}
	for i := 0; i < 300000; i++ {
		ev, p := s.Next()
		st := states[ev.ViewerUserID]
		if st == nil {
			st = &detect.UserState{}
			states[ev.ViewerUserID] = st
		}
		events[p]++
		if st.Evaluate(&ev).Velocity {
			velocity[p]++
		}
	}
	for _, p := range Personas {
		rate := float64(velocity[p]) / float64(events[p])
		if p == Teleporter && rate < 0.1 || p != Teleporter && rate != 0 {
			t.Errorf("%s velocity trigger rate %.3f", p, rate)
		}
	}

	// Mean of each feature per persona, over users with enough history.
	means := map[Persona][]float64{}
	counts := map[Persona]int{}
	for id, st := range states {
		f, ok := st.Window.Features()
		if !ok {
			continue
		}
		p := personas[id]
		if means[p] == nil {
			means[p] = make([]float64, detect.FeatureDims)
		}
		for i, v := range f {
			means[p][i] += float64(v)
		}
		counts[p]++
	}
	for p, m := range means {
		for i := range m {
			m[i] /= float64(counts[p])
		}
	}
	const rate, regularity, distinct = 0, 2, 3
	stealth, power, human, scraper := means[StealthScraper], means[PowerUser], means[Human], means[Scraper]
	if stealth == nil || power == nil || human == nil || scraper == nil {
		t.Fatalf("missing personas in feature means: %v", counts)
	}
	if !(scraper[regularity] < stealth[regularity] && stealth[regularity] < power[regularity]) {
		t.Errorf("regularity should order scraper < stealth < power user: %.3f %.3f %.3f",
			scraper[regularity], stealth[regularity], power[regularity])
	}
	if !(stealth[distinct] > human[distinct] && power[distinct] > human[distinct]) {
		t.Errorf("stealth and power users should view more distinct profiles than humans: %.3f %.3f %.3f",
			stealth[distinct], power[distinct], human[distinct])
	}
	// The hard pair overlaps on rate: within a factor of 3 of each other.
	if r := stealth[rate] / power[rate]; r < 0.33 || r > 3 {
		t.Errorf("stealth and power-user rates too far apart: %.3f vs %.3f", stealth[rate], power[rate])
	}
	t.Logf("feature means [rate gap regularity distinct size]:")
	for _, p := range Personas {
		t.Logf("  %-16s %.3f (n=%d)", p, means[p], counts[p])
	}
}
