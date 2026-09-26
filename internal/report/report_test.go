package report

import (
	"strings"
	"testing"

	"anti-scraping-pipeline/internal/event"
	"anti-scraping-pipeline/internal/gen"
)

func TestBuild(t *testing.T) {
	truth := gen.Truth{StartedAt: 100, Users: map[string]gen.Persona{
		"h1": gen.Human, "h2": gen.Human, "s1": gen.Scraper, "s2": gen.Scraper, "t1": gen.Teleporter,
	}}
	alerts := []event.Alert{
		{ViewerUserID: "s1", EvaluatedAt: 200, ActionTaken: event.ActionBlockSession},
		{ViewerUserID: "s1", EvaluatedAt: 201, ActionTaken: event.ActionBlockSession},
		{ViewerUserID: "t1", EvaluatedAt: 200, ActionTaken: event.ActionFlagForReview},
		{ViewerUserID: "h1", EvaluatedAt: 200, ActionTaken: event.ActionFlagForReview},
		{ViewerUserID: "h2", EvaluatedAt: 50, ActionTaken: event.ActionBlockSession}, // before the run
		{ViewerUserID: "other", EvaluatedAt: 200, ActionTaken: event.ActionBlockSession},
	}
	r := Build(truth, alerts)
	if r.Ignored != 2 {
		t.Fatalf("ignored = %d", r.Ignored)
	}
	s := r.Personas[gen.Scraper]
	if s.Users != 2 || s.UsersAlerted != 1 || s.UsersBlocked != 1 || s.Alerts[event.ActionBlockSession] != 2 {
		t.Fatalf("scraper stats: %+v", s)
	}
	if got := r.Precision(false); got < 0.66 || got > 0.67 {
		t.Fatalf("alert precision = %v", got) // s1, t1 of s1, t1, h1
	}
	if r.Precision(true) != 1 {
		t.Fatalf("block precision = %v", r.Precision(true))
	}
	var b strings.Builder
	r.Write(&b)
	if !strings.Contains(b.String(), "scraper") {
		t.Fatal(b.String())
	}
}
