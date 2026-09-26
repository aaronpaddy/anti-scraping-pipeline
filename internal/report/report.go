// Package report measures detection accuracy against the generator's truth file.
package report

import (
	"fmt"
	"io"
	"sort"

	"anti-scraping-pipeline/internal/event"
	"anti-scraping-pipeline/internal/gen"
)

type PersonaStats struct {
	Users        int
	UsersAlerted int // any alert (flag or block)
	UsersBlocked int
	Alerts       map[event.Action]int
}

type Report struct {
	Personas map[gen.Persona]*PersonaStats
	Ignored  int // alerts for users not in the truth file or from before the run
}

// Build aggregates alerts for users in the truth file, evaluated at or after
// the run started.
func Build(t gen.Truth, alerts []event.Alert) Report {
	r := Report{Personas: map[gen.Persona]*PersonaStats{}}
	for _, p := range t.Users {
		if r.Personas[p] == nil {
			r.Personas[p] = &PersonaStats{Alerts: map[event.Action]int{}}
		}
		r.Personas[p].Users++
	}
	alerted := map[string]bool{}
	blocked := map[string]bool{}
	for _, a := range alerts {
		p, ok := t.Users[a.ViewerUserID]
		if !ok || a.EvaluatedAt < t.StartedAt {
			r.Ignored++
			continue
		}
		ps := r.Personas[p]
		ps.Alerts[a.ActionTaken]++
		if !alerted[a.ViewerUserID] {
			alerted[a.ViewerUserID] = true
			ps.UsersAlerted++
		}
		if a.ActionTaken == event.ActionBlockSession && !blocked[a.ViewerUserID] {
			blocked[a.ViewerUserID] = true
			ps.UsersBlocked++
		}
	}
	return r
}

// Precision returns the share of alerted (or blocked) users that are bots.
func (r Report) Precision(blockedOnly bool) float64 {
	var bots, all int
	for p, ps := range r.Personas {
		n := ps.UsersAlerted
		if blockedOnly {
			n = ps.UsersBlocked
		}
		all += n
		if p.IsBot() {
			bots += n
		}
	}
	if all == 0 {
		return 0
	}
	return float64(bots) / float64(all)
}

func (r Report) Write(w io.Writer) {
	personas := make([]gen.Persona, 0, len(r.Personas))
	for p := range r.Personas {
		personas = append(personas, p)
	}
	sort.Slice(personas, func(i, j int) bool { return personas[i] < personas[j] })

	fmt.Fprintf(w, "%-11s %7s %9s %9s %9s %9s\n", "persona", "users", "alerted", "blocked", "flags", "blocks")
	for _, p := range personas {
		ps := r.Personas[p]
		fmt.Fprintf(w, "%-11s %7d %8.1f%% %8.1f%% %9d %9d\n", p, ps.Users,
			pct(ps.UsersAlerted, ps.Users), pct(ps.UsersBlocked, ps.Users),
			ps.Alerts[event.ActionFlagForReview], ps.Alerts[event.ActionBlockSession])
	}
	fmt.Fprintf(w, "\nuser-level precision: alerted %.1f%%, blocked %.1f%%\n", 100*r.Precision(false), 100*r.Precision(true))
	fmt.Fprintln(w, "(alerted/blocked for humans = false-positive rate; for bots = recall)")
	if r.Ignored > 0 {
		fmt.Fprintf(w, "ignored %d alerts from other runs\n", r.Ignored)
	}
}

func pct(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return 100 * float64(n) / float64(d)
}
