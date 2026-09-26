// Command seed-export builds labeled behavior vectors for the Qdrant index. It
// runs simulated traffic through the same feature code the engine uses, in
// memory, and writes JSON lines of {"features": [...], "label": "human"|"bot"}.
//
// Every persona is sampled separately (up to -per-persona vectors each), so
// slow personas such as stealth scrapers are not drowned out by fast ones.
// Power users are labeled human and stealth scrapers bot: the index stands in
// for a history of labeled incidents.
//
// Teleporters are left out: their timing is human, so labeling them "bot"
// would teach the index that human behavior is bot-like. The velocity check
// catches them instead.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"

	"anti-scraping-pipeline/internal/cli"
	"anti-scraping-pipeline/internal/detect"
	"anti-scraping-pipeline/internal/gen"
)

func main() {
	seed := flag.Uint64("seed", uint64(cli.EnvInt("SEED", 1001)), "random seed; keep different from the evaluation run")
	users := flag.Int("users", cli.EnvInt("USERS", 20000), "simulated users")
	events := flag.Int("events", cli.EnvInt("EVENTS", 3_000_000), "simulated events to run")
	perPersona := flag.Int("per-persona", cli.EnvInt("PER_PERSONA", 10000), "vectors to keep per persona")
	out := flag.String("out", cli.Env("OUT", "data/seed.jsonl"), "output file")
	flag.Parse()

	sim := gen.New(gen.Config{
		Seed: *seed, Users: *users, StartMs: 1_790_000_000_000,
		ScraperPct: 0.05, StealthPct: 0.10, PowerUserPct: 0.10,
	})
	states := map[string]*detect.UserState{}

	type row struct {
		Features []float32   `json:"features"`
		Label    string      `json:"label"`
		Persona  gen.Persona `json:"persona"` // informational; scoring uses Label
	}
	// Reservoir-sample each persona so vectors come from across the whole run.
	rng := rand.New(rand.NewPCG(*seed, 1))
	res := map[gen.Persona][]row{}
	seen := map[gen.Persona]int{}
	for i := 0; i < *events; i++ {
		ev, p := sim.Next()
		st := states[ev.ViewerUserID]
		if st == nil {
			st = &detect.UserState{}
			states[ev.ViewerUserID] = st
		}
		r := st.Evaluate(&ev)
		if r.Features == nil || p == gen.Teleporter {
			continue
		}
		label := "human"
		if p.IsBot() {
			label = "bot"
		}
		seen[p]++
		if len(res[p]) < *perPersona {
			res[p] = append(res[p], row{r.Features, label, p})
		} else if j := rng.IntN(seen[p]); j < *perPersona {
			res[p][j] = row{r.Features, label, p}
		}
	}

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	summary := map[gen.Persona]int{}
	for _, p := range gen.Personas {
		for _, r := range res[p] {
			enc.Encode(r)
		}
		if len(res[p]) > 0 {
			summary[p] = len(res[p])
		}
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	f.Close()
	fmt.Printf("wrote %s: %v\n", *out, summary)
}
