// Command eval reads the alerts topic from the beginning and reports detection
// accuracy per persona against the generator's truth file.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"telemetry-pipeline/internal/cli"
	"telemetry-pipeline/internal/event"
	"telemetry-pipeline/internal/gen"
	"telemetry-pipeline/internal/report"
)

func main() {
	brokers := flag.String("brokers", cli.Env("KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers, comma separated")
	truthPath := flag.String("truth", cli.Env("TRUTH", "data/truth.json"), "truth file written by the generator")
	idle := flag.Duration("idle", cli.EnvDuration("IDLE", 10*time.Second), "stop after no new alerts for this long")
	flag.Parse()

	truth, err := gen.ReadTruth(*truthPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read truth:", err)
		os.Exit(1)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cli.Split(*brokers)...),
		kgo.ConsumeTopics(event.AlertsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cl.Close()

	var alerts []event.Alert
	for {
		ctx, cancel := context.WithTimeout(context.Background(), *idle)
		fetches := cl.PollFetches(ctx)
		cancel()
		n := 0
		fetches.EachRecord(func(r *kgo.Record) {
			var a event.Alert
			if json.Unmarshal(r.Value, &a) == nil {
				alerts = append(alerts, a)
				n++
			}
		})
		if n == 0 {
			break
		}
	}
	fmt.Printf("read %d alerts\n\n", len(alerts))
	report.Build(truth, alerts).Write(os.Stdout)
}
