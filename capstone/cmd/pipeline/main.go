// Command pipeline runs the capstone: go run ./capstone/cmd/pipeline -in data -out out [-state state] [-workers 4] [-checkpoint-every 2000]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"dataflow/capstone/model"
	"dataflow/capstone/pipeline"
)

func main() {
	var cfg model.Config
	flag.StringVar(&cfg.InputDir, "in", "data", "directory with events-*.ndjson (make some with: go run ./capstone/cmd/gen -out data)")
	flag.StringVar(&cfg.OutputDir, "out", "out", "directory for report.json and events.col")
	flag.StringVar(&cfg.StateDir, "state", "", "directory for checkpoints (empty = none)")
	flag.IntVar(&cfg.Workers, "workers", 4, "parsing goroutines")
	flag.IntVar(&cfg.CheckpointEvery, "checkpoint-every", 2000, "checkpoint after this many input lines (0 = never)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt) // Ctrl-C = a clean cancellation
	defer stop()

	report, err := pipeline.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pipeline:", err)
		os.Exit(1)
	}
	t := report.Totals
	fmt.Printf("read=%d accepted=%d duplicates=%d late=%d rejected=%v windows=%d\nwrote %s/report.json and %s/events.col\n",
		t.Read, t.Accepted, t.Duplicates, t.Late, t.Rejected, len(report.Windows), cfg.OutputDir, cfg.OutputDir)
}
