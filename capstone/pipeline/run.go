package pipeline

import (
	"context"

	"dataflow/capstone/model"
	"dataflow/internal/todo"
)

// Run executes the whole pipeline once and returns the Report; it also writes report.json and events.col into cfg.OutputDir.
//
// TODO(capstone): wire everything together:
//  1. cfg = cfg.WithDefaults(); if cfg.StateDir != "" try LoadCheckpoint (resume) else start from NewState.
//  2. ReadBatches from the checkpoint's Position.
//  3. Parse each batch with cfg.Workers goroutines but hand the results to the state IN INPUT ORDER (windows, dedup and lateness
//     depend on order; the result must not depend on Workers).
//  4. state.Observe every Parsed line; every cfg.CheckpointEvery lines call SaveCheckpoint.
//  5. On ctx cancellation stop promptly WITHOUT writing outputs (a killed run must leave no report.json) and return ctx.Err().
//  6. At the end WriteOutputs, return state.Report().
//
// Running a finished job again (same StateDir) must give the same Report.
func Run(ctx context.Context, cfg model.Config) (model.Report, error) {
	return model.Report{}, todo.NotImplemented("capstone: Run")
}
