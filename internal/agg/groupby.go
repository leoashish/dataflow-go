package agg

import (
	"context"

	"dataflow/internal/todo"
)

// GroupBy computes a Stat per key in one sequential pass: for every item, `key(item)` says which group and `val(item)`
// which number to add. Empty input -> an empty (non-nil) map.
func GroupBy[T any](items []T, key func(T) string, val func(T) float64) map[string]Stat {
	return map[string]Stat{} // TODO Ch11
}

// ParallelGroupBy does the same with `workers` goroutines and NO LOCKS in the hot loop:
//
//  1. Split items into `workers` contiguous chunks (as even as possible).
//  2. Each worker builds its OWN private map[string]Stat from its chunk -- a "combiner" / partial aggregate. Because no
//     map is shared, nothing needs a mutex and nothing is contended.
//  3. When all workers are done, merge the partial maps into one result with Stat.Merge.
//
// This "aggregate locally, merge globally" shape is how every distributed engine (Spark, Flink, BigQuery...) computes
// GROUP BY: it shuffles tiny partial aggregates over the network, never the raw rows.
//
//   - workers < 1 is treated as 1; more workers than items is fine.
//   - Cancelling ctx stops the workers promptly and returns ctx.Err(); an already-cancelled ctx does no work.
//   - The result equals GroupBy's up to floating-point rounding (merge order changes the last digits).
func ParallelGroupBy[T any](ctx context.Context, items []T, workers int, key func(T) string, val func(T) float64) (map[string]Stat, error) {
	return nil, todo.NotImplemented("Ch11: ParallelGroupBy")
}
