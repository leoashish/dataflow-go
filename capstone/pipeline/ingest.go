package pipeline

import (
	"context"

	"dataflow/capstone/model"
)

// ReadBatches streams cfg.InputDir/events-*.ndjson (files in FILE-NAME order, lines in order) starting at `from`, cut into batches
// of up to batchLines lines. It must handle lines of any length, a final line without a trailing newline, and stop when ctx ends.
// The error channel delivers at most one error and is closed when the batch channel is.
//
// TODO(capstone): chapters 03 (streaming I/O: never read a whole file) and 01 (channels, cancellation, no goroutine leaks).
func ReadBatches(ctx context.Context, cfg model.Config, from Position, batchLines int) (<-chan Batch, <-chan error) {
	batches, errs := make(chan Batch), make(chan error)
	close(batches)
	close(errs)
	return batches, errs
}
