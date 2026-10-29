// Package pipeline builds concurrent processing pipelines out of goroutines and channels.
package pipeline

// closedChan returns an already-closed channel. Unfinished stubs return it, so code reading from a stub's output
// finishes immediately (with nothing) instead of hanging forever on a nil channel. GIVEN.
func closedChan[T any]() <-chan T {
	c := make(chan T)
	close(c)
	return c
}
