// Package batch groups a stream of items into batches and decouples fast producers from slow consumers.
package batch

import (
	"sync/atomic"
	"time"
)

// Options configures Batches.
type Options struct {
	Size    int           // flush when a batch reaches this many items (< 1 is treated as 1)
	MaxWait time.Duration // flush when the batch's FIRST item has waited this long; 0 means "no time limit"

	// After is a timer factory, exactly like time.After. nil means time.After. Tests inject a fake one so that
	// "10 seconds later" takes zero real time and can never flake. GIVEN: just call o.After(o.MaxWait).
	After func(d time.Duration) <-chan time.Time
}

func (o Options) after(d time.Duration) <-chan time.Time {
	if o.After != nil {
		return o.After(d)
	}
	return time.After(d)
}

func closedBatches[T any]() <-chan []T {
	c := make(chan []T)
	close(c)
	return c
}

func closedItems[T any]() <-chan T {
	c := make(chan T)
	close(c)
	return c
}

// Policy says what an elastic Buffer does when it is full.
type Policy int

const (
	Block      Policy = iota // stop accepting input until there is room: the producer is slowed down (back-pressure)
	DropNewest               // accept the new item and throw IT away: the buffer keeps the oldest items
	DropOldest               // make room by throwing the OLDEST buffered item away: the buffer keeps the newest items
)

// atomicCounter is a tiny helper used by Buffer. GIVEN.
type atomicCounter struct{ n atomic.Int64 }
