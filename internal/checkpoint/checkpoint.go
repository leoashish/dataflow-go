// Package checkpoint makes pipelines survive crashes: remember how far you got, and make repeating work harmless.
package checkpoint

import (
	"context"
	"errors"
	"sync"
)

// ErrNotFound is returned by Store.Load when nothing was ever saved under that name. GIVEN.
var ErrNotFound = errors.New("checkpoint: not found")

// Record is one entry of a replayable source. Offset is its position in the source (0, 1, 2, ...).
type Record struct {
	Offset int64
	Key    string
	Value  string
}

// Source is a REPLAYABLE log: you can ask for the records from any offset again (Kafka, a file, a table with an
// increasing id). Without replayability there is no recovery.
// Read returns up to max records whose Offset >= from, in order; an empty slice means "nothing more right now".
type Source interface {
	Read(ctx context.Context, from int64, max int) ([]Record, error)
}

// Sink receives records. Write may be called for the same record more than once after a crash/restart.
type Sink interface {
	Write(ctx context.Context, rec Record) error
}

// Options tunes Run.
type Options struct {
	Name            string // checkpoint name in the Store
	BatchSize       int    // records requested per Source.Read; < 1 means 1
	CheckpointEvery int    // save the checkpoint after this many records written; < 1 means after every record
}

// MemorySource is an in-memory replayable Source. GIVEN (tests and your experiments use it).
type MemorySource struct {
	mu   sync.Mutex
	recs []Record
}

func NewMemorySource(recs []Record) *MemorySource {
	return &MemorySource{recs: append([]Record(nil), recs...)}
}

func (m *MemorySource) Read(ctx context.Context, from int64, max int) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	for _, r := range m.recs {
		if r.Offset >= from && len(out) < max {
			out = append(out, r)
		}
	}
	return out, nil
}
