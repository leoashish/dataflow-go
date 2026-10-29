package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

func Run(ctx context.Context, src Source, sink Sink, store *Store, o Options) (int, error) {
	batch := max(o.BatchSize, 1)
	every := max(o.CheckpointEvery, 1)

	next, err := loadOffset(store, o.Name)
	if err != nil {
		return 0, err
	}

	written, sinceCheckpoint := 0, 0
	save := func() error {
		sinceCheckpoint = 0
		return store.Save(o.Name, []byte(strconv.FormatInt(next, 10)))
	}

	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		records, err := src.Read(ctx, next, batch)
		if err != nil {
			return written, err
		}
		if len(records) == 0 { // the source is exhausted
			if sinceCheckpoint > 0 {
				return written, save()
			}
			return written, nil
		}
		for _, rec := range records {
			if err := sink.Write(ctx, rec); err != nil {
				return written, err // NO checkpoint for this record: it will be replayed
			}
			written++
			sinceCheckpoint++
			next = rec.Offset + 1
			if sinceCheckpoint >= every {
				if err := save(); err != nil {
					return written, err
				}
			}
		}
	}
}

func loadOffset(store *Store, name string) (int64, error) {
	data, err := store.Load(name)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("checkpoint %q is corrupt: %w", name, err)
	}
	return n, nil
}
