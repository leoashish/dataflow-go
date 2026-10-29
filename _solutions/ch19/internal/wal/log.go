package wal

import (
	"errors"
	"io/fs"
	"os"
	"sync"
)

type Options struct {
	SyncEveryAppend bool
	SyncFunc        func(*os.File) error
}

type WAL struct {
	mu        sync.Mutex
	f         *os.File
	opts      Options
	n         uint64
	closed    bool
	poisoned  error
	recovered int
	truncated int64
}

func Open(path string, opts Options) (*WAL, error) {
	if opts.SyncFunc == nil {
		opts.SyncFunc = func(f *os.File) error { return f.Sync() }
	}
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	records, valid, _ := ScanRecords(data) // the error only says WHY the scan stopped; the valid prefix is what counts
	cut := int64(len(data) - valid)
	if cut > 0 {
		if err := os.Truncate(path, int64(valid)); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, opts: opts, n: uint64(len(records)), recovered: len(records), truncated: cut}, nil
}

func (w *WAL) Append(payload []byte) (uint64, error) { return w.AppendBatch([][]byte{payload}) }

func (w *WAL) AppendBatch(payloads [][]byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if w.poisoned != nil {
		return 0, w.poisoned
	}
	var buf []byte
	for _, p := range payloads {
		if len(p) > MaxRecordSize {
			return 0, ErrTooLarge
		}
		buf = append(buf, EncodeRecord(p)...)
	}
	if len(buf) == 0 {
		return w.n + 1, nil
	}
	if _, err := w.f.Write(buf); err != nil {
		return 0, err
	}
	first := w.n + 1
	w.n += uint64(len(payloads))
	if w.opts.SyncEveryAppend {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
	}
	return first, nil
}

func (w *WAL) syncLocked() error {
	if w.poisoned != nil {
		return w.poisoned
	}
	if err := w.opts.SyncFunc(w.f); err != nil {
		w.poisoned = err // never retry a failed fsync: the kernel may already have dropped the data
		return err
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.syncLocked()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	syncErr := w.syncLocked()
	if err := w.f.Close(); err != nil && syncErr == nil {
		return err
	}
	return syncErr
}

func (w *WAL) Len() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func (w *WAL) Recovered() (int, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.recovered, w.truncated
}

func ReadAll(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	records, _, _ := ScanRecords(data)
	return records, nil
}
