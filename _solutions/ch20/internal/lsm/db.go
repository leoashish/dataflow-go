package lsm

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"dataflow/internal/extsort"
	"dataflow/internal/wal"
)

type DB struct {
	mu      sync.Mutex
	opts    Options
	mem     *Memtable
	wal     *wal.WAL
	tables  []*SSTable // newest first
	nextNum int
	closed  bool
}

func (db *DB) walPath() string { return filepath.Join(db.opts.Dir, "wal.log") }

func (db *DB) openWAL() error {
	w, err := wal.Open(db.walPath(), wal.Options{SyncEveryAppend: db.opts.SyncWrites})
	db.wal = w
	return err
}

func Open(opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if opts.Dir == "" {
		return nil, fmt.Errorf("lsm: Options.Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{opts: opts, mem: NewMemtable(), nextNum: 1}

	names, err := filepath.Glob(filepath.Join(opts.Dir, "??????.sst"))
	if err != nil {
		return nil, err
	}
	sort.Strings(names) // zero-padded numbers sort numerically
	for _, name := range names {
		n, err := strconv.Atoi(strings.TrimSuffix(filepath.Base(name), ".sst"))
		if err != nil {
			continue
		}
		t, err := OpenSSTable(name, opts.BloomFPR)
		if err != nil {
			db.closeTables()
			return nil, err
		}
		db.tables = append([]*SSTable{t}, db.tables...) // newest first
		db.nextNum = max(db.nextNum, n+1)
	}

	records, err := wal.ReadAll(db.walPath())
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		db.closeTables()
		return nil, err
	}
	for _, rec := range records {
		e, err := decodeOp(rec)
		if err != nil {
			db.closeTables()
			return nil, err
		}
		if e.Tombstone {
			db.mem.Delete(e.Key)
		} else {
			db.mem.Put(e.Key, e.Value)
		}
	}
	if err := db.openWAL(); err != nil {
		db.closeTables()
		return nil, err
	}
	return db, nil
}

func (db *DB) closeTables() {
	for _, t := range db.tables {
		t.Close()
	}
}

func (db *DB) write(e Entry) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if e.Key == "" {
		return ErrEmptyKey
	}
	if _, err := db.wal.Append(encodeOp(e)); err != nil {
		return err
	}
	if e.Tombstone {
		db.mem.Delete(e.Key)
	} else {
		db.mem.Put(e.Key, e.Value)
	}
	if db.mem.Len() >= db.opts.MemtableMaxEntries {
		return db.flushLocked()
	}
	return nil
}

func (db *DB) Put(key, value string) error { return db.write(Entry{Key: key, Value: value}) }

func (db *DB) Delete(key string) error { return db.write(Entry{Key: key, Tombstone: true}) }

func (db *DB) Get(key string) (string, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return "", false, ErrClosed
	}
	if key == "" {
		return "", false, ErrEmptyKey
	}
	if e, ok := db.mem.Get(key); ok {
		return e.Value, !e.Tombstone, nil
	}
	for _, t := range db.tables {
		e, ok, err := t.Get(key)
		if err != nil {
			return "", false, err
		}
		if ok { // the newest entry decides, a tombstone included
			return e.Value, !e.Tombstone, nil
		}
	}
	return "", false, nil
}

// mergeNewestWins feeds the sources (NEWEST FIRST) through the k-way merge and calls fn once per key with its newest entry.
func mergeNewestWins(sources [][]Entry, fn func(Entry)) error {
	iters := make([]func() (Entry, bool), len(sources))
	for i, src := range sources {
		src, pos := src, 0
		iters[i] = func() (Entry, bool) {
			if pos >= len(src) {
				return Entry{}, false
			}
			pos++
			return src[pos-1], true
		}
	}
	var last string
	first := true
	return extsort.KWayMerge(iters, func(a, b Entry) bool { return a.Key < b.Key }, func(e Entry) error {
		if !first && e.Key == last {
			return nil // an older version of a key we already emitted
		}
		first, last = false, e.Key
		fn(e)
		return nil
	})
}

func (db *DB) Scan(from, to string) ([]KV, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	inRange := func(entries []Entry) []Entry {
		var out []Entry
		for _, e := range entries {
			if e.Key >= from && (to == "" || e.Key < to) {
				out = append(out, e)
			}
		}
		return out
	}
	sources := [][]Entry{inRange(db.mem.Entries())}
	for _, t := range db.tables {
		entries, err := t.Entries()
		if err != nil {
			return nil, err
		}
		sources = append(sources, inRange(entries))
	}
	var out []KV
	err := mergeNewestWins(sources, func(e Entry) {
		if !e.Tombstone {
			out = append(out, KV{Key: e.Key, Value: e.Value})
		}
	})
	return out, err
}

func (db *DB) nextPath() string {
	p := filepath.Join(db.opts.Dir, fmt.Sprintf("%06d.sst", db.nextNum))
	db.nextNum++
	return p
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.flushLocked()
}

func (db *DB) flushLocked() error {
	if db.mem.Len() == 0 {
		return nil
	}
	path := db.nextPath()
	if err := WriteSSTable(path, db.mem.Entries(), db.opts.BloomFPR); err != nil {
		return err
	}
	t, err := OpenSSTable(path, db.opts.BloomFPR)
	if err != nil {
		return err
	}
	db.tables = append([]*SSTable{t}, db.tables...)
	db.mem = NewMemtable()
	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := os.Remove(db.walPath()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := db.openWAL(); err != nil {
		return err
	}
	if db.opts.CompactAfter > 0 && len(db.tables) >= db.opts.CompactAfter {
		return db.compactLocked()
	}
	return nil
}

func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.compactLocked()
}

func (db *DB) compactLocked() error {
	// compactLocked is also called from flushLocked, which has just emptied the memtable; an explicit Compact flushes first.
	if err := db.flushNoCompactLocked(); err != nil {
		return err
	}
	if len(db.tables) == 0 {
		return nil
	}
	sources := make([][]Entry, len(db.tables))
	for i, t := range db.tables {
		entries, err := t.Entries()
		if err != nil {
			return err
		}
		sources[i] = entries
	}
	var live []Entry
	if err := mergeNewestWins(sources, func(e Entry) {
		if !e.Tombstone { // nothing older survives below a full compaction, so tombstones can finally go
			live = append(live, e)
		}
	}); err != nil {
		return err
	}
	old := db.tables
	var merged []*SSTable
	if len(live) > 0 {
		path := db.nextPath()
		if err := WriteSSTable(path, live, db.opts.BloomFPR); err != nil {
			return err
		}
		t, err := OpenSSTable(path, db.opts.BloomFPR)
		if err != nil {
			return err
		}
		merged = []*SSTable{t}
	}
	db.tables = merged
	for _, t := range old {
		t.Close()
		os.Remove(t.Path())
	}
	return nil
}

// flushNoCompactLocked flushes the memtable without triggering the automatic compaction (avoids recursion).
func (db *DB) flushNoCompactLocked() error {
	saved := db.opts.CompactAfter
	db.opts.CompactAfter = 0
	defer func() { db.opts.CompactAfter = saved }()
	return db.flushLocked()
}

func (db *DB) Stats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	s := Stats{MemtableEntries: db.mem.Len(), Tables: len(db.tables)}
	for _, t := range db.tables {
		s.BloomSkips += t.BloomSkips()
		s.BlockReads += t.BlockReads()
	}
	return s
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	err := db.wal.Close()
	db.closeTables()
	return err
}
