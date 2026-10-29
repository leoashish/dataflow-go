package join

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"dataflow/internal/hashx"
)

type partitionWriter struct {
	f   *os.File
	bw  *bufio.Writer
	enc *json.Encoder
}

func newPartitionWriters(dir, side string, n int) ([]*partitionWriter, error) {
	ws := make([]*partitionWriter, n)
	for p := range ws {
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%s-%04d.jsonl", side, p)))
		if err != nil {
			return nil, err
		}
		bw := bufio.NewWriter(f)
		ws[p] = &partitionWriter{f: f, bw: bw, enc: json.NewEncoder(bw)}
	}
	return ws, nil
}

func closeWriters(ws []*partitionWriter) error {
	var first error
	for _, w := range ws {
		if w == nil {
			continue
		}
		if err := w.bw.Flush(); err != nil && first == nil {
			first = err
		}
		if err := w.f.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// readPartition streams the rows of one partition file.
func readPartition(dir, side string, p int, fn func(Row) error) error {
	f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s-%04d.jsonl", side, p)))
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var r Row
		if err := dec.Decode(&r); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
}

func GraceHashJoin(left, right Iter, leftKey, rightKey string, kind Kind, o GraceOptions, emit func(Row) error) (GraceStats, error) {
	if o.Partitions < 1 {
		return GraceStats{}, fmt.Errorf("join: Partitions must be at least 1, got %d", o.Partitions)
	}
	dir, err := os.MkdirTemp(o.TmpDir, "grace-")
	if err != nil {
		return GraceStats{}, err
	}
	defer os.RemoveAll(dir)

	st := GraceStats{Partitions: o.Partitions}
	partition := func(key string) int { return int(hashx.Sum64(key) % uint64(o.Partitions)) }

	// pass 1: partition both inputs to disk
	lw, err := newPartitionWriters(dir, "left", o.Partitions)
	if err != nil {
		closeWriters(lw)
		return st, err
	}
	rw, err := newPartitionWriters(dir, "right", o.Partitions)
	if err != nil {
		closeWriters(lw)
		closeWriters(rw)
		return st, err
	}
	fail := func(err error) (GraceStats, error) {
		closeWriters(lw)
		closeWriters(rw)
		return st, err
	}
	for r, ok := right(); ok; r, ok = right() {
		if k, has := keyOf(r, rightKey); has {
			if err := rw[partition(k)].enc.Encode(r); err != nil {
				return fail(err)
			}
			st.SpilledRight++
		}
	}
	for l, ok := left(); ok; l, ok = left() {
		k, has := keyOf(l, leftKey)
		if !has {
			if kind == LeftOuter {
				if err := emit(copyRow(l)); err != nil {
					return fail(err)
				}
			}
			continue
		}
		if err := lw[partition(k)].enc.Encode(l); err != nil {
			return fail(err)
		}
		st.SpilledLeft++
	}
	if err := closeWriters(lw); err != nil {
		closeWriters(rw)
		return st, err
	}
	if err := closeWriters(rw); err != nil {
		return st, err
	}

	// pass 2: one in-memory hash join per partition
	for p := 0; p < o.Partitions; p++ {
		build := map[string][]Row{}
		rows := 0
		if err := readPartition(dir, "right", p, func(r Row) error {
			build[r[rightKey]] = append(build[r[rightKey]], r)
			rows++
			return nil
		}); err != nil {
			return st, err
		}
		st.MaxBuildRows = max(st.MaxBuildRows, rows)

		if err := readPartition(dir, "left", p, func(l Row) error {
			matches := build[l[leftKey]]
			if len(matches) == 0 {
				if kind == LeftOuter {
					return emit(l)
				}
				return nil
			}
			for _, r := range matches {
				if err := emit(merge(l, r)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return st, err
		}
	}
	return st, nil
}
