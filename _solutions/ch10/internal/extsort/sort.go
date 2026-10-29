package extsort

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func SortLines(r io.Reader, w io.Writer, o Options) (Stats, error) {
	if o.MaxLinesInMemory < 1 {
		return Stats{}, fmt.Errorf("extsort: MaxLinesInMemory must be at least 1, got %d", o.MaxLinesInMemory)
	}
	if o.MaxMergeFanIn == 1 || o.MaxMergeFanIn < 0 {
		return Stats{}, fmt.Errorf("extsort: MaxMergeFanIn must be 0 (unlimited) or at least 2, got %d", o.MaxMergeFanIn)
	}
	less := o.Less
	if less == nil {
		less = func(a, b string) bool { return a < b }
	}

	dir, err := os.MkdirTemp(o.TmpDir, "extsort-")
	if err != nil {
		return Stats{}, err
	}
	defer os.RemoveAll(dir) // always, success or failure

	var st Stats
	runs, err := makeRuns(r, dir, o.MaxLinesInMemory, less, &st)
	if err != nil {
		return st, err
	}

	fanIn := o.MaxMergeFanIn
	if fanIn == 0 {
		fanIn = max(len(runs), 2)
	}
	counter := 0
	for len(runs) > fanIn { // intermediate passes: merge groups of CONSECUTIVE runs (keeps the merge stable)
		var next []string
		for i := 0; i < len(runs); i += fanIn {
			group := runs[i:min(i+fanIn, len(runs))]
			counter++
			merged := filepath.Join(dir, fmt.Sprintf("merged-%06d", counter))
			if err := mergeIntoFile(group, merged, less); err != nil {
				return st, err
			}
			for _, p := range group {
				os.Remove(p) // free the disk space as we go
			}
			next = append(next, merged)
		}
		runs = next
		st.MergePasses++
	}

	bw := bufio.NewWriter(w)
	if err := mergeFiles(runs, less, func(line string) error {
		_, err := bw.WriteString(line + "\n")
		return err
	}); err != nil {
		return st, err
	}
	return st, bw.Flush()
}

// makeRuns is phase 1: sorted chunks spilled to disk.
func makeRuns(r io.Reader, dir string, limit int, less func(a, b string) bool, st *Stats) ([]string, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var runs []string
	chunk := make([]string, 0, limit)

	spill := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.SliceStable(chunk, func(i, j int) bool { return less(chunk[i], chunk[j]) })
		path := filepath.Join(dir, fmt.Sprintf("run-%06d", len(runs)))
		if err := writeLines(path, chunk); err != nil {
			return err
		}
		runs = append(runs, path)
		st.Runs++
		chunk = chunk[:0]
		return nil
	}

	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			chunk = append(chunk, strings.TrimSuffix(line, "\n"))
			st.Lines++
			if len(chunk) >= limit {
				if serr := spill(); serr != nil {
					return nil, serr
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return runs, spill()
		}
		if err != nil {
			return nil, err
		}
	}
}

func writeLines(path string, lines []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for _, l := range lines {
		bw.WriteString(l)
		bw.WriteByte('\n')
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func mergeIntoFile(paths []string, out string, less func(a, b string) bool) error {
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	err = mergeFiles(paths, less, func(line string) error {
		_, werr := bw.WriteString(line + "\n")
		return werr
	})
	if err == nil {
		err = bw.Flush()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// mergeFiles k-way merges sorted line files into emit.
func mergeFiles(paths []string, less func(a, b string) bool, emit func(string) error) error {
	var firstErr error
	files := make([]*os.File, 0, len(paths))
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	iters := make([]func() (string, bool), len(paths))
	for i, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		files = append(files, f)
		br := bufio.NewReaderSize(f, 64*1024)
		iters[i] = func() (string, bool) {
			line, err := br.ReadString('\n')
			if len(line) > 0 {
				return strings.TrimSuffix(line, "\n"), true
			}
			if err != nil && !errors.Is(err, io.EOF) && firstErr == nil {
				firstErr = err // an iterator cannot return an error: remember it
			}
			return "", false
		}
	}
	if err := KWayMerge(iters, less, emit); err != nil {
		return err
	}
	return firstErr
}
