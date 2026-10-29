package extsort

import (
	"io"

	"dataflow/internal/todo"
)

// SortLines sorts the lines of r and writes them to w, using at most o.MaxLinesInMemory lines of memory at a time
// (plus a few buffers), however large the input.
//
// THE ALGORITHM (external merge sort):
//
//	Phase 1 -- make sorted RUNS:   read up to MaxLinesInMemory lines, sort them (sort.SliceStable with o.Less), write them
//	                               to a temporary file (one line per row); repeat until the input ends.
//	Phase 2 -- merge the runs:     k-way merge (KWayMerge) of the run files into w.
//	                               If there are more runs than MaxMergeFanIn, merge in PASSES: merge groups of MaxMergeFanIn
//	                               CONSECUTIVE runs into bigger runs, repeat until at most MaxMergeFanIn remain, then do the
//	                               final merge into w. (You cannot keep 100 000 files open at once.)
//
// Rules:
//   - Lines may be of any length (do not use bufio.Scanner's default). Every output line ends with "\n", even if the last
//     input line had none. A "line" never includes its "\n". Empty input -> no output, zero Stats.
//   - STABLE: lines that compare equal keep their input order (also across runs and passes: merge consecutive runs,
//     break ties by run order).
//   - Stats.Runs = runs created in phase 1; Stats.MergePasses = intermediate passes only. Example: 100 runs with
//     MaxMergeFanIn 4 -> 25 runs (pass 1) -> 7 (pass 2) -> 2 (pass 3) -> final merge: MergePasses = 3.
//   - Create ONE private subdirectory for the temp files (os.MkdirTemp(o.TmpDir, "extsort-")) and ALWAYS delete it
//     (defer os.RemoveAll), also when something fails. A test checks TmpDir is left empty, even after an error.
//   - MaxLinesInMemory < 1, or MaxMergeFanIn of 1 or less than 0, is a real error. Errors from r or w are returned.
//   - o.Less == nil means a < b.
//
// Hint: a small helper `mergeFiles(paths []string, less, emit func(string) error) error` that opens every file, builds one
// iterator per file (a bufio.Reader + ReadString('\n')), calls KWayMerge and closes the files serves both the intermediate
// passes and the final merge. Remember an iterator cannot return an error: stash the first read error in a variable.
func SortLines(r io.Reader, w io.Writer, o Options) (Stats, error) {
	return Stats{}, todo.NotImplemented("Ch10: SortLines")
}
