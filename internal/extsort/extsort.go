// Package extsort sorts data that does not fit in memory: sorted runs on disk, merged with a heap.
package extsort

// Options configures SortLines.
type Options struct {
	MaxLinesInMemory int                    // the memory budget, in lines: never hold more than this many at once (must be >= 1)
	TmpDir           string                 // where to put temporary run files ("" = the system temp directory)
	Less             func(a, b string) bool // the ordering; nil means plain byte-wise a < b
	MaxMergeFanIn    int                    // how many run files one merge step may have open: 0 = unlimited, 1 is invalid, >= 2 limits it
}

// Stats reports what happened (tests use it to check you really spilled and merged in passes).
type Stats struct {
	Lines       int // lines read and written
	Runs        int // sorted runs created in phase 1 (a run is one memory-full of sorted lines)
	MergePasses int // INTERMEDIATE merge passes (those before the final merge into the writer)
}
