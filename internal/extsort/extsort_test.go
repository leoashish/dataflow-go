package extsort_test

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"dataflow/internal/extsort"
	"dataflow/internal/testutil"
)

// Chapter 10 -- external merge sort.   Run:  go test ./internal/extsort -run Ch10 -race -v

// ---------------- KWayMerge ----------------

func sliceIter[T any](items []T) func() (T, bool) {
	i := 0
	return func() (T, bool) {
		if i >= len(items) {
			var zero T
			return zero, false
		}
		i++
		return items[i-1], true
	}
}

func collectMerge[T any](t *testing.T, iters []func() (T, bool), less func(a, b T) bool) []T {
	t.Helper()
	var out []T
	if err := extsort.KWayMerge(iters, less, func(v T) error { out = append(out, v); return nil }); err != nil {
		t.Fatalf("KWayMerge: %v", err)
	}
	return out
}

func intLess(a, b int) bool { return a < b }

func TestCh10_KWayMerge_MergesSortedSequences(t *testing.T) {
	got := collectMerge(t, []func() (int, bool){sliceIter([]int{1, 4, 9}), sliceIter([]int{2, 3, 10}), sliceIter([]int{5})}, intLess)
	if want := []int{1, 2, 3, 4, 5, 9, 10}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh10_KWayMerge_HandlesEmptyNilAndSingleSequences(t *testing.T) {
	if got := collectMerge(t, nil, intLess); len(got) != 0 {
		t.Fatalf("no iterators: got %v", got)
	}
	got := collectMerge(t, []func() (int, bool){sliceIter[int](nil), nil, sliceIter([]int{3, 4})}, intLess)
	if !slices.Equal(got, []int{3, 4}) {
		t.Fatalf("got %v, want [3 4]", got)
	}
}

type tagged struct {
	key int
	src string
}

func TestCh10_KWayMerge_IsStable_EqualItemsComeFromTheLowerNumberedSequenceFirst(t *testing.T) {
	a := []tagged{{1, "a"}, {2, "a"}, {2, "a2"}}
	b := []tagged{{2, "b"}, {3, "b"}}
	c := []tagged{{2, "c"}}
	got := collectMerge(t, []func() (tagged, bool){sliceIter(a), sliceIter(b), sliceIter(c)}, func(x, y tagged) bool { return x.key < y.key })
	want := []tagged{{1, "a"}, {2, "a"}, {2, "a2"}, {2, "b"}, {2, "c"}, {3, "b"}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCh10_KWayMerge_StopsWhenEmitFails(t *testing.T) {
	boom := errors.New("stop")
	n := 0
	err := extsort.KWayMerge([]func() (int, bool){sliceIter([]int{1, 2, 3, 4, 5})}, intLess, func(int) error {
		n++
		if n == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || n != 2 {
		t.Fatalf("err=%v emitted=%d, want the error after exactly 2 items", err, n)
	}
}

func TestCh10_KWayMerge_ManySequences_MatchesAFullSort(t *testing.T) {
	rnd := testutil.Rand(7)
	var iters []func() (int, bool)
	var all []int
	for range 25 {
		part := make([]int, rnd.Intn(200))
		for i := range part {
			part[i] = rnd.Intn(1000)
		}
		sort.Ints(part)
		all = append(all, part...)
		iters = append(iters, sliceIter(part))
	}
	sort.Ints(all)
	if got := collectMerge(t, iters, intLess); !slices.Equal(got, all) {
		t.Fatalf("merge of 25 sorted sequences differs from a full sort (%d vs %d items)", len(got), len(all))
	}
}

// ---------------- SortLines ----------------

func sortString(t *testing.T, input string, o extsort.Options) (string, extsort.Stats) {
	t.Helper()
	var sb strings.Builder
	st, err := extsort.SortLines(strings.NewReader(input), &sb, o)
	if err != nil {
		t.Fatalf("SortLines: %v", err)
	}
	return sb.String(), st
}

func numericLess(a, b string) bool {
	x, _ := strconv.Atoi(a)
	y, _ := strconv.Atoi(b)
	return x < y
}

func TestCh10_SortLines_SortsAndNormalisesTheFinalNewline(t *testing.T) {
	got, st := sortString(t, "pear\napple\nfig", extsort.Options{MaxLinesInMemory: 100, TmpDir: t.TempDir()})
	if got != "apple\nfig\npear\n" || st.Lines != 3 {
		t.Fatalf("got %q stats=%+v", got, st)
	}
}

func TestCh10_SortLines_EmptyInput_ProducesNothing(t *testing.T) {
	got, st := sortString(t, "", extsort.Options{MaxLinesInMemory: 10, TmpDir: t.TempDir()})
	if got != "" || st != (extsort.Stats{}) {
		t.Fatalf("got %q stats=%+v, want nothing and zero stats", got, st)
	}
}

func randomNumbers(n int) []string {
	rnd := testutil.Rand(42)
	out := make([]string, n)
	for i := range out {
		out[i] = strconv.Itoa(rnd.Intn(100_000))
	}
	return out
}

func TestCh10_SortLines_SortsMuchMoreDataThanItHoldsInMemory(t *testing.T) {
	nums := randomNumbers(10_000)
	tmp := t.TempDir()

	got, st := sortString(t, strings.Join(nums, "\n")+"\n", extsort.Options{MaxLinesInMemory: 100, TmpDir: tmp, Less: numericLess})

	want := slices.Clone(nums)
	sort.SliceStable(want, func(i, j int) bool { return numericLess(want[i], want[j]) })
	if got != strings.Join(want, "\n")+"\n" {
		t.Fatal("the output is not the sorted input")
	}
	if st.Lines != 10_000 || st.Runs != 100 {
		t.Fatalf("stats=%+v, want 10000 lines in exactly 100 runs of 100 lines (the memory budget)", st)
	}
	if st.MergePasses != 0 {
		t.Fatalf("MergePasses = %d, want 0 with unlimited fan-in (one final merge of 100 runs)", st.MergePasses)
	}
}

func TestCh10_SortLines_IsStable_AlsoAcrossRunBoundaries(t *testing.T) {
	// Sort by the first field only; lines with the same key must keep their input order.
	var lines []string
	for i := 0; i < 300; i++ {
		lines = append(lines, fmt.Sprintf("%d\tseq%03d", i%3, i))
	}
	keyLess := func(a, b string) bool { return strings.SplitN(a, "\t", 2)[0] < strings.SplitN(b, "\t", 2)[0] }

	got, _ := sortString(t, strings.Join(lines, "\n")+"\n", extsort.Options{MaxLinesInMemory: 7, TmpDir: t.TempDir(), Less: keyLess, MaxMergeFanIn: 3})

	want := slices.Clone(lines)
	sort.SliceStable(want, func(i, j int) bool { return keyLess(want[i], want[j]) })
	if got != strings.Join(want, "\n")+"\n" {
		t.Fatal("equal keys were reordered: runs must be sorted stably AND merged with ties broken by run order, in every pass")
	}
}

func TestCh10_SortLines_LimitedFanIn_MergesInPasses(t *testing.T) {
	nums := randomNumbers(1000)
	got, st := sortString(t, strings.Join(nums, "\n")+"\n", extsort.Options{MaxLinesInMemory: 10, TmpDir: t.TempDir(), Less: numericLess, MaxMergeFanIn: 4})

	want := slices.Clone(nums)
	sort.SliceStable(want, func(i, j int) bool { return numericLess(want[i], want[j]) })
	if got != strings.Join(want, "\n")+"\n" {
		t.Fatal("output is not sorted")
	}
	if st.Runs != 100 || st.MergePasses != 3 { // 100 -> 25 -> 7 -> 2 runs, then the final merge
		t.Fatalf("stats=%+v, want 100 runs and 3 intermediate passes", st)
	}
}

func TestCh10_SortLines_KeepsDuplicatesAndVeryLongLines(t *testing.T) {
	long := strings.Repeat("z", 200_000)
	got, _ := sortString(t, "b\n"+long+"\na\nb\na\n", extsort.Options{MaxLinesInMemory: 2, TmpDir: t.TempDir()})
	if want := "a\na\nb\nb\n" + long + "\n"; got != want {
		t.Fatalf("got %d bytes starting %q, want duplicates kept and the long line last", len(got), got[:min(20, len(got))])
	}
}

func TestCh10_SortLines_LeavesNoTemporaryFilesBehind(t *testing.T) {
	tmp := t.TempDir()
	sortString(t, strings.Join(randomNumbers(500), "\n"), extsort.Options{MaxLinesInMemory: 10, TmpDir: tmp, MaxMergeFanIn: 5})
	if names := testutil.DirEntries(t, tmp); len(names) != 0 {
		t.Fatalf("temporary files left in %s: %v", tmp, names)
	}
}

type failingWriter struct{ after int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.after -= len(p); w.after < 0 {
		return 0, errors.New("disk full")
	}
	return len(p), nil
}

func TestCh10_SortLines_ReportsWriterAndReaderErrors_AndStillCleansUp(t *testing.T) {
	tmp := t.TempDir()
	input := strings.Join(randomNumbers(300), "\n") + "\n"

	_, err := extsort.SortLines(strings.NewReader(input), &failingWriter{after: 100}, extsort.Options{MaxLinesInMemory: 10, TmpDir: tmp})
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("writer failure: err = %v, want 'disk full'", err)
	}

	boom := errors.New("network reset")
	_, err = extsort.SortLines(io.MultiReader(strings.NewReader(input), iotest.ErrReader(boom)), io.Discard, extsort.Options{MaxLinesInMemory: 10, TmpDir: tmp})
	if !errors.Is(err, boom) {
		t.Fatalf("reader failure: err = %v, want the read error", err)
	}
	if names := testutil.DirEntries(t, tmp); len(names) != 0 {
		t.Fatalf("temporary files left behind after errors: %v", names)
	}
}

func TestCh10_SortLines_RejectsBadOptions(t *testing.T) {
	for _, o := range []extsort.Options{
		{MaxLinesInMemory: 0},
		{MaxLinesInMemory: -5},
		{MaxLinesInMemory: 10, MaxMergeFanIn: 1},
		{MaxLinesInMemory: 10, MaxMergeFanIn: -2},
	} {
		o.TmpDir = t.TempDir()
		_, err := extsort.SortLines(strings.NewReader("a\n"), io.Discard, o)
		testutil.ExpectError(t, err)
	}
}
