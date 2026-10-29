package streamio_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"dataflow/internal/streamio"
	"dataflow/internal/testutil"
)

// Chapter 03 -- streaming I/O.   Run:  go test ./internal/streamio -run Ch03 -race -v

func TestCh03_Stats_CountsLinesBytesAndTheLongestLine(t *testing.T) {
	st, err := streamio.Stats(strings.NewReader("ab\ncdef\n\nx\n"))
	if err != nil {
		t.Fatal(err)
	}
	if want := (streamio.LineStats{Lines: 4, Bytes: 11, LongestLine: 4}); st != want {
		t.Fatalf("got %+v, want %+v", st, want)
	}
}

func TestCh03_Stats_CountsALastLineWithoutANewline(t *testing.T) {
	st, _ := streamio.Stats(strings.NewReader("one\ntwo"))
	if want := (streamio.LineStats{Lines: 2, Bytes: 7, LongestLine: 3}); st != want {
		t.Fatalf("got %+v, want %+v", st, want)
	}
}

func TestCh03_Stats_OfNothing_IsAllZeros(t *testing.T) {
	st, err := streamio.Stats(strings.NewReader(""))
	if err != nil || st != (streamio.LineStats{}) {
		t.Fatalf("got (%+v, %v), want zeros", st, err)
	}
}

func TestCh03_Stats_HandlesALineLongerThanTheScannerLimit(t *testing.T) {
	huge := strings.Repeat("x", 1<<20) // 1 MiB on one line: bufio.Scanner's default limit is 64 KiB
	st, err := streamio.Stats(strings.NewReader("short\n" + huge + "\nend"))
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if want := (streamio.LineStats{Lines: 3, Bytes: int64(6 + len(huge) + 1 + 3), LongestLine: len(huge)}); st != want {
		t.Fatalf("got %+v, want %+v", st, want)
	}
}

func TestCh03_Stats_ReturnsReadErrors(t *testing.T) {
	boom := errors.New("disk on fire")
	_, err := streamio.Stats(io.MultiReader(strings.NewReader("abc\n"), iotest.ErrReader(boom)))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want the read error", err)
	}
}

func TestCh03_WordFrequencies_FoldsCaseAndSplitsOnPunctuation(t *testing.T) {
	got, err := streamio.WordFrequencies(strings.NewReader("The cat, the HAT; the cat's 2nd café! Café."))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"the": 3, "cat": 2, "hat": 1, "s": 1, "2nd": 1, "café": 2}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for w, n := range want {
		if got[w] != n {
			t.Fatalf("count of %q = %d, want %d (all: %v)", w, got[w], n, got)
		}
	}
}

// lineGenerator produces `lines` lines on demand without ever holding them all: a stand-in for a huge file.
type lineGenerator struct {
	lines, next int
	pending     []byte
}

func (g *lineGenerator) Read(p []byte) (int, error) {
	if len(g.pending) == 0 {
		if g.next >= g.lines {
			return 0, io.EOF
		}
		g.pending = []byte(fmt.Sprintf("user%d visited page%d\n", g.next%100, g.next%7))
		g.next++
	}
	n := copy(p, g.pending)
	g.pending = g.pending[n:]
	return n, nil
}

func TestCh03_WordFrequencies_StreamsALargeInput(t *testing.T) {
	got, err := streamio.WordFrequencies(&lineGenerator{lines: 200_000})
	if err != nil {
		t.Fatal(err)
	}
	if got["visited"] != 200_000 || got["user7"] != 2000 || got["page3"] < 28_000 {
		t.Fatalf("unexpected counts: visited=%d user7=%d page3=%d", got["visited"], got["user7"], got["page3"])
	}
	if len(got) != 100+7+1 { // 100 users + 7 pages + "visited"
		t.Fatalf("%d distinct words, want 108", len(got))
	}
}

func TestCh03_ReadChunks_DeliversFullChunks_ThenTheRemainder(t *testing.T) {
	var sizes []int
	err := streamio.ReadChunks(strings.NewReader(strings.Repeat("a", 25)), 10, func(c []byte) error { sizes = append(sizes, len(c)); return nil })
	if err != nil || !slices.Equal(sizes, []int{10, 10, 5}) {
		t.Fatalf("err=%v sizes=%v, want [10 10 5]", err, sizes)
	}
}

func TestCh03_ReadChunks_OnAnExactMultiple_HasNoEmptyFinalChunk(t *testing.T) {
	var sizes []int
	_ = streamio.ReadChunks(strings.NewReader(strings.Repeat("a", 20)), 10, func(c []byte) error { sizes = append(sizes, len(c)); return nil })
	if !slices.Equal(sizes, []int{10, 10}) {
		t.Fatalf("sizes = %v, want [10 10]", sizes)
	}
}

func TestCh03_ReadChunks_ReusesOneBuffer(t *testing.T) {
	var first *byte
	chunks := 0
	_ = streamio.ReadChunks(strings.NewReader(strings.Repeat("a", 100)), 10, func(c []byte) error {
		if first == nil {
			first = &c[0]
		} else if &c[0] != first {
			t.Error("a new buffer was allocated for a later chunk: reuse ONE buffer")
		}
		chunks++
		return nil
	})
	if chunks != 10 {
		t.Fatalf("%d chunks, want 10", chunks)
	}
}

func TestCh03_ReadChunks_StillFillsChunks_WhenTheReaderReturnsOneByteAtATime(t *testing.T) {
	var sizes []int
	err := streamio.ReadChunks(iotest.OneByteReader(strings.NewReader(strings.Repeat("a", 25))), 10, func(c []byte) error { sizes = append(sizes, len(c)); return nil })
	if err != nil || !slices.Equal(sizes, []int{10, 10, 5}) {
		t.Fatalf("err=%v sizes=%v, want [10 10 5]", err, sizes)
	}
}

func TestCh03_ReadChunks_StopsOnTheCallbacksError_AndRejectsBadSizes(t *testing.T) {
	boom := errors.New("stop")
	calls := 0
	err := streamio.ReadChunks(strings.NewReader(strings.Repeat("a", 100)), 10, func([]byte) error { calls++; return boom })
	if !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want the callback's error after 1 call", err, calls)
	}
	testutil.ExpectError(t, streamio.ReadChunks(strings.NewReader("abc"), 0, func([]byte) error { return nil }))
}

func TestCh03_BufferPool_ReusesSmallBuffers_ResetsThem_AndDropsHugeOnes(t *testing.T) {
	pool := streamio.NewBufferPool(1024)

	// 1. It really pools: a Get/Write/Put loop allocates (almost) nothing. (Under -race sync.Pool deliberately drops about
	//    a quarter of the items to catch misuse, so the bound is "less than one allocation per round", not zero.)
	allocs := testing.AllocsPerRun(1000, func() {
		b := pool.Get()
		b.WriteString("some payload")
		pool.Put(b)
	})
	if allocs >= 1 {
		t.Fatalf("%.2f allocations per Get/Put round: buffers are not being reused", allocs)
	}

	// 2. A buffer that comes back is EMPTY.
	b := pool.Get()
	b.WriteString("old data")
	pool.Put(b)
	if got := pool.Get(); got.Len() != 0 {
		t.Fatalf("Get returned a buffer containing %q; it must be reset", got.String())
	}

	// 3. A buffer that grew beyond maxKeep is dropped, not pooled. Put(nil) is harmless.
	big := new(bytes.Buffer)
	big.Grow(1 << 20)
	pool.Put(big)
	pool.Put(nil)
	for range 20 {
		if got := pool.Get(); got.Cap() > 1024 {
			t.Fatalf("the pool handed out a buffer with capacity %d; buffers above maxKeep must not be pooled", got.Cap())
		}
	}
}

// ---- splitting a file at line boundaries ----

func makeFile(t *testing.T, lines []string, trailingNewline bool) string {
	t.Helper()
	content := strings.Join(lines, "\n")
	if trailingNewline && len(lines) > 0 {
		content += "\n"
	}
	return testutil.WriteFile(t, t.TempDir(), "data.txt", content)
}

func collect(t *testing.T, path string, splits []streamio.Split) []string {
	t.Helper()
	var all []string
	for _, s := range splits {
		if err := streamio.ProcessSplit(path, s, func(line []byte) error { all = append(all, string(line)); return nil }); err != nil {
			t.Fatalf("ProcessSplit(%+v): %v", s, err)
		}
	}
	return all
}

func variedLines(n int) []string {
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("line-%04d-%s", i, strings.Repeat("x", (i*7)%40))
	}
	return lines
}

func TestCh03_Splits_EveryLineLandsInExactlyOneRange_InOrder(t *testing.T) {
	lines := variedLines(1000)
	path := makeFile(t, lines, true)
	for _, n := range []int{1, 2, 3, 7, 50, 333} {
		splits, err := streamio.LineAlignedSplits(path, n)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if got := collect(t, path, splits); !slices.Equal(got, lines) {
			t.Fatalf("n=%d: the ranges do not reproduce the file exactly (got %d lines, want %d)", n, len(got), len(lines))
		}
	}
}

func TestCh03_Splits_AreContiguousNonEmptyAndCoverTheFile(t *testing.T) {
	path := makeFile(t, variedLines(500), true)
	splits, err := streamio.LineAlignedSplits(path, 8)
	if err != nil || len(splits) == 0 {
		t.Fatalf("splits=%v err=%v", splits, err)
	}
	if splits[0].Start != 0 {
		t.Fatalf("first range starts at %d, want 0", splits[0].Start)
	}
	for i, s := range splits {
		if s.End <= s.Start {
			t.Fatalf("range %d is empty: %+v", i, s)
		}
		if i > 0 && s.Start != splits[i-1].End {
			t.Fatalf("range %d starts at %d but the previous one ends at %d", i, s.Start, splits[i-1].End)
		}
	}
}

func TestCh03_Splits_NeverCutALineInHalf(t *testing.T) {
	lines := variedLines(300)
	path := makeFile(t, lines, true)
	splits, err := streamio.LineAlignedSplits(path, 11)
	if err != nil || len(splits) < 2 {
		t.Fatalf("splits=%v err=%v, want several ranges", splits, err)
	}
	content := strings.Join(lines, "\n") + "\n"
	for _, s := range splits[1:] {
		if content[s.Start-1] != '\n' {
			t.Fatalf("range starts at %d in the middle of a line (the previous byte is %q)", s.Start, content[s.Start-1])
		}
	}
}

func TestCh03_Splits_AreRoughlyBalanced(t *testing.T) {
	lines := variedLines(2000)
	path := makeFile(t, lines, true)
	size := int64(len(strings.Join(lines, "\n")) + 1)
	maxLine := int64(0)
	for _, l := range lines {
		maxLine = max(maxLine, int64(len(l)+1))
	}
	splits, _ := streamio.LineAlignedSplits(path, 10)
	for i, s := range splits {
		if got := s.End - s.Start; got > size/10+maxLine {
			t.Fatalf("range %d has %d bytes; an even split would be about %d (plus at most one line)", i, got, size/10)
		}
	}
	if len(splits) != 10 {
		t.Fatalf("%d ranges, want 10 for 2000 short lines", len(splits))
	}
}

func TestCh03_Splits_HandleAFileWithoutATrailingNewline(t *testing.T) {
	lines := variedLines(100)
	path := makeFile(t, lines, false)
	splits, _ := streamio.LineAlignedSplits(path, 6)
	if got := collect(t, path, splits); !slices.Equal(got, lines) {
		t.Fatalf("lost or duplicated lines: got %d, want %d", len(got), len(lines))
	}
}

func TestCh03_Splits_OneHugeLine_StaysInOneRange(t *testing.T) {
	huge := strings.Repeat("y", 300_000) // longer than any buffer
	lines := []string{"head", huge, "tail"}
	path := makeFile(t, lines, true)
	splits, err := streamio.LineAlignedSplits(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(t, path, splits); !slices.Equal(got, lines) {
		t.Fatalf("the huge line was damaged: got %d lines, lengths %v", len(got), func() []int {
			var l []int
			for _, g := range got {
				l = append(l, len(g))
			}
			return l
		}())
	}
}

func TestCh03_Splits_MoreRangesRequestedThanLines_GivesAtMostOnePerLine(t *testing.T) {
	lines := []string{"alpha", "beta", "gamma"}
	path := makeFile(t, lines, true)
	splits, err := streamio.LineAlignedSplits(path, 10)
	if err != nil || len(splits) > 3 || len(splits) == 0 {
		t.Fatalf("splits=%v err=%v, want 1 to 3 ranges", splits, err)
	}
	if got := collect(t, path, splits); !slices.Equal(got, lines) {
		t.Fatalf("got %v, want %v", got, lines)
	}
}

func TestCh03_Splits_EmptyFile_HasNoRanges_AndBadNIsAnError(t *testing.T) {
	empty := testutil.WriteFile(t, t.TempDir(), "empty.txt", "")
	splits, err := streamio.LineAlignedSplits(empty, 4)
	if err != nil || len(splits) != 0 {
		t.Fatalf("empty file: splits=%v err=%v, want none", splits, err)
	}
	_, err = streamio.LineAlignedSplits(makeFile(t, []string{"a"}, true), 0)
	testutil.ExpectError(t, err)
}

func TestCh03_ProcessSplit_PassesBlankLines_AndStopsOnTheCallbacksError(t *testing.T) {
	path := makeFile(t, []string{"a", "", "b"}, true)
	splits, _ := streamio.LineAlignedSplits(path, 1)
	if len(splits) != 1 {
		t.Fatalf("got %d ranges, want exactly 1", len(splits))
	}
	if got := collect(t, path, splits); !slices.Equal(got, []string{"a", "", "b"}) {
		t.Fatalf("got %q, want blank lines preserved", got)
	}
	boom := errors.New("stop")
	calls := 0
	err := streamio.ProcessSplit(path, splits[0], func([]byte) error { calls++; return boom })
	if !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want the callback's error after one call", err, calls)
	}
}

func TestCh03_Splits_CanBeProcessedInParallel_WithoutCoordination(t *testing.T) {
	lines := variedLines(5000)
	path := makeFile(t, lines, true)
	splits, err := streamio.LineAlignedSplits(path, 8)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	total := 0
	var wg sync.WaitGroup
	for _, s := range splits {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := 0
			if err := streamio.ProcessSplit(path, s, func([]byte) error { n++; return nil }); err != nil {
				t.Errorf("ProcessSplit: %v", err)
			}
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	if total != len(lines) {
		t.Fatalf("workers saw %d lines in total, want %d", total, len(lines))
	}
}
