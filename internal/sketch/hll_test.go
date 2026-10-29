package sketch_test

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"dataflow/internal/sketch"
	"dataflow/internal/testutil"
)

// Chapter 17 -- HyperLogLog.   Run:  go test ./internal/sketch -run Ch17 -race -v

func newHLL(t *testing.T, p int) *sketch.HLL {
	t.Helper()
	h, err := sketch.NewHLL(p)
	if err != nil {
		t.Fatalf("NewHLL(%d): %v", p, err)
	}
	return h
}

func addRange(h *sketch.HLL, prefix string, from, to int) {
	for i := from; i < to; i++ {
		h.Add(fmt.Sprintf("%s-%d", prefix, i))
	}
}

func relErr(got uint64, want int) float64 {
	return math.Abs(float64(got)-float64(want)) / float64(want)
}

func TestCh17_HLL_RejectsPrecisionsOutsideFourToSixteen(t *testing.T) {
	for _, p := range []int{-1, 0, 3, 17, 30} {
		_, err := sketch.NewHLL(p)
		testutil.ExpectError(t, err)
		if !errors.Is(err, sketch.ErrBadParameter) {
			t.Fatalf("NewHLL(%d): %v does not wrap ErrBadParameter", p, err)
		}
	}
	for _, p := range []int{4, 12, 16} {
		h := newHLL(t, p)
		if h.Precision() != p || h.SizeBytes() != 1<<p {
			t.Fatalf("p=%d: Precision=%d SizeBytes=%d", p, h.Precision(), h.SizeBytes())
		}
	}
}

func TestCh17_HLL_AnEmptySketchCountsZero_AndDuplicatesAreFree(t *testing.T) {
	h := newHLL(t, 12)
	if h.Count() != 0 {
		t.Fatalf("empty sketch counts %d, want 0", h.Count())
	}
	addRange(h, "user", 0, 1000)
	once := h.Count()
	if once < 900 || once > 1100 {
		t.Fatalf("1000 distinct items counted as %d", once)
	}
	for range 50 { // the same 1000 items, 50 more times
		addRange(h, "user", 0, 1000)
	}
	if h.Count() != once {
		t.Fatalf("count changed from %d to %d after re-adding the SAME items: repeated items hash identically and must change nothing", once, h.Count())
	}
}

func TestCh17_HLL_IsExactEnoughForTinyCounts(t *testing.T) {
	// With very few items almost every register is empty, and linear counting is near-exact.
	for _, n := range []int{1, 10, 100} {
		h := newHLL(t, 12)
		addRange(h, "tiny", 0, n)
		if got := h.Count(); math.Abs(float64(got)-float64(n)) > 1+float64(n)*0.02 {
			t.Fatalf("n=%d counted as %d: for small counts the estimate should be almost exact (linear counting)", n, got)
		}
	}
}

func TestCh17_HLL_StaysWithinTheStandardError_FromThousandsToHundredsOfThousands(t *testing.T) {
	// p = 12: 4096 registers, standard error 1.04/sqrt(4096) = 1.6%. Allow three standard errors.
	for _, n := range []int{1_000, 10_000, 100_000, 300_000} {
		h := newHLL(t, 12)
		addRange(h, "visitor", 0, n)
		if e := relErr(h.Count(), n); e > 0.05 {
			t.Fatalf("n=%d counted as %d (error %.1f%%), want within 5%%", n, h.Count(), e*100)
		}
	}
}

func TestCh17_HLL_MoreRegistersMeanLessError(t *testing.T) {
	const n = 200_000
	worst := func(p int) float64 {
		w := 0.0
		for _, prefix := range []string{"a", "b", "c", "d", "e", "f"} {
			h := newHLL(t, p)
			addRange(h, prefix, 0, n)
			w = math.Max(w, relErr(h.Count(), n))
		}
		return w
	}
	low, high := worst(8), worst(15) // 256 registers: ~6.5% error; 32768 registers: ~0.6%
	if high >= low {
		t.Fatalf("p=15 worst error %.2f%%, p=8 worst error %.2f%%: more registers must be more accurate", high*100, low*100)
	}
	if high > 0.03 {
		t.Fatalf("p=15 error %.2f%%, want under 3%%", high*100)
	}
}

func TestCh17_HLL_AHundredThousandTimesSmallerThanASet(t *testing.T) {
	h := newHLL(t, 14)
	if h.SizeBytes() != 16384 {
		t.Fatalf("p=14 is %d bytes, want 16384 (16 KB)", h.SizeBytes())
	}
	addRange(h, "u", 0, 500_000)
	if e := relErr(h.Count(), 500_000); e > 0.03 {
		t.Fatalf("500k distinct counted as %d in 16 KB (error %.2f%%)", h.Count(), e*100)
	}
}

func TestCh17_HLL_MergeIsTheUnion(t *testing.T) {
	a, b := newHLL(t, 12), newHLL(t, 12)
	addRange(a, "x", 0, 5_000)
	addRange(b, "x", 2_500, 7_500) // overlaps a on 2 500 items: the union has 7 500
	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if e := relErr(a.Count(), 7_500); e > 0.05 {
		t.Fatalf("union of 5000 and 5000 overlapping by 2500 counted as %d, want ~7500", a.Count())
	}
}

func TestCh17_HLL_MergeIsCommutative_Idempotent_AndLeavesTheArgumentAlone(t *testing.T) {
	a1, b1 := newHLL(t, 10), newHLL(t, 10)
	a2, b2 := newHLL(t, 10), newHLL(t, 10)
	for _, h := range []*sketch.HLL{a1, a2} {
		addRange(h, "left", 0, 3_000)
	}
	for _, h := range []*sketch.HLL{b1, b2} {
		addRange(h, "right", 0, 4_000)
	}
	bBefore := b1.Count()

	_ = a1.Merge(b1) // a ∪ b
	_ = b2.Merge(a2) // b ∪ a
	if a1.Count() != b2.Count() {
		t.Fatalf("a∪b counts %d but b∪a counts %d", a1.Count(), b2.Count())
	}
	before := a1.Count()
	_ = a1.Merge(b1) // merging the same sketch again changes nothing
	if a1.Count() != before {
		t.Fatalf("a second identical merge changed the count from %d to %d", before, a1.Count())
	}
	if b1.Count() != bBefore || before <= bBefore {
		t.Fatalf("argument count %d -> %d (it must not change), union %d", bBefore, b1.Count(), before)
	}
}

func TestCh17_HLL_RefusesToMergeDifferentPrecisions(t *testing.T) {
	a, b := newHLL(t, 10), newHLL(t, 12)
	if err := a.Merge(b); !errors.Is(err, sketch.ErrShapeMismatch) {
		t.Fatalf("error = %v, want ErrShapeMismatch", err)
	}
}

func TestCh17_HLL_DistributedCountDistinct_FourWorkersOneAnswer(t *testing.T) {
	// Four workers each see a shard of the data (with heavy overlap between shards); the driver merges their 4 KB sketches.
	const distinct = 40_000
	rnd := testutil.Rand(17)
	workers := make([]*sketch.HLL, 4)
	for i := range workers {
		workers[i] = newHLL(t, 12)
	}
	for i := 0; i < 400_000; i++ { // 400k events over 40k distinct users
		workers[rnd.Intn(4)].Add(fmt.Sprintf("user-%d", rnd.Intn(distinct)))
	}
	total := newHLL(t, 12)
	for _, w := range workers {
		if err := total.Merge(w); err != nil {
			t.Fatal(err)
		}
	}
	if e := relErr(total.Count(), distinct); e > 0.05 {
		t.Fatalf("merged count %d for %d distinct users (error %.1f%%)", total.Count(), distinct, e*100)
	}
}
