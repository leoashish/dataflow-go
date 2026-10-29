package sketch_test

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"dataflow/internal/sketch"
	"dataflow/internal/testutil"
)

// Chapter 16 -- Count-Min sketch and heavy hitters.   Run:  go test ./internal/sketch -run Ch16 -race -v

// zipfStream returns n events drawn from a Zipf distribution over `distinct` keys -- like page views: a few keys are
// extremely popular, a long tail is rare -- and the exact counts.
func zipfStream(n, distinct int, seed int64) ([]string, map[string]uint64) {
	rnd := testutil.Rand(seed)
	z := rand.NewZipf(rnd, 1.3, 1, uint64(distinct-1))
	events := make([]string, n)
	exact := map[string]uint64{}
	for i := range events {
		events[i] = fmt.Sprintf("page-%d", z.Uint64())
		exact[events[i]]++
	}
	return events, exact
}

func TestCh16_CountMin_IsSizedByEpsilonAndDelta(t *testing.T) {
	c, err := sketch.NewCountMin(0.001, 0.01)
	if err != nil {
		t.Fatal(err)
	}
	if c.Width() != 2719 || c.Depth() != 5 { // ceil(e/0.001) = 2719, ceil(ln 100) = 5
		t.Fatalf("grid is %d wide x %d deep, want 2719 x 5", c.Width(), c.Depth())
	}
}

func TestCh16_CountMin_RejectsBadParameters(t *testing.T) {
	for _, c := range [][2]float64{{0, 0.1}, {0.1, 0}, {1, 0.1}, {0.1, 1}, {-1, 0.1}} {
		_, err := sketch.NewCountMin(c[0], c[1])
		testutil.ExpectError(t, err)
		if !errors.Is(err, sketch.ErrBadParameter) {
			t.Fatalf("NewCountMin(%v, %v): %v does not wrap ErrBadParameter", c[0], c[1], err)
		}
	}
}

func TestCh16_CountMin_NeverUndercounts_AndStaysWithinTheErrorBound(t *testing.T) {
	events, exact := zipfStream(200_000, 20_000, 1)
	const epsilon, delta = 0.001, 0.01
	c, _ := sketch.NewCountMin(epsilon, delta)
	for _, e := range events {
		c.Add(e, 1)
	}
	if c.Total() != 200_000 {
		t.Fatalf("Total = %d, want 200000", c.Total())
	}
	bound := uint64(epsilon * float64(c.Total())) // 200
	overBound, worst := 0, uint64(0)
	for key, truth := range exact {
		est := c.Estimate(key)
		if est < truth {
			t.Fatalf("Estimate(%q) = %d is BELOW the true count %d: a Count-Min sketch can only over-estimate", key, est, truth)
		}
		if est-truth > bound {
			overBound++
		}
		worst = max(worst, est-truth)
	}
	if frac := float64(overBound) / float64(len(exact)); frac > 3*delta {
		t.Fatalf("%.1f%% of keys exceed the epsilon*N = %d bound (worst error %d); the guarantee is at most ~%.0f%%", frac*100, bound, worst, delta*100)
	}
}

func TestCh16_CountMin_HeavyKeysAreAccurate_AbsentKeysAreNearZero(t *testing.T) {
	events, exact := zipfStream(200_000, 20_000, 2)
	c, _ := sketch.NewCountMin(0.001, 0.01)
	for _, e := range events {
		c.Add(e, 1)
	}
	if truth, est := exact["page-0"], c.Estimate("page-0"); truth < 10_000 || est < truth || est > truth+300 {
		t.Fatalf("hottest key: truth %d, estimate %d (want truth <= estimate <= truth+300)", truth, est)
	}
	if est := c.Estimate("never-seen-this-key"); est > 400 {
		t.Fatalf("an absent key is estimated at %d; collisions should add at most ~epsilon*N = 200 (rarely more)", est)
	}
}

func TestCh16_CountMin_AddsWeightedCounts_AndMergeEqualsOneBigSketch(t *testing.T) {
	events, _ := zipfStream(60_000, 5_000, 3)
	whole, _ := sketch.NewCountMin(0.01, 0.01)
	left, _ := sketch.NewCountMin(0.01, 0.01)
	right, _ := sketch.NewCountMin(0.01, 0.01)
	for i, e := range events {
		whole.Add(e, 2)
		if i%2 == 0 {
			left.Add(e, 2)
		} else {
			right.Add(e, 2)
		}
	}
	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if left.Total() != whole.Total() || whole.Total() != 120_000 {
		t.Fatalf("totals: merged %d, whole %d, want 120000", left.Total(), whole.Total())
	}
	for _, key := range []string{"page-0", "page-1", "page-17", "page-4999", "absent"} {
		if left.Estimate(key) != whole.Estimate(key) {
			t.Fatalf("Estimate(%q): merged %d, single sketch %d: merging must be exact", key, left.Estimate(key), whole.Estimate(key))
		}
	}
}

func TestCh16_CountMin_RefusesToMergeDifferentShapes(t *testing.T) {
	a, _ := sketch.NewCountMin(0.01, 0.01)
	b, _ := sketch.NewCountMin(0.001, 0.01)
	if err := a.Merge(b); !errors.Is(err, sketch.ErrShapeMismatch) {
		t.Fatalf("error = %v, want ErrShapeMismatch", err)
	}
}

// ---------------- Space-Saving ----------------

func TestCh16_SpaceSaving_RejectsKBelowOne(t *testing.T) {
	_, err := sketch.NewSpaceSaving(0)
	testutil.ExpectError(t, err)
}

func TestCh16_SpaceSaving_IsExactWhenThereAreFewDistinctItems(t *testing.T) {
	s, _ := sketch.NewSpaceSaving(5)
	for _, it := range []string{"a", "b", "a", "c", "a", "b", "d"} {
		s.Offer(it)
	}
	top := s.Top()
	want := []sketch.Counter{{Item: "a", Count: 3}, {Item: "b", Count: 2}, {Item: "c", Count: 1}, {Item: "d", Count: 1}}
	if s.N() != 7 || len(top) != 4 {
		t.Fatalf("N=%d top=%v", s.N(), top)
	}
	for i := range want {
		if top[i] != want[i] {
			t.Fatalf("Top()[%d] = %+v, want %+v (exact counts, Err 0, ties by item name)", i, top[i], want[i])
		}
	}
}

func TestCh16_SpaceSaving_FindsTheHeavyHittersInALongNoisyStream(t *testing.T) {
	rnd := testutil.Rand(7)
	const n = 60_000
	truth := map[string]uint64{}
	s, _ := sketch.NewSpaceSaving(20)
	for i := 0; i < n; i++ {
		var item string
		switch r := rnd.Intn(100); {
		case r < 30:
			item = "HOT-1" // 30%
		case r < 50:
			item = "HOT-2" // 20%
		case r < 60:
			item = "HOT-3" // 10%
		default:
			item = fmt.Sprintf("noise-%d", rnd.Intn(5000)) // 40% spread over 5000 keys
		}
		truth[item]++
		s.Offer(item)
	}

	top := s.Top()
	if len(top) != 20 {
		t.Fatalf("%d counters, want exactly 20", len(top))
	}
	for i, want := range []string{"HOT-1", "HOT-2", "HOT-3"} {
		if top[i].Item != want {
			t.Fatalf("rank %d is %q, want %q; top: %v", i+1, top[i].Item, want, top[:5])
		}
	}
	for _, c := range top { // the documented guarantee, for EVERY counter
		if c.Count < truth[c.Item] || c.Count-c.Err > truth[c.Item] {
			t.Fatalf("%q: truth %d is outside [Count-Err, Count] = [%d, %d]", c.Item, truth[c.Item], c.Count-c.Err, c.Count)
		}
	}
}

func TestCh16_SpaceSaving_EveryItemAboveNOverKIsMonitored(t *testing.T) {
	const n, k = 40_000, 10
	events, truth := zipfStream(n, 2_000, 11) // a few very hot keys and a long tail
	s, _ := sketch.NewSpaceSaving(k)
	for _, e := range events {
		s.Offer(e)
	}
	monitored := map[string]bool{}
	for _, c := range s.Top() {
		monitored[c.Item] = true
	}
	var frequent []string
	for item, f := range truth {
		if f > n/k {
			frequent = append(frequent, item)
		}
	}
	sort.Strings(frequent)
	if len(frequent) < 2 {
		t.Fatalf("the test data has only %d items above N/k = %d", len(frequent), n/k)
	}
	for _, item := range frequent {
		if !monitored[item] {
			t.Fatalf("%q occurred %d times (> N/k = %d) yet is not among the %d counters", item, truth[item], n/k, k)
		}
	}
}

func TestCh16_SpaceSaving_TopIsACopy(t *testing.T) {
	s, _ := sketch.NewSpaceSaving(3)
	s.Offer("x")
	top := s.Top()
	if len(top) != 1 {
		t.Fatalf("Top() = %v", top)
	}
	top[0].Count = 999
	if got := s.Top(); got[0].Count != 1 {
		t.Fatal("modifying the slice returned by Top changed the summary")
	}
}
