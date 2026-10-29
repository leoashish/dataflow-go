package sketch_test

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"dataflow/internal/sketch"
	"dataflow/internal/testutil"
)

// Chapter 15 -- the Bloom filter.   Run:  go test ./internal/sketch -run Ch15 -race -v

func newBloom(t *testing.T, n int, p float64) *sketch.Bloom {
	t.Helper()
	b, err := sketch.NewBloom(n, p)
	if err != nil {
		t.Fatalf("NewBloom(%d, %v): %v", n, p, err)
	}
	return b
}

func TestCh15_Bloom_IsSizedByTheClassicFormulas(t *testing.T) {
	tests := []struct {
		n    int
		p    float64
		m, k int
	}{
		{1000, 0.01, 9586, 7},
		{1_000_000, 0.001, 14_377_588, 10},
		{100, 0.5, 145, 1}, // a sloppy filter still needs one hash function
	}
	for _, c := range tests {
		b := newBloom(t, c.n, c.p)
		if b.Bits() != c.m || b.Hashes() != c.k {
			t.Fatalf("n=%d p=%v: %d bits and %d hashes, want %d and %d", c.n, c.p, b.Bits(), b.Hashes(), c.m, c.k)
		}
	}
}

func TestCh15_Bloom_RejectsBadParameters(t *testing.T) {
	for _, c := range []struct {
		n int
		p float64
	}{{0, 0.01}, {-5, 0.01}, {10, 0}, {10, 1}, {10, -0.1}, {10, 1.5}, {10, math.NaN()}} {
		_, err := sketch.NewBloom(c.n, c.p)
		testutil.ExpectError(t, err)
		if !errors.Is(err, sketch.ErrBadParameter) {
			t.Fatalf("NewBloom(%d, %v): error %v does not wrap ErrBadParameter", c.n, c.p, err)
		}
	}
}

func TestCh15_Bloom_NeverForgetsAnItemItWasGiven(t *testing.T) {
	b := newBloom(t, 10_000, 0.01)
	for i := 0; i < 10_000; i++ {
		b.Add(fmt.Sprintf("member-%d", i))
	}
	for i := 0; i < 10_000; i++ {
		if !b.Contains(fmt.Sprintf("member-%d", i)) {
			t.Fatalf("member-%d was added but Contains says it is definitely absent: a Bloom filter has NO false negatives", i)
		}
	}
}

func TestCh15_Bloom_FalsePositiveRateStaysNearTheTarget(t *testing.T) {
	b := newBloom(t, 10_000, 0.01)
	for i := 0; i < 10_000; i++ {
		b.Add(fmt.Sprintf("member-%d", i))
	}
	const probes = 100_000
	falsePositives := 0
	for i := 0; i < probes; i++ {
		if b.Contains(fmt.Sprintf("stranger-%d", i)) {
			falsePositives++
		}
	}
	observed := float64(falsePositives) / probes
	if observed < 0.004 || observed > 0.02 {
		t.Fatalf("observed false-positive rate %.4f, want about 0.01", observed)
	}
	if est := b.EstimatedFPR(); est < observed/2 || est > observed*2 {
		t.Fatalf("EstimatedFPR() = %.4f but the observed rate is %.4f: they should agree within a factor of 2", est, observed)
	}
}

func TestCh15_Bloom_AFullFilterIsAboutHalfOnes_AndOverfillingMakesItWorse(t *testing.T) {
	b := newBloom(t, 5_000, 0.01)
	if b.FillRatio() != 0 {
		t.Fatalf("an empty filter has fill ratio %v, want 0", b.FillRatio())
	}
	for i := 0; i < 5_000; i++ {
		b.Add(fmt.Sprint("x", i))
	}
	fill := b.FillRatio()
	if fill < 0.4 || fill > 0.6 {
		t.Fatalf("fill ratio at capacity = %.3f, want about 0.5 (that is what the optimal k achieves)", fill)
	}
	atCapacity := b.EstimatedFPR()
	for i := 5_000; i < 20_000; i++ { // 4x over capacity
		b.Add(fmt.Sprint("x", i))
	}
	if b.EstimatedFPR() < atCapacity*20 {
		t.Fatalf("estimated FPR went from %.4f to %.4f when overfilled 4x: it should explode", atCapacity, b.EstimatedFPR())
	}
}

func TestCh15_Bloom_MergeIsTheUnion(t *testing.T) {
	a, b := newBloom(t, 2_000, 0.01), newBloom(t, 2_000, 0.01)
	for i := 0; i < 1_000; i++ {
		a.Add(fmt.Sprint("a", i))
		b.Add(fmt.Sprint("b", i))
	}
	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	for i := 0; i < 1_000; i++ {
		if !a.Contains(fmt.Sprint("a", i)) || !a.Contains(fmt.Sprint("b", i)) {
			t.Fatalf("after Merge item %d of one of the sets is missing", i)
		}
	}
	if b.Contains("a1") && b.Contains("a2") && b.Contains("a3") && b.Contains("a4") && b.Contains("a5") && b.Contains("a6") {
		t.Fatal("Merge modified its argument")
	}
}

func TestCh15_Bloom_RefusesToMergeDifferentShapes(t *testing.T) {
	a, b := newBloom(t, 1_000, 0.01), newBloom(t, 5_000, 0.01)
	if err := a.Merge(b); !errors.Is(err, sketch.ErrShapeMismatch) {
		t.Fatalf("error = %v, want ErrShapeMismatch", err)
	}
	c := newBloom(t, 1_000, 0.001) // same n, different rate -> different m and k
	if err := a.Merge(c); !errors.Is(err, sketch.ErrShapeMismatch) {
		t.Fatalf("error = %v, want ErrShapeMismatch", err)
	}
}

func TestCh15_Bloom_UsesAFractionOfTheMemoryOfAnExactSet(t *testing.T) {
	// 1 million items at 1% false positives: about 1.2 MB, however long the keys are.
	b := newBloom(t, 1_000_000, 0.01)
	if bytes := b.Bits() / 8; bytes > 1_300_000 {
		t.Fatalf("%d bytes for 1M items at 1%%", bytes)
	}
	b.Add("a-very-very-long-key-" + string(make([]byte, 1000)))
	if !b.Contains("a-very-very-long-key-" + string(make([]byte, 1000))) {
		t.Fatal("long keys must work too")
	}
}
