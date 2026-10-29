package agg_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"

	"dataflow/internal/agg"
	"dataflow/internal/testutil"
)

// Chapter 11 -- aggregation.   Run:  go test ./internal/agg -run Ch11 -race -v

func near(a, b, relTol float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) <= relTol*math.Max(math.Abs(a), math.Abs(b))+1e-9
}

func TestCh11_Stat_TracksCountSumMinMaxAndMean(t *testing.T) {
	var s agg.Stat
	for _, v := range []float64{4, 8, 6, 2} {
		s.Add(v)
	}
	if s.Count != 4 || s.Sum != 20 || s.Min != 2 || s.Max != 8 || s.Mean() != 5 {
		t.Fatalf("got %+v mean=%v, want count 4 sum 20 min 2 max 8 mean 5", s, s.Mean())
	}
}

func TestCh11_Stat_ComputesTheSampleVariance(t *testing.T) {
	var s agg.Stat
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} { // population variance 4, sample variance 32/7
		s.Add(v)
	}
	if !near(s.Variance(), 32.0/7.0, 1e-12) || !near(s.StdDev(), math.Sqrt(32.0/7.0), 1e-12) {
		t.Fatalf("variance = %v, want %v", s.Variance(), 32.0/7.0)
	}
}

func TestCh11_Stat_EmptyAndSingleValueAreSafe(t *testing.T) {
	var s agg.Stat
	if s.Mean() != 0 || s.Variance() != 0 {
		t.Fatalf("empty: mean=%v variance=%v, want zeros", s.Mean(), s.Variance())
	}
	s.Add(5)
	if s.Mean() != 5 || s.Variance() != 0 || s.Min != 5 || s.Max != 5 {
		t.Fatalf("one value: %+v mean=%v var=%v", s, s.Mean(), s.Variance())
	}
}

func TestCh11_Stat_VarianceIsNumericallyStable_ForValuesFarFromZero(t *testing.T) {
	// Deviations from the mean are -6, -3, 3, 6 -> sample variance exactly 30. The naive E[x^2]-E[x]^2 formula loses
	// every significant digit when the values are around 1e9 (squares around 1e18 in float64).
	var s agg.Stat
	for _, v := range []float64{1e9 + 4, 1e9 + 7, 1e9 + 13, 1e9 + 16} {
		s.Add(v)
	}
	if !near(s.Variance(), 30, 1e-6) {
		t.Fatalf("variance = %v, want 30 (use Welford's algorithm, not sum of squares)", s.Variance())
	}
}

func randomValues(n int, seed int64, offset float64) []float64 {
	rnd := testutil.Rand(seed)
	out := make([]float64, n)
	for i := range out {
		out[i] = offset + rnd.NormFloat64()*10
	}
	return out
}

func TestCh11_Stat_MergeEqualsOneSinglePass(t *testing.T) {
	values := randomValues(10_000, 1, 1e6)
	var whole, left, right agg.Stat
	for i, v := range values {
		whole.Add(v)
		if i < 3000 {
			left.Add(v)
		} else {
			right.Add(v)
		}
	}

	if whole.Count != 10_000 || left.Count != 3000 || right.Count != 7000 {
		t.Fatalf("counts: whole=%d left=%d right=%d, want 10000, 3000, 7000", whole.Count, left.Count, right.Count)
	}
	left.Merge(right)

	if left.Count != whole.Count || left.Min != whole.Min || left.Max != whole.Max || !near(left.Sum, whole.Sum, 1e-12) {
		t.Fatalf("merged %+v, single pass %+v", left, whole)
	}
	if !near(left.Mean(), whole.Mean(), 1e-12) || !near(left.Variance(), whole.Variance(), 1e-9) {
		t.Fatalf("merged mean=%v var=%v, single pass mean=%v var=%v", left.Mean(), left.Variance(), whole.Mean(), whole.Variance())
	}
}

func TestCh11_Stat_MergeWithEmpty_IsHarmless_AndDoesNotModifyTheArgument(t *testing.T) {
	var a, empty agg.Stat
	a.Add(3)
	a.Add(9)
	before := a
	a.Merge(empty)
	if a != before {
		t.Fatalf("merging an empty Stat changed %+v into %+v", before, a)
	}
	empty.Merge(a)
	if empty.Count != 2 || empty.Min != 3 || empty.Max != 9 || empty.Mean() != 6 {
		t.Fatalf("merging into an empty Stat: %+v", empty)
	}
	arg := a
	var c agg.Stat
	c.Add(100)
	c.Merge(arg)
	if arg != a {
		t.Fatal("Merge modified its argument")
	}
}

func TestCh11_Stat_ManyPartsMergeToTheSameAnswer(t *testing.T) {
	values := randomValues(8000, 2, -5e8)
	var whole agg.Stat
	parts := make([]agg.Stat, 16)
	for i, v := range values {
		whole.Add(v)
		parts[i%16].Add(v)
	}
	var merged agg.Stat
	for _, p := range parts {
		merged.Merge(p)
	}
	// Values near 5e8 are only representable to about 6e-8, so the two summation orders can legitimately differ in the
	// 9th significant digit of the variance: compare to 1e-7, not to machine precision.
	if !near(merged.Variance(), whole.Variance(), 1e-7) || !near(merged.Mean(), whole.Mean(), 1e-12) || merged.Count != 8000 {
		t.Fatalf("merged var=%v mean=%v, single pass var=%v mean=%v", merged.Variance(), merged.Mean(), whole.Variance(), whole.Mean())
	}
}

type sale struct {
	region string
	amount float64
}

func sales(n int) []sale {
	rnd := testutil.Rand(11)
	regions := []string{"north", "south", "east", "west", "centre"}
	out := make([]sale, n)
	for i := range out {
		out[i] = sale{region: regions[rnd.Intn(len(regions))], amount: float64(rnd.Intn(10_000)) / 100}
	}
	return out
}

func region(s sale) string  { return s.region }
func amount(s sale) float64 { return s.amount }

func TestCh11_GroupBy_SummarisesEachGroup(t *testing.T) {
	got := agg.GroupBy([]sale{{"a", 1}, {"b", 10}, {"a", 3}, {"b", 20}, {"a", 5}}, region, amount)
	if len(got) != 2 || got["a"].Count != 3 || got["a"].Sum != 9 || got["a"].Max != 5 || got["b"].Mean() != 15 {
		t.Fatalf("got %+v", got)
	}
	if empty := agg.GroupBy[sale](nil, region, amount); empty == nil || len(empty) != 0 {
		t.Fatalf("empty input: got %v, want an empty non-nil map", empty)
	}
}

func TestCh11_ParallelGroupBy_AgreesWithTheSequentialVersion(t *testing.T) {
	testutil.NoLeaks(t)
	data := sales(100_000)
	want := agg.GroupBy(data, region, amount)

	for _, workers := range []int{1, 2, 7, 16} {
		got, err := agg.ParallelGroupBy(context.Background(), data, workers, region, amount)
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if len(got) != len(want) {
			t.Fatalf("workers=%d: %d groups, want %d", workers, len(got), len(want))
		}
		for k, w := range want {
			g := got[k]
			if g.Count != w.Count || !near(g.Sum, w.Sum, 1e-9) || g.Min != w.Min || g.Max != w.Max || !near(g.Variance(), w.Variance(), 1e-9) {
				t.Fatalf("workers=%d group %s: got %+v, want %+v", workers, k, g, w)
			}
		}
	}
}

func TestCh11_ParallelGroupBy_HandlesMoreWorkersThanItems_ZeroWorkers_AndEmptyInput(t *testing.T) {
	testutil.NoLeaks(t)
	got, err := agg.ParallelGroupBy(context.Background(), []sale{{"a", 1}, {"a", 2}}, 50, region, amount)
	if err != nil || got["a"].Count != 2 {
		t.Fatalf("got (%v, %v)", got, err)
	}
	got, err = agg.ParallelGroupBy(context.Background(), []sale{{"a", 1}}, 0, region, amount)
	if err != nil || got["a"].Count != 1 {
		t.Fatalf("workers=0: got (%v, %v)", got, err)
	}
	got, err = agg.ParallelGroupBy(context.Background(), nil, 4, region, amount)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty: got (%v, %v), want an empty non-nil map", got, err)
	}
}

func TestCh11_ParallelGroupBy_ReturnsTheContextError_WhenCancelled(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := agg.ParallelGroupBy(ctx, sales(1000), 4, region, amount); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

// ---------------- MapReduce ----------------

var docs = []string{
	"the cat sat on the mat",
	"the dog sat on the log",
	"a cat and a dog",
	"",
	"the end",
}

func wordCountMap(doc string, emit func(string, int)) {
	for _, w := range strings.Fields(doc) {
		emit(w, 1)
	}
}

func sumInts(_ string, values []int) int {
	total := 0
	for _, v := range values {
		total += v
	}
	return total
}

func TestCh11_MapReduce_WordCount(t *testing.T) {
	testutil.NoLeaks(t)
	got, err := agg.MapReduce(context.Background(), docs, wordCountMap, sumInts, agg.MapReduceOptions{Mappers: 3, Reducers: 4})
	if err != nil {
		t.Fatalf("MapReduce: %v", err)
	}
	want := map[string]int{"the": 5, "cat": 2, "sat": 2, "on": 2, "mat": 1, "dog": 2, "log": 1, "a": 2, "and": 1, "end": 1}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for w, n := range want {
		if got[w] != n {
			t.Fatalf("count[%q] = %d, want %d (all: %v)", w, got[w], n, got)
		}
	}
}

func TestCh11_MapReduce_EveryKeyIsReducedExactlyOnce_WithTheValuesOfAllMappers(t *testing.T) {
	testutil.NoLeaks(t)
	var mu sync.Mutex
	calls := map[string]int{}
	_, err := agg.MapReduce(context.Background(), docs, wordCountMap, func(key string, values []int) int {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		return len(values)
	}, agg.MapReduceOptions{Mappers: 4, Reducers: 3})
	if err != nil {
		t.Fatal(err)
	}
	for key, n := range calls {
		if n != 1 {
			t.Fatalf("key %q was reduced %d times; the shuffle must send all its values to ONE reducer", key, n)
		}
	}
	if len(calls) != 10 {
		t.Fatalf("%d keys reduced, want 10", len(calls))
	}
}

func TestCh11_MapReduce_WorksForAnyShape_InvertedIndex(t *testing.T) {
	testutil.NoLeaks(t)
	inputs := []int{0, 1, 2, 3, 4}
	got, err := agg.MapReduce(context.Background(), inputs,
		func(i int, emit func(string, int)) {
			for _, w := range strings.Fields(docs[i]) {
				emit(w, i)
			}
		},
		func(_ string, ids []int) string { // reduceFn must not depend on the order of values: sort and de-duplicate
			sort.Ints(ids)
			var out []string
			for i, id := range ids {
				if i == 0 || id != ids[i-1] {
					out = append(out, fmt.Sprint(id))
				}
			}
			return strings.Join(out, ",")
		},
		agg.MapReduceOptions{Mappers: 2, Reducers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got["cat"] != "0,2" || got["the"] != "0,1,4" || got["end"] != "4" {
		t.Fatalf("index = %v", got)
	}
}

func TestCh11_MapReduce_DefaultsAndEmptyInput(t *testing.T) {
	testutil.NoLeaks(t)
	got, err := agg.MapReduce(context.Background(), docs[:1], wordCountMap, sumInts, agg.MapReduceOptions{}) // 0 -> 1
	if err != nil || got["the"] != 2 {
		t.Fatalf("got (%v, %v)", got, err)
	}
	got, err = agg.MapReduce(context.Background(), []string(nil), wordCountMap, sumInts, agg.MapReduceOptions{Mappers: 3, Reducers: 3})
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty input: (%v, %v), want an empty non-nil map", got, err)
	}
}

func TestCh11_MapReduce_BigRun_AgreesWithADirectCount(t *testing.T) {
	testutil.NoLeaks(t)
	rnd := testutil.Rand(5)
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	inputs := make([]string, 2000)
	want := map[string]int{}
	for i := range inputs {
		var parts []string
		for range 30 {
			w := words[rnd.Intn(len(words))]
			parts = append(parts, w)
			want[w]++
		}
		inputs[i] = strings.Join(parts, " ")
	}
	got, err := agg.MapReduce(context.Background(), inputs, wordCountMap, sumInts, agg.MapReduceOptions{Mappers: 8, Reducers: 5})
	if err != nil {
		t.Fatal(err)
	}
	for w, n := range want {
		if got[w] != n {
			t.Fatalf("count[%q] = %d, want %d", w, got[w], n)
		}
	}
}

func TestCh11_MapReduce_ReturnsTheContextError_WhenCancelled(t *testing.T) {
	testutil.NoLeaks(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := agg.MapReduce(ctx, docs, wordCountMap, sumInts, agg.MapReduceOptions{Mappers: 2, Reducers: 2}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
