package sketch_test

import (
	"errors"
	"math"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"dataflow/internal/sketch"
	"dataflow/internal/testutil"
)

// Chapter 18 -- sampling and streaming quantiles.   Run:  go test ./internal/sketch -run Ch18 -race -v

func TestCh18_Reservoir_RejectsBadParameters(t *testing.T) {
	_, err := sketch.NewReservoir[int](0, testutil.Rand(1))
	testutil.ExpectError(t, err)
	_, err = sketch.NewReservoir[int](5, nil)
	testutil.ExpectError(t, err)
	if !errors.Is(err, sketch.ErrBadParameter) {
		t.Fatalf("error %v does not wrap ErrBadParameter", err)
	}
}

func TestCh18_Reservoir_KeepsEverythingUntilItIsFull(t *testing.T) {
	r, err := sketch.NewReservoir[int](10, testutil.Rand(1))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		r.Add(i)
	}
	got := r.Sample()
	slices.Sort(got)
	if r.Seen() != 7 || !slices.Equal(got, []int{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("Seen=%d sample=%v, want all 7 items", r.Seen(), got)
	}
	for i := 7; i < 1000; i++ {
		r.Add(i)
	}
	if r.Seen() != 1000 || len(r.Sample()) != 10 {
		t.Fatalf("Seen=%d len(sample)=%d, want 1000 and exactly 10", r.Seen(), len(r.Sample()))
	}
}

func TestCh18_Reservoir_SampleIsACopy(t *testing.T) {
	r, _ := sketch.NewReservoir[int](3, testutil.Rand(1))
	for i := 0; i < 3; i++ {
		r.Add(i + 1)
	}
	s := r.Sample()
	if len(s) != 3 {
		t.Fatalf("Sample() has %d items, want 3", len(s))
	}
	for i := range s {
		s[i] = -1
	}
	for _, v := range r.Sample() {
		if v == -1 {
			t.Fatal("modifying the returned slice changed the reservoir")
		}
	}
}

func TestCh18_Reservoir_EveryPositionOfTheStreamIsEquallyLikelyToBeKept(t *testing.T) {
	const stream, k, trials = 100, 10, 20_000
	rnd := testutil.Rand(2)
	included := make([]int, stream)
	for trial := 0; trial < trials; trial++ {
		r, _ := sketch.NewReservoir[int](k, rnd)
		for i := 0; i < stream; i++ {
			r.Add(i)
		}
		for _, v := range r.Sample() {
			included[v]++
		}
	}
	want := float64(trials) * k / stream // 2000 each
	for pos, got := range included {
		if math.Abs(float64(got)-want) > want*0.15 {
			t.Fatalf("position %d of the stream was kept %d times, want about %.0f (+/-15%%): the sample is biased (early items %d, late items %d)",
				pos, got, want, included[0], included[stream-1])
		}
	}
}

func TestCh18_Reservoir_IsReproducibleWithTheSameSeed(t *testing.T) {
	run := func() []int {
		r, _ := sketch.NewReservoir[int](5, testutil.Rand(42))
		for i := 0; i < 500; i++ {
			r.Add(i)
		}
		return r.Sample()
	}
	a, b := run(), run()
	if len(a) != 5 || !slices.Equal(a, b) {
		t.Fatalf("the same seed gave %v then %v", a, b)
	}
}

// ---------------- P-square ----------------

func exactQuantile(data []float64, q float64) float64 {
	sorted := append([]float64(nil), data...)
	sort.Float64s(sorted)
	return sorted[int(math.Ceil(q*float64(len(sorted))))-1]
}

func feed(t *testing.T, q float64, data []float64) *sketch.P2 {
	t.Helper()
	p, err := sketch.NewP2(q)
	if err != nil {
		t.Fatalf("NewP2(%v): %v", q, err)
	}
	for _, x := range data {
		p.Add(x)
	}
	return p
}

func TestCh18_P2_RejectsBadQuantiles(t *testing.T) {
	for _, q := range []float64{0, 1, -0.5, 1.5, math.NaN()} {
		_, err := sketch.NewP2(q)
		testutil.ExpectError(t, err)
		if !errors.Is(err, sketch.ErrBadParameter) {
			t.Fatalf("NewP2(%v): %v does not wrap ErrBadParameter", q, err)
		}
	}
}

func TestCh18_P2_IsExactForFewerThanFiveValues(t *testing.T) {
	p := feed(t, 0.5, nil)
	if !math.IsNaN(p.Value()) {
		t.Fatalf("no observations: Value = %v, want NaN", p.Value())
	}
	p = feed(t, 0.5, []float64{5, 1, 3})
	if p.Value() != 3 || p.Count() != 3 {
		t.Fatalf("median of {5,1,3} = %v (count %d), want 3", p.Value(), p.Count())
	}
	if v := feed(t, 0.9, []float64{5, 1, 3}).Value(); v != 5 {
		t.Fatalf("p90 of {5,1,3} = %v, want 5", v)
	}
	if v := feed(t, 0.1, []float64{5, 1, 3, 9}).Value(); v != 1 {
		t.Fatalf("p10 of {5,1,3,9} = %v, want 1", v)
	}
}

func TestCh18_P2_IgnoresNaNAndInfinity(t *testing.T) {
	p := feed(t, 0.5, []float64{1, 2, 3, math.NaN(), math.Inf(1), math.Inf(-1), 4, 5, 6})
	if p.Count() != 6 {
		t.Fatalf("Count = %d, want 6 (NaN and Inf are ignored)", p.Count())
	}
	if math.IsNaN(p.Value()) || p.Value() < 2 || p.Value() > 5 {
		t.Fatalf("Value = %v", p.Value())
	}
}

func TestCh18_P2_TracksQuantilesOfAUniformStream(t *testing.T) {
	rnd := testutil.Rand(3)
	data := make([]float64, 100_000)
	for i := range data {
		data[i] = rnd.Float64()
	}
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		if got := feed(t, q, data).Value(); math.Abs(got-q) > 0.01 {
			t.Fatalf("q=%v of uniform(0,1): estimate %.4f, want within 0.01 of %.2f", q, got, q)
		}
	}
}

func TestCh18_P2_TracksQuantilesOfASkewedStream_LikeLatencies(t *testing.T) {
	rnd := testutil.Rand(4)
	data := make([]float64, 100_000)
	for i := range data {
		data[i] = math.Exp(rnd.NormFloat64()*0.8 + 3) // lognormal: a long right tail, like request latencies
	}
	for _, q := range []float64{0.5, 0.9, 0.99} {
		exact := exactQuantile(data, q)
		got := feed(t, q, data).Value()
		if rel := math.Abs(got-exact) / exact; rel > 0.04 {
			t.Fatalf("q=%v of lognormal: estimate %.3f vs exact %.3f (%.1f%% off), want within 4%%", q, got, exact, rel*100)
		}
	}
}

func TestCh18_P2_HandlesSortedInput_AscendingAndDescending(t *testing.T) {
	const n = 50_000
	asc := make([]float64, n)
	desc := make([]float64, n)
	for i := range asc {
		asc[i] = float64(i)
		desc[i] = float64(n - i)
	}
	for name, data := range map[string][]float64{"ascending": asc, "descending": desc} {
		exact := exactQuantile(data, 0.9)
		if got := feed(t, 0.9, data).Value(); math.Abs(got-exact)/exact > 0.03 {
			t.Fatalf("%s input: p90 estimate %.1f vs exact %.1f", name, got, exact)
		}
	}
}

func TestCh18_P2_AConstantStreamGivesThatConstant(t *testing.T) {
	data := make([]float64, 5_000)
	for i := range data {
		data[i] = 7.5
	}
	if got := feed(t, 0.99, data).Value(); got != 7.5 {
		t.Fatalf("estimate = %v, want exactly 7.5", got)
	}
}

func TestCh18_P2_UsesConstantMemoryHoweverLongTheStream(t *testing.T) {
	rnd := rand.New(rand.NewSource(5))
	p, _ := sketch.NewP2(0.95)
	allocs := testing.AllocsPerRun(1, func() {
		for i := 0; i < 200_000; i++ {
			p.Add(rnd.Float64())
		}
	})
	if allocs > 20 { // five markers, no growing slices: a tiny, FIXED number of allocations
		t.Fatalf("%.0f allocations over 200k values: P-square must not accumulate the data", allocs)
	}
	if p.Count() < 200_000 || math.Abs(p.Value()-0.95) > 0.02 {
		t.Fatalf("Count=%d Value=%.3f", p.Count(), p.Value())
	}
}
