package partition_test

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"

	"dataflow/internal/hashx"
	"dataflow/internal/partition"
	"dataflow/internal/testutil"
)

// Chapter 13 -- partitioning, skew, consistent hashing.   Run:  go test ./internal/partition -run Ch13 -race -v

func keys(n int, prefix string) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

func TestCh13_Hash_IsDeterministic_InRange_AndSpreadsKeysEvenly(t *testing.T) {
	const n = 16
	counts := make([]int, n)
	for _, k := range keys(100_000, "user-") {
		p, err := partition.Hash(k, n)
		if err != nil {
			t.Fatalf("Hash(%q): %v", k, err)
		}
		if p < 0 || p >= n {
			t.Fatalf("Hash(%q, %d) = %d, out of range", k, n, p)
		}
		if again, _ := partition.Hash(k, n); again != p {
			t.Fatalf("Hash is not deterministic for %q: %d then %d", k, p, again)
		}
		counts[p]++
	}
	if ratio := partition.SkewRatio(counts); ratio > 1.08 {
		t.Fatalf("skew ratio %.3f over %v: a good hash spreads sequential keys evenly", ratio, counts)
	}
}

func TestCh13_Hash_RejectsFewerThanOnePartition_AndOneBucketTakesEverything(t *testing.T) {
	for _, n := range []int{0, -3} {
		_, err := partition.Hash("k", n)
		testutil.ExpectError(t, err)
		if !errors.Is(err, partition.ErrBadPartitionCount) {
			t.Fatalf("error = %v, want one wrapping ErrBadPartitionCount", err)
		}
	}
	if p, err := partition.Hash("anything", 1); err != nil || p != 0 {
		t.Fatalf("Hash(_, 1) = (%d, %v), want (0, nil)", p, err)
	}
}

func TestCh13_Range_PlacesKeysBetweenBoundaries_EqualGoesToTheUpperPartition(t *testing.T) {
	rp, err := partition.NewRangePartitioner([]string{"g", "n", "t"})
	if err != nil {
		t.Fatalf("NewRangePartitioner: %v", err)
	}
	if rp.Partitions() != 4 {
		t.Fatalf("Partitions = %d, want 4", rp.Partitions())
	}
	tests := map[string]int{"apple": 0, "fig": 0, "g": 1, "gamma": 1, "mango": 1, "n": 2, "pear": 2, "t": 3, "zebra": 3, "": 0}
	for key, want := range tests {
		if got := rp.Partition(key); got != want {
			t.Errorf("Partition(%q) = %d, want %d", key, got, want)
		}
	}
}

func TestCh13_Range_NoBoundaries_IsOnePartition_AndBoundariesMustBeStrictlyAscending(t *testing.T) {
	rp, err := partition.NewRangePartitioner(nil)
	if err != nil || rp.Partitions() != 1 || rp.Partition("anything") != 0 {
		t.Fatalf("no boundaries: (%v, %v), want a single partition 0", rp, err)
	}
	for _, bad := range [][]string{{"b", "a"}, {"a", "a"}, {"a", "c", "b"}} {
		_, err := partition.NewRangePartitioner(bad)
		testutil.ExpectError(t, err)
	}
}

// skewedKeys: 70% of the keys start with 'a', then 'b' ... -- like surnames, product SKUs, or a viral customer.
func skewedKeys(n int, seed int64) []string {
	rnd := testutil.Rand(seed)
	letters := "aaaaaaabbbcdefghij" // 'a' = 7/18 + ... weighted
	out := make([]string, n)
	for i := range out {
		l := letters[rnd.Intn(len(letters))]
		out[i] = fmt.Sprintf("%c%05d", l, rnd.Intn(100_000))
	}
	return out
}

func loads(t *testing.T, rp *partition.RangePartitioner, data []string) []int {
	t.Helper()
	c := make([]int, rp.Partitions())
	for _, k := range data {
		p := rp.Partition(k)
		if p < 0 || p >= len(c) {
			t.Fatalf("Partition(%q) = %d, outside 0..%d", k, p, len(c)-1)
		}
		c[p]++
	}
	return c
}

func TestCh13_Range_SampledBoundariesBalanceSkewedData_WhereEvenLettersDoNot(t *testing.T) {
	data := skewedKeys(60_000, 1)

	naive, err := partition.NewRangePartitioner([]string{"c", "f", "i"}) // "evenly spaced" letters
	if err != nil {
		t.Fatalf("NewRangePartitioner: %v", err)
	}
	naiveRatio := partition.SkewRatio(loads(t, naive, data))

	sample := skewedKeys(2_000, 2) // a SMALL independent sample of the same distribution
	bounds := partition.BoundariesFromSample(sample, 4)
	if len(bounds) != 3 {
		t.Fatalf("BoundariesFromSample returned %v, want 3 boundaries for 4 partitions", bounds)
	}
	sampled, err := partition.NewRangePartitioner(bounds)
	if err != nil {
		t.Fatalf("the boundaries you produced are not valid: %v", err)
	}
	sampledRatio := partition.SkewRatio(loads(t, sampled, data))

	if naiveRatio < 1.5 {
		t.Fatalf("test data is not skewed enough (naive ratio %.2f)", naiveRatio)
	}
	if sampledRatio > 1.2 {
		t.Fatalf("sampled boundaries %v give skew ratio %.2f (naive: %.2f); equi-depth boundaries should be close to 1", bounds, sampledRatio, naiveRatio)
	}

	// edge cases
	if got := partition.BoundariesFromSample(nil, 4); len(got) != 0 {
		t.Fatalf("empty sample: %v", got)
	}
	if got := partition.BoundariesFromSample(sample, 1); len(got) != 0 {
		t.Fatalf("n < 2: %v", got)
	}
	same := make([]string, 100)
	for i := range same {
		same[i] = "x"
	}
	if got := partition.BoundariesFromSample(same, 4); len(got) > 1 {
		t.Fatalf("a sample of one repeated key gave %v: duplicate boundaries must collapse", got)
	}
}

func TestCh13_Ring_EmptyRing_HasNoOwner_ASingleNodeOwnsEverything(t *testing.T) {
	r := partition.NewRing(50)
	r.Add("only")
	for _, k := range keys(200, "k") {
		if node, ok := r.Get(k); !ok || node != "only" {
			t.Fatalf("Get(%q) = (%q, %v), want the only node", k, node, ok)
		}
	}
	r.Remove("only")
	if node, ok := partition.NewRing(5).Get("x"); ok || node != "" {
		t.Fatalf("an empty ring returned (%q, %v)", node, ok)
	}
	if node, ok := r.Get("x"); ok {
		t.Fatalf("after removing the last node Get returned (%q, %v)", node, ok)
	}
}

func buildRing(vnodes int, nodes ...string) *partition.Ring {
	r := partition.NewRing(vnodes)
	for _, n := range nodes {
		r.Add(n)
	}
	return r
}

func TestCh13_Ring_IsDeterministic_AcrossInstancesAndInsertionOrders(t *testing.T) {
	a := buildRing(100, "alpha", "beta", "gamma")
	b := buildRing(100, "gamma", "alpha", "beta")
	for _, k := range keys(2000, "key-") {
		na, ok := a.Get(k)
		nb, _ := b.Get(k)
		if !ok || (na != "alpha" && na != "beta" && na != "gamma") {
			t.Fatalf("key %q was assigned to %q (ok=%v), want one of the three nodes", k, na, ok)
		}
		if na != nb {
			t.Fatalf("key %q -> %q on one ring and %q on another built in a different order", k, na, nb)
		}
	}
}

func TestCh13_Ring_AddingTheSameNodeTwice_RemovingAnUnknownNode_AndNodesAreSorted(t *testing.T) {
	r := buildRing(20, "b", "a", "c")
	before := map[string]string{}
	for _, k := range keys(500, "k") {
		before[k], _ = r.Get(k)
	}
	r.Add("a")
	r.Remove("never-added")
	if !slices.Equal(r.Nodes(), []string{"a", "b", "c"}) {
		t.Fatalf("Nodes = %v, want [a b c]", r.Nodes())
	}
	for k, want := range before {
		if got, _ := r.Get(k); got != want {
			t.Fatalf("key %q moved from %q to %q after a no-op change", k, want, got)
		}
	}
}

func TestCh13_Ring_VirtualNodesSpreadTheLoadEvenly(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	data := keys(50_000, "item-")
	measure := func(vnodes int) float64 {
		r := buildRing(vnodes, nodes...)
		counts := map[string]int{}
		for _, k := range data {
			n, _ := r.Get(k)
			counts[n]++
		}
		var c []int
		for _, n := range nodes {
			c = append(c, counts[n])
		}
		return partition.SkewRatio(c)
	}
	few, many := measure(1), measure(200)
	if many == 0 || few == 0 {
		t.Fatal("no keys were assigned to any node")
	}
	if many > 1.25 {
		t.Fatalf("with 200 virtual nodes the busiest node still has %.2fx its fair share", many)
	}
	if few < many*1.2 {
		t.Fatalf("1 virtual node gave ratio %.2f and 200 gave %.2f: virtual nodes should clearly help", few, many)
	}
}

func TestCh13_Ring_AddingANodeOnlyMovesKeysToTheNewNode(t *testing.T) {
	data := keys(40_000, "k-")
	r := buildRing(200, "n1", "n2", "n3", "n4")
	before := make([]string, len(data))
	for i, k := range data {
		before[i], _ = r.Get(k)
	}

	r.Add("n5")

	moved := 0
	for i, k := range data {
		now, _ := r.Get(k)
		if now != before[i] {
			moved++
			if now != "n5" {
				t.Fatalf("key %q moved from %s to %s: when a node is ADDED, keys may only move to the NEW node", k, before[i], now)
			}
		}
	}
	if frac := float64(moved) / float64(len(data)); frac < 0.10 || frac > 0.30 {
		t.Fatalf("%.1f%% of the keys moved; about 20%% (1/5) is expected", frac*100)
	}
}

func TestCh13_Ring_RemovingANodeOnlyMovesThatNodesKeys(t *testing.T) {
	data := keys(30_000, "k-")
	r := buildRing(200, "n1", "n2", "n3", "n4", "n5")
	before := make([]string, len(data))
	for i, k := range data {
		before[i], _ = r.Get(k)
	}

	r.Remove("n3")

	targets := map[string]bool{}
	for i, k := range data {
		now, ok := r.Get(k)
		if !ok || now == "n3" {
			t.Fatalf("key %q is still owned by the removed node (%q, %v)", k, now, ok)
		}
		if before[i] != "n3" && now != before[i] {
			t.Fatalf("key %q moved from %s to %s although its node was NOT removed", k, before[i], now)
		}
		if before[i] == "n3" {
			targets[now] = true
		}
	}
	if len(targets) < 3 {
		t.Fatalf("the removed node's keys went to only %d nodes; virtual nodes should spread them over all the others", len(targets))
	}
}

func TestCh13_ConsistentHashingMovesFarFewerKeysThanModulo(t *testing.T) {
	data := keys(30_000, "k-")
	movedModulo := 0
	for _, k := range data {
		a, _ := partition.Hash(k, 4)
		b, _ := partition.Hash(k, 5)
		if a != b {
			movedModulo++
		}
	}
	r := buildRing(200, "n1", "n2", "n3", "n4")
	before := make([]string, len(data))
	for i, k := range data {
		before[i], _ = r.Get(k)
	}
	r.Add("n5")
	movedRing := 0
	for i, k := range data {
		if now, _ := r.Get(k); now != before[i] {
			movedRing++
		}
	}
	fm, fr := float64(movedModulo)/float64(len(data)), float64(movedRing)/float64(len(data))
	if fm < 0.7 || fr > 0.3 {
		t.Fatalf("hash %% N moved %.0f%% of keys when going 4 -> 5 nodes (expect ~80%%); the ring moved %.0f%% (expect ~20%%)", fm*100, fr*100)
	}
}

// The documented layout lets the test compute where the points are, and therefore who MUST own a key past the last point.
func TestCh13_Ring_KeysPastTheLastPointWrapAroundToTheFirstPoint(t *testing.T) {
	nodes := []string{"red", "green", "blue"}
	r := buildRing(1, nodes...) // one point per node: hash("<node>#0")
	type pt struct {
		hash uint64
		node string
	}
	var points []pt
	for _, n := range nodes {
		points = append(points, pt{hashx.Sum64(n + "#0"), n})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].hash < points[j].hash })
	first, last := points[0], points[len(points)-1]

	pastLast, beforeFirst := 0, 0
	for _, k := range keys(2_000, "probe-") {
		h := hashx.Sum64(k)
		got, ok := r.Get(k)
		if !ok {
			t.Fatalf("Get(%q) found no owner on a non-empty ring", k)
		}
		var want string
		switch {
		case h > last.hash:
			want, pastLast = first.node, pastLast+1 // walked clockwise off the end: WRAP to the first point
		default:
			for _, p := range points { // the first point at or after the key's position
				if p.hash >= h {
					want = p.node
					break
				}
			}
			if h <= first.hash {
				beforeFirst++
			}
		}
		if got != want {
			t.Fatalf("key %q (hash %d) is owned by %q, want %q; points: %v", k, h, got, want, points)
		}
	}
	if pastLast < 10 || beforeFirst < 10 {
		t.Fatalf("the probe keys did not exercise both edges (past the last point: %d, before the first: %d)", pastLast, beforeFirst)
	}
}
