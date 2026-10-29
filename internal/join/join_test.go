package join_test

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"dataflow/internal/join"
	"dataflow/internal/testutil"
)

// Chapter 12 -- joins.   Run:  go test ./internal/join -run Ch12 -race -v

func r(kv ...string) join.Row {
	row := join.Row{}
	for i := 0; i < len(kv); i += 2 {
		row[kv[i]] = kv[i+1]
	}
	return row
}

// canon turns rows into a sorted slice of strings, so results can be compared as MULTISETS (order ignored).
func canon(rows []join.Row) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s=%s;", k, row[k])
		}
		out[i] = sb.String()
	}
	sort.Strings(out)
	return out
}

func sameRows(t *testing.T, got, want []join.Row, what string) {
	t.Helper()
	g, w := canon(got), canon(want)
	if !slices.Equal(g, w) {
		t.Fatalf("%s: results differ (%d rows vs %d). first got=%v first want=%v", what, len(g), len(w), g[:min(2, len(g))], w[:min(2, len(w))])
	}
}

var customers = []join.Row{
	r("cid", "1", "name", "Ada"), r("cid", "2", "name", "Grace"), r("cid", "3", "name", "Linus"), r("name", "no-key"), r("cid", "", "name", "empty-key"),
}
var orders = []join.Row{
	r("oid", "100", "cust", "1", "amount", "5"), r("oid", "101", "cust", "1", "amount", "7"),
	r("oid", "102", "cust", "3", "amount", "2"), r("oid", "103", "cust", "9", "amount", "4"), r("oid", "104", "amount", "1"),
}

// ---------------- HashJoin ----------------

func TestCh12_HashJoin_Inner_PairsMatchingRows(t *testing.T) {
	got := join.HashJoin(customers, orders, "cid", "cust", join.Inner)
	want := []join.Row{
		r("cid", "1", "name", "Ada", "oid", "100", "cust", "1", "amount", "5"),
		r("cid", "1", "name", "Ada", "oid", "101", "cust", "1", "amount", "7"),
		r("cid", "3", "name", "Linus", "oid", "102", "cust", "3", "amount", "2"),
	}
	sameRows(t, got, want, "inner join")
}

func TestCh12_HashJoin_Inner_KeepsTheLeftOrder_ThenTheRightOrder(t *testing.T) {
	got := join.HashJoin(customers, orders, "cid", "cust", join.Inner)
	var oids []string
	for _, row := range got {
		oids = append(oids, row["oid"])
	}
	if !slices.Equal(oids, []string{"100", "101", "102"}) {
		t.Fatalf("output order = %v, want left order then right order within a key", oids)
	}
}

func TestCh12_HashJoin_LeftOuter_KeepsUnmatchedLeftRows_Alone(t *testing.T) {
	got := join.HashJoin(customers, orders, "cid", "cust", join.LeftOuter)
	want := []join.Row{
		r("cid", "1", "name", "Ada", "oid", "100", "cust", "1", "amount", "5"),
		r("cid", "1", "name", "Ada", "oid", "101", "cust", "1", "amount", "7"),
		r("cid", "2", "name", "Grace"),
		r("cid", "3", "name", "Linus", "oid", "102", "cust", "3", "amount", "2"),
		r("name", "no-key"),
		r("cid", "", "name", "empty-key"),
	}
	sameRows(t, got, want, "left outer join")
}

func TestCh12_HashJoin_ManyToMany_GivesTheCrossProductOfEachKey(t *testing.T) {
	left := []join.Row{r("k", "a", "l", "1"), r("k", "a", "l", "2"), r("k", "b", "l", "3")}
	right := []join.Row{r("k", "a", "r", "x"), r("k", "a", "r", "y"), r("k", "a", "r", "z"), r("k", "c", "r", "w")}
	if got := join.HashJoin(left, right, "k", "k", join.Inner); len(got) != 6 { // 2 x 3 for key a
		t.Fatalf("%d rows, want 6 (2 left x 3 right for key a)", len(got))
	}
}

func TestCh12_HashJoin_AClashingColumnName_GetsASuffix(t *testing.T) {
	got := join.HashJoin([]join.Row{r("id", "1", "v", "left")}, []join.Row{r("id", "1", "v", "right")}, "id", "id", join.Inner)
	want := []join.Row{r("id", "1", "v", "left", "id_r", "1", "v_r", "right")}
	sameRows(t, got, want, "clashing names")
}

func TestCh12_HashJoin_EmptyInputs(t *testing.T) {
	if got := join.HashJoin(nil, orders, "cid", "cust", join.Inner); len(got) != 0 {
		t.Fatalf("empty left: %v", got)
	}
	if got := join.HashJoin(customers[:2], nil, "cid", "cust", join.Inner); len(got) != 0 {
		t.Fatalf("empty right, inner: %v", got)
	}
	if got := join.HashJoin(customers[:2], nil, "cid", "cust", join.LeftOuter); len(got) != 2 {
		t.Fatalf("empty right, left outer: %d rows, want both left rows", len(got))
	}
}

func TestCh12_HashJoin_DoesNotShareMapsWithTheInput(t *testing.T) {
	left := []join.Row{r("k", "a", "v", "1")}
	got := join.HashJoin(left, nil, "k", "k", join.LeftOuter)
	if len(got) != 1 {
		t.Fatalf("got %d rows, want the one unmatched left row", len(got))
	}
	got[0]["v"] = "CHANGED"
	if left[0]["v"] != "1" {
		t.Fatal("an unmatched left row must be copied, not aliased: mutating the output changed the input")
	}
}

// ---------------- random data shared by the three algorithms ----------------

func randomTables(nLeft, nRight, keys int, seed int64) (left, right []join.Row) {
	rnd := testutil.Rand(seed)
	for i := 0; i < nLeft; i++ {
		row := r("lid", fmt.Sprint(i), "lval", fmt.Sprint(rnd.Intn(1000)))
		if rnd.Intn(20) != 0 { // 5% of rows have no key
			row["k"] = fmt.Sprintf("key-%d", rnd.Intn(keys))
		}
		left = append(left, row)
	}
	for i := 0; i < nRight; i++ {
		row := r("rid", fmt.Sprint(i), "rval", fmt.Sprint(rnd.Intn(1000)))
		if rnd.Intn(20) != 0 {
			row["k2"] = fmt.Sprintf("key-%d", rnd.Intn(keys))
		}
		right = append(right, row)
	}
	return
}

// nestedLoop is the obviously-correct, hopelessly slow reference every real join must agree with. (It is independent of
// your HashJoin, so a broken HashJoin cannot make a broken SortMergeJoin look right.)
func nestedLoop(left, right []join.Row, leftKey, rightKey string, kind join.Kind) []join.Row {
	var out []join.Row
	for _, l := range left {
		matched := false
		if lk := l[leftKey]; lk != "" {
			for _, rr := range right {
				if rk := rr[rightKey]; rk != "" && rk == lk {
					row := join.Row{}
					for k, v := range l {
						row[k] = v
					}
					for k, v := range rr {
						if _, clash := l[k]; clash {
							row[k+"_r"] = v
						} else {
							row[k] = v
						}
					}
					out = append(out, row)
					matched = true
				}
			}
		}
		if !matched && kind == join.LeftOuter {
			row := join.Row{}
			for k, v := range l {
				row[k] = v
			}
			out = append(out, row)
		}
	}
	return out
}

// ---------------- SortMergeJoin ----------------

func TestCh12_SortMergeJoin_AgreesWithHashJoin_OnRandomData(t *testing.T) {
	left, right := randomTables(600, 500, 80, 1)
	for _, kind := range []join.Kind{join.Inner, join.LeftOuter} {
		want := nestedLoop(left, right, "k", "k2", kind)
		if len(want) < 100 {
			t.Fatalf("the test data should produce many rows, the oracle found %d", len(want))
		}
		sameRows(t, join.SortMergeJoin(left, right, "k", "k2", kind), want, fmt.Sprintf("kind %d", kind))
		sameRows(t, join.HashJoin(left, right, "k", "k2", kind), want, fmt.Sprintf("HashJoin, kind %d", kind))
	}
}

func TestCh12_SortMergeJoin_Inner_OutputIsSortedByTheJoinKey(t *testing.T) {
	left, right := randomTables(300, 300, 40, 2)
	got := join.SortMergeJoin(left, right, "k", "k2", join.Inner)
	if len(got) == 0 {
		t.Fatal("expected some matches")
	}
	for i := 1; i < len(got); i++ {
		if got[i-1]["k"] > got[i]["k"] {
			t.Fatalf("row %d has key %q after %q: the output must be sorted by key", i, got[i]["k"], got[i-1]["k"])
		}
	}
}

func TestCh12_SortMergeJoin_ManyToManyGroups_AndEmptySides(t *testing.T) {
	left := []join.Row{r("k", "a", "l", "1"), r("k", "a", "l", "2"), r("k", "b", "l", "3")}
	right := []join.Row{r("k", "a", "r", "x"), r("k", "a", "r", "y"), r("k", "c", "r", "w")}
	if got := join.SortMergeJoin(left, right, "k", "k", join.Inner); len(got) != 4 {
		t.Fatalf("%d rows, want 4 (2 x 2 for key a)", len(got))
	}
	if got := join.SortMergeJoin(left, nil, "k", "k", join.LeftOuter); len(got) != 3 {
		t.Fatalf("right empty, left outer: %d rows, want 3", len(got))
	}
	if got := join.SortMergeJoin(nil, right, "k", "k", join.LeftOuter); len(got) != 0 {
		t.Fatalf("left empty: %d rows, want 0", len(got))
	}
}

func TestCh12_SortMergeJoin_KeysAreComparedAsText(t *testing.T) {
	left := []join.Row{r("k", "10", "l", "ten"), r("k", "9", "l", "nine")}
	right := []join.Row{r("k", "9", "r", "nine"), r("k", "10", "r", "ten")}
	got := join.SortMergeJoin(left, right, "k", "k", join.Inner)
	if len(got) != 2 || got[0]["k"] != "10" || got[1]["k"] != "9" { // "10" < "9" as strings
		t.Fatalf("got %v, want both matched, sorted as text (10 before 9)", got)
	}
}

// ---------------- GraceHashJoin ----------------

func runGrace(t *testing.T, left, right []join.Row, kind join.Kind, p int, tmp string) ([]join.Row, join.GraceStats) {
	t.Helper()
	var out []join.Row
	st, err := join.GraceHashJoin(join.SliceIter(left), join.SliceIter(right), "k", "k2", kind, join.GraceOptions{Partitions: p, TmpDir: tmp},
		func(row join.Row) error { out = append(out, row); return nil })
	if err != nil {
		t.Fatalf("GraceHashJoin: %v", err)
	}
	return out, st
}

func TestCh12_GraceHashJoin_AgreesWithHashJoin_AndNeverHoldsTheWholeBuildSide(t *testing.T) {
	left, right := randomTables(5000, 2000, 400, 3)
	tmp := t.TempDir()
	for _, kind := range []join.Kind{join.Inner, join.LeftOuter} {
		got, st := runGrace(t, left, right, kind, 8, tmp)
		sameRows(t, got, nestedLoop(left, right, "k", "k2", kind), fmt.Sprintf("kind %d", kind))
		if st.Partitions != 8 {
			t.Fatalf("Partitions = %d, want 8", st.Partitions)
		}
		if st.SpilledRight < 1800 || st.SpilledRight > 2000 || st.SpilledLeft < 4500 || st.SpilledLeft > 5000 {
			t.Fatalf("stats %+v: only rows WITH a key are spilled (~95%% of each side)", st)
		}
		if st.MaxBuildRows >= st.SpilledRight/3 || st.MaxBuildRows == 0 {
			t.Fatalf("MaxBuildRows = %d of %d right rows: with 8 partitions the build table should hold only about an eighth", st.MaxBuildRows, st.SpilledRight)
		}
	}
}

func TestCh12_GraceHashJoin_LeavesNoTemporaryFiles(t *testing.T) {
	left, right := randomTables(500, 500, 50, 4)
	tmp := t.TempDir()
	runGrace(t, left, right, join.Inner, 4, tmp)
	if names := testutil.DirEntries(t, tmp); len(names) != 0 {
		t.Fatalf("temporary files left behind: %v", names)
	}
}

func TestCh12_GraceHashJoin_LeftOuter_EmitsKeylessLeftRows(t *testing.T) {
	left := []join.Row{r("k", "a", "l", "1"), r("l", "no key")}
	right := []join.Row{r("k2", "a", "r", "x")}
	got, _ := runGrace(t, left, right, join.LeftOuter, 3, t.TempDir())
	sameRows(t, got, []join.Row{r("k", "a", "l", "1", "k2", "a", "r", "x"), r("l", "no key")}, "grace left outer")
}

func TestCh12_GraceHashJoin_OneHotKey_CannotBeSplitByPartitioning(t *testing.T) {
	var left, right []join.Row
	for i := 0; i < 300; i++ {
		left = append(left, r("k", "hot", "lid", fmt.Sprint(i)))
		right = append(right, r("k2", "hot", "rid", fmt.Sprint(i)))
	}
	got, st := runGrace(t, left, right, join.Inner, 8, t.TempDir())
	if len(got) != 300*300 {
		t.Fatalf("%d rows, want %d", len(got), 300*300)
	}
	if st.MaxBuildRows != 300 {
		t.Fatalf("MaxBuildRows = %d, want 300: skew defeats partitioning (all rows of one key land in ONE partition)", st.MaxBuildRows)
	}
}

func TestCh12_GraceHashJoin_StopsOnEmitError_AndRejectsBadPartitionCounts(t *testing.T) {
	left, right := randomTables(200, 200, 10, 5)
	tmp := t.TempDir()
	boom := errors.New("downstream full")
	n := 0
	_, err := join.GraceHashJoin(join.SliceIter(left), join.SliceIter(right), "k", "k2", join.Inner, join.GraceOptions{Partitions: 4, TmpDir: tmp},
		func(join.Row) error { n++; return boom })
	if !errors.Is(err, boom) || n != 1 {
		t.Fatalf("err=%v emitted=%d, want the error after exactly one row", err, n)
	}
	if names := testutil.DirEntries(t, tmp); len(names) != 0 {
		t.Fatalf("temporary files left after an error: %v", names)
	}
	_, err = join.GraceHashJoin(join.SliceIter(left), join.SliceIter(right), "k", "k2", join.Inner, join.GraceOptions{Partitions: 0, TmpDir: tmp}, func(join.Row) error { return nil })
	testutil.ExpectError(t, err)
}

func TestCh12_EmptyKeysNeverMatch_InAnyOfTheThreeAlgorithms(t *testing.T) {
	// an empty value is "no key", exactly like a missing column: two empty keys are NOT equal (SQL: NULL = NULL is not true)
	left := []join.Row{r("k", "", "l", "1"), r("k", "a", "l", "2")}
	right := []join.Row{r("k2", "", "r", "x"), r("k2", "a", "r", "y")}
	want := []join.Row{r("k", "a", "l", "2", "k2", "a", "r", "y")}

	sameRows(t, join.HashJoin(left, right, "k", "k2", join.Inner), want, "HashJoin inner")
	sameRows(t, join.SortMergeJoin(left, right, "k", "k2", join.Inner), want, "SortMergeJoin inner")
	got, _ := runGrace(t, left, right, join.Inner, 3, t.TempDir())
	sameRows(t, got, want, "GraceHashJoin inner")

	wantOuter := append([]join.Row{r("k", "", "l", "1")}, want...)
	sameRows(t, join.HashJoin(left, right, "k", "k2", join.LeftOuter), wantOuter, "HashJoin left outer")
	sameRows(t, join.SortMergeJoin(left, right, "k", "k2", join.LeftOuter), wantOuter, "SortMergeJoin left outer")
	got, _ = runGrace(t, left, right, join.LeftOuter, 3, t.TempDir())
	sameRows(t, got, wantOuter, "GraceHashJoin left outer")
}
