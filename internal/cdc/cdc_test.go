package cdc_test

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"dataflow/internal/cdc"
	"dataflow/internal/testutil"
)

// Chapter 22 -- upserts, CDC and slowly changing dimensions.   Run:  go test ./internal/cdc -run Ch22 -race -v

func row(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func ins(seq uint64, key string, r map[string]string) cdc.Event {
	return cdc.Event{Seq: seq, Op: cdc.OpInsert, Key: key, Row: r, TS: int64(seq) * 10}
}
func upd(seq uint64, key string, r map[string]string) cdc.Event {
	return cdc.Event{Seq: seq, Op: cdc.OpUpdate, Key: key, Row: r, TS: int64(seq) * 10}
}
func del(seq uint64, key string) cdc.Event {
	return cdc.Event{Seq: seq, Op: cdc.OpDelete, Key: key, TS: int64(seq) * 10}
}

// ---------------- Table: the current state ----------------

func TestCh22_Table_InsertUpdateDelete(t *testing.T) {
	tb := cdc.NewTable()
	if !tb.Apply(ins(1, "u1", row("name", "ada"))) || !tb.Apply(ins(2, "u2", row("name", "grace"))) {
		t.Fatal("fresh events must be applied (Apply returns true)")
	}
	if !tb.Apply(upd(3, "u1", row("name", "ada lovelace", "city", "london"))) {
		t.Fatal("an update with a higher Seq must be applied")
	}
	got, ok := tb.Get("u1")
	if !ok || !reflect.DeepEqual(got, row("name", "ada lovelace", "city", "london")) {
		t.Fatalf("Get(u1) = (%v, %v)", got, ok)
	}
	if !tb.Apply(del(4, "u2")) {
		t.Fatal("a delete must be applied")
	}
	if _, ok := tb.Get("u2"); ok {
		t.Fatal("u2 was deleted but Get still finds it")
	}
	if tb.Len() != 1 || len(tb.Rows()) != 1 {
		t.Fatalf("Len = %d, Rows = %v: want only u1 (tombstones are not rows)", tb.Len(), tb.Rows())
	}
}

func TestCh22_Table_DuplicatesAndStaleEventsAreIgnored(t *testing.T) {
	tb := cdc.NewTable()
	tb.Apply(ins(5, "k", row("v", "five")))
	if tb.Apply(ins(5, "k", row("v", "five"))) {
		t.Fatal("applying the very same event twice must report false (idempotent)")
	}
	if tb.Apply(upd(3, "k", row("v", "STALE"))) {
		t.Fatal("an update older than the applied one must be ignored")
	}
	if tb.Apply(upd(5, "k", row("v", "same-seq-different-data"))) {
		t.Fatal("an event with the SAME Seq as the applied one is a duplicate and must be ignored")
	}
	if got, _ := tb.Get("k"); !reflect.DeepEqual(got, row("v", "five")) {
		t.Fatalf("Get(k) = %v, want {v: five}", got)
	}
}

func TestCh22_Table_ALateUpdateCannotResurrectADeletedRow(t *testing.T) {
	tb := cdc.NewTable()
	tb.Apply(ins(1, "k", row("v", "1")))
	tb.Apply(del(5, "k"))
	if tb.Apply(upd(3, "k", row("v", "late"))) {
		t.Fatal("an update older than the delete must be ignored")
	}
	if _, ok := tb.Get("k"); ok {
		t.Fatal("a late update resurrected a deleted row: deletes must be remembered as tombstones with their Seq")
	}
	if !tb.Apply(ins(9, "k", row("v", "reborn"))) { // a genuinely NEWER insert may bring it back
		t.Fatal("a newer insert after a delete must be applied")
	}
	if got, ok := tb.Get("k"); !ok || got["v"] != "reborn" {
		t.Fatalf("Get(k) = (%v, %v)", got, ok)
	}
}

func TestCh22_Table_IsAnUpsert_AndAnEarlyDeleteProtectsAgainstALateInsert(t *testing.T) {
	tb := cdc.NewTable()
	if !tb.Apply(upd(7, "new", row("v", "x"))) { // an update for a key never seen: creates it
		t.Fatal("an update for an unknown key must create the row (upsert)")
	}
	if got, ok := tb.Get("new"); !ok || got["v"] != "x" {
		t.Fatalf("Get(new) = (%v, %v)", got, ok)
	}
	tb.Apply(ins(9, "k", row("v", "overwritten"))) // an insert for an existing key overwrites
	tb.Apply(ins(10, "k", row("v", "again")))
	if got, _ := tb.Get("k"); got["v"] != "again" {
		t.Fatalf("Get(k) = %v, want the insert with the higher Seq to win", got)
	}
	// a delete arrives BEFORE the insert it deletes (reordering): the late insert must lose
	tb.Apply(del(20, "ghost"))
	if tb.Apply(ins(15, "ghost", row("v", "late insert"))) {
		t.Fatal("an insert older than a tombstone must be ignored even if the key was never seen before the delete")
	}
	if _, ok := tb.Get("ghost"); ok {
		t.Fatal("the row exists although its delete has a higher Seq")
	}
}

func TestCh22_Table_NeverAliasesCallerMaps(t *testing.T) {
	tb := cdc.NewTable()
	input := row("v", "original")
	tb.Apply(ins(1, "k", input))
	input["v"] = "MUTATED-INPUT"
	got, _ := tb.Get("k")
	if got["v"] != "original" {
		t.Fatalf("mutating the event's Row after Apply changed the table: %v", got)
	}
	got["v"] = "MUTATED-OUTPUT"
	if again, _ := tb.Get("k"); again["v"] != "original" {
		t.Fatalf("mutating the row returned by Get changed the table: %v", again)
	}
	tb.Rows()["k"]["v"] = "MUTATED-ROWS"
	if again, _ := tb.Get("k"); again["v"] != "original" {
		t.Fatalf("mutating the map returned by Rows changed the table: %v", again)
	}
}

func TestCh22_Table_IgnoresUnknownOperations_AndApplyAllCountsWhatWasApplied(t *testing.T) {
	tb := cdc.NewTable()
	if tb.Apply(cdc.Event{Seq: 1, Op: "x", Key: "k", Row: row("v", "1")}) {
		t.Fatal("an unknown Op must be ignored")
	}
	n := tb.ApplyAll([]cdc.Event{ins(1, "a", row("v", "1")), ins(2, "b", row("v", "2")), ins(1, "a", row("v", "1")), upd(0, "b", row("v", "old")), del(3, "a")})
	if n != 3 || tb.Len() != 1 {
		t.Fatalf("ApplyAll applied %d events (want 3: two fresh inserts and a delete), Len = %d", n, tb.Len())
	}
}

// randomChangeLog makes a realistic stream: many keys, inserts, updates and deletes, with strictly increasing Seq.
func randomChangeLog(n int, seed int64) []cdc.Event {
	rnd := testutil.Rand(seed)
	events := make([]cdc.Event, 0, n)
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("key-%02d", rnd.Intn(20))
		switch r := rnd.Intn(10); {
		case r < 2:
			events = append(events, cdc.Event{Seq: uint64(i), Op: cdc.OpInsert, Key: key, Row: row("v", fmt.Sprint(i), "who", "ins"), TS: int64(i) * 100})
		case r < 8:
			events = append(events, cdc.Event{Seq: uint64(i), Op: cdc.OpUpdate, Key: key, Row: row("v", fmt.Sprint(i), "who", "upd"), TS: int64(i) * 100})
		default:
			events = append(events, cdc.Event{Seq: uint64(i), Op: cdc.OpDelete, Key: key, TS: int64(i) * 100})
		}
	}
	return events
}

func TestCh22_Table_AnyArrivalOrderWithDuplicatesConvergesToTheSameTable(t *testing.T) {
	log := randomChangeLog(600, 22)
	reference := cdc.NewTable()
	reference.ApplyAll(log)
	want := reference.Rows()
	if len(want) == 0 || len(want) == 20 {
		t.Fatalf("the test data should leave some but not all of the 20 keys alive, got %d", len(want))
	}

	rnd := testutil.Rand(23)
	for trial := 0; trial < 60; trial++ {
		mixed := slices.Clone(log)
		for i := 0; i < 150; i++ { // sprinkle duplicates
			mixed = append(mixed, log[rnd.Intn(len(log))])
		}
		rnd.Shuffle(len(mixed), func(i, j int) { mixed[i], mixed[j] = mixed[j], mixed[i] })

		tb := cdc.NewTable()
		tb.ApplyAll(mixed)
		if got := tb.Rows(); !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d: a shuffled stream with duplicates produced a DIFFERENT table (%d rows vs %d): apply must be idempotent and order-independent", trial, len(got), len(want))
		}
	}
}

func TestCh22_Compact_KeepsOnlyTheLatestEventPerKey_AndReplaysToTheSameTable(t *testing.T) {
	log := randomChangeLog(800, 24)
	compacted := cdc.Compact(log)

	seenKey := map[string]bool{}
	for i, e := range compacted {
		if seenKey[e.Key] {
			t.Fatalf("key %s appears twice in the compacted log", e.Key)
		}
		seenKey[e.Key] = true
		if i > 0 && compacted[i-1].Seq >= e.Seq {
			t.Fatalf("the compacted log must be sorted by Seq: %d then %d", compacted[i-1].Seq, e.Seq)
		}
	}
	if len(compacted) >= len(log)/5 || len(compacted) == 0 {
		t.Fatalf("compaction left %d of %d events; with 20 keys it should keep at most 20", len(compacted), len(log))
	}

	full, small := cdc.NewTable(), cdc.NewTable()
	full.ApplyAll(log)
	small.ApplyAll(compacted)
	if !reflect.DeepEqual(full.Rows(), small.Rows()) {
		t.Fatal("replaying the compacted log gives a different table than replaying the full log")
	}
	// deletes must survive compaction, so a late stale event is still rejected
	for _, e := range compacted {
		if e.Op == cdc.OpDelete {
			if small.Apply(cdc.Event{Seq: e.Seq - 1, Op: cdc.OpUpdate, Key: e.Key, Row: row("v", "late")}) {
				t.Fatalf("after compaction a late update for deleted key %s was accepted: its tombstone must be kept", e.Key)
			}
			return
		}
	}
	t.Fatal("the test data should end with at least one deleted key")
}

func TestCh22_Compact_EmptyAndSingle(t *testing.T) {
	if got := cdc.Compact(nil); len(got) != 0 {
		t.Fatalf("Compact(nil) = %v", got)
	}
	one := []cdc.Event{ins(1, "k", row("v", "1"))}
	if got := cdc.Compact(one); len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("Compact(one event) = %v", got)
	}
}

// ---------------- History: SCD type 2 ----------------

func apply(t *testing.T, h *cdc.History, events ...cdc.Event) {
	t.Helper()
	for _, e := range events {
		if err := h.Apply(e); err != nil {
			t.Fatalf("Apply(%+v): %v", e, err)
		}
	}
}

func TestCh22_History_AnUpdateClosesTheOldVersionAndOpensANewOne(t *testing.T) {
	h := cdc.NewHistory()
	apply(t, h,
		cdc.Event{Op: cdc.OpInsert, Key: "c7", Row: row("region", "south"), TS: 100},
		cdc.Event{Op: cdc.OpUpdate, Key: "c7", Row: row("region", "north"), TS: 300},
	)
	vs := h.Versions("c7")
	if len(vs) != 2 {
		t.Fatalf("%d versions, want 2: %+v", len(vs), vs)
	}
	if v := vs[0]; v.Key != "c7" || v.ValidFrom != 100 || v.ValidTo != 300 || v.Current || v.Row["region"] != "south" {
		t.Fatalf("first version = %+v, want south valid [100, 300) and not current", v)
	}
	if v := vs[1]; v.ValidFrom != 300 || v.ValidTo != cdc.OpenEnded || !v.Current || v.Row["region"] != "north" {
		t.Fatalf("second version = %+v, want north valid [300, OpenEnded) and current", v)
	}
	if cur := h.Current(); !reflect.DeepEqual(cur, map[string]map[string]string{"c7": row("region", "north")}) {
		t.Fatalf("Current() = %v", cur)
	}
}

func TestCh22_History_AnUpdateThatChangesNothingCreatesNoNewVersion(t *testing.T) {
	h := cdc.NewHistory()
	apply(t, h,
		cdc.Event{Op: cdc.OpInsert, Key: "k", Row: row("a", "1", "b", "2"), TS: 10},
		cdc.Event{Op: cdc.OpUpdate, Key: "k", Row: row("b", "2", "a", "1"), TS: 20}, // identical content
		cdc.Event{Op: cdc.OpUpdate, Key: "k", Row: row("a", "1", "b", "2"), TS: 30},
	)
	if vs := h.Versions("k"); len(vs) != 1 || vs[0].ValidFrom != 10 || vs[0].ValidTo != cdc.OpenEnded {
		t.Fatalf("versions = %+v: unchanged updates must not bloat the history", vs)
	}
	apply(t, h, cdc.Event{Op: cdc.OpUpdate, Key: "k", Row: row("a", "1"), TS: 40}) // a column disappeared: that IS a change
	if len(h.Versions("k")) != 2 {
		t.Fatalf("a row with a different set of columns must be a new version")
	}
}

func TestCh22_History_AsOfUsesAHalfOpenInterval(t *testing.T) {
	h := cdc.NewHistory()
	apply(t, h,
		cdc.Event{Op: cdc.OpInsert, Key: "k", Row: row("v", "one"), TS: 100},
		cdc.Event{Op: cdc.OpUpdate, Key: "k", Row: row("v", "two"), TS: 200},
	)
	cases := []struct {
		ts   int64
		want string // "" = no row
	}{{99, ""}, {100, "one"}, {150, "one"}, {199, "one"}, {200, "two"}, {9_999_999, "two"}}
	for _, c := range cases {
		got, ok := h.AsOf("k", c.ts)
		if c.want == "" && ok {
			t.Errorf("AsOf(%d) = %v, want nothing (before the first version)", c.ts, got)
		}
		if c.want != "" && (!ok || got["v"] != c.want) {
			t.Errorf("AsOf(%d) = (%v, %v), want %q (ValidFrom is inclusive, ValidTo exclusive)", c.ts, got, ok, c.want)
		}
	}
	if _, ok := h.AsOf("unknown", 150); ok {
		t.Error("AsOf for an unknown key must report not found")
	}
}

func TestCh22_History_DeleteClosesTheCurrentVersion_AndAReinsertLeavesAGap(t *testing.T) {
	h := cdc.NewHistory()
	apply(t, h,
		cdc.Event{Op: cdc.OpInsert, Key: "k", Row: row("v", "first life"), TS: 10},
		cdc.Event{Op: cdc.OpDelete, Key: "k", TS: 50},
		cdc.Event{Op: cdc.OpDelete, Key: "k", TS: 60},     // deleting again: nothing to close
		cdc.Event{Op: cdc.OpDelete, Key: "never", TS: 70}, // deleting an unknown key: nothing happens
		cdc.Event{Op: cdc.OpInsert, Key: "k", Row: row("v", "second life"), TS: 90},
	)
	vs := h.Versions("k")
	if len(vs) != 2 || vs[0].ValidTo != 50 || vs[0].Current || vs[1].ValidFrom != 90 || !vs[1].Current {
		t.Fatalf("versions = %+v", vs)
	}
	if _, ok := h.AsOf("k", 70); ok {
		t.Fatal("AsOf(70) found a row although the key was deleted from 50 to 90")
	}
	if got, ok := h.AsOf("k", 30); !ok || got["v"] != "first life" {
		t.Fatalf("AsOf(30) = (%v, %v)", got, ok)
	}
	if got, ok := h.AsOf("k", 95); !ok || got["v"] != "second life" {
		t.Fatalf("AsOf(95) = (%v, %v)", got, ok)
	}
	if len(h.Versions("never")) != 0 {
		t.Fatal("a delete for an unknown key must not create history")
	}
}

func TestCh22_History_RejectsEventsFromThePast_WithoutChangingAnything(t *testing.T) {
	h := cdc.NewHistory()
	apply(t, h,
		cdc.Event{Op: cdc.OpInsert, Key: "k", Row: row("v", "1"), TS: 100},
		cdc.Event{Op: cdc.OpUpdate, Key: "k", Row: row("v", "2"), TS: 200},
	)
	before := h.Versions("k")
	err := h.Apply(cdc.Event{Op: cdc.OpUpdate, Key: "k", Row: row("v", "late"), TS: 150})
	if !errors.Is(err, cdc.ErrOutOfOrder) {
		t.Fatalf("an event older than the current version: error = %v, want ErrOutOfOrder", err)
	}
	if !reflect.DeepEqual(before, h.Versions("k")) {
		t.Fatal("a rejected event changed the history")
	}
	// after a delete the key's timeline has already advanced to the delete's timestamp
	apply(t, h, cdc.Event{Op: cdc.OpDelete, Key: "k", TS: 300})
	if err := h.Apply(cdc.Event{Op: cdc.OpInsert, Key: "k", Row: row("v", "back-dated"), TS: 250}); !errors.Is(err, cdc.ErrOutOfOrder) {
		t.Fatalf("an insert dated inside the closed interval: error = %v, want ErrOutOfOrder (it would overlap the previous version)", err)
	}
	if err := h.Apply(cdc.Event{Op: cdc.OpInsert, Key: "k", Row: row("v", "later"), TS: 300}); err != nil {
		t.Fatalf("an insert at exactly the delete's timestamp must be accepted: %v", err)
	}
}

func TestCh22_History_NeverAliasesCallerMaps(t *testing.T) {
	h := cdc.NewHistory()
	input := row("v", "original")
	apply(t, h, cdc.Event{Op: cdc.OpInsert, Key: "k", Row: input, TS: 10})
	input["v"] = "MUTATED"
	vs, cur := h.Versions("k"), h.Current()
	if len(vs) != 1 || cur["k"] == nil {
		t.Fatalf("Versions = %v, Current = %v, want one version of k", vs, cur)
	}
	vs[0].Row["v"] = "MUTATED"
	cur["k"]["v"] = "MUTATED"
	got, ok := h.AsOf("k", 10)
	if !ok {
		t.Fatal("AsOf(k, 10) found nothing")
	}
	got["v"] = "MUTATED"
	if again, _ := h.AsOf("k", 10); again["v"] != "original" {
		t.Fatalf("the history aliased a caller's map: %v", again)
	}
}

func TestCh22_History_TimelinesStayWellFormedUnderARandomStream(t *testing.T) {
	log := randomChangeLog(1500, 25) // TS strictly increases with Seq
	h := cdc.NewHistory()
	for _, e := range log {
		if err := h.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	checked := 0
	for k := 0; k < 20; k++ {
		key := fmt.Sprintf("key-%02d", k)
		vs := h.Versions(key)
		for i, v := range vs {
			checked++
			if v.ValidFrom >= v.ValidTo && !(v.Current && v.ValidTo == cdc.OpenEnded) {
				t.Fatalf("%s version %d has an empty or inverted interval [%d, %d)", key, i, v.ValidFrom, v.ValidTo)
			}
			if v.Current != (v.ValidTo == cdc.OpenEnded) {
				t.Fatalf("%s version %d: Current=%v but ValidTo=%d", key, i, v.Current, v.ValidTo)
			}
			if v.Current && i != len(vs)-1 {
				t.Fatalf("%s: only the LAST version may be current", key)
			}
			if i > 0 && vs[i-1].ValidTo > v.ValidFrom {
				t.Fatalf("%s: versions %d and %d overlap in time", key, i-1, i)
			}
		}
	}
	if checked < 100 {
		t.Fatalf("only %d versions were produced: the history is not recording changes", checked)
	}
}

func TestCh22_History_AndTable_AgreeAboutTheCurrentState(t *testing.T) {
	log := randomChangeLog(1500, 26)
	h, tb := cdc.NewHistory(), cdc.NewTable()
	for _, e := range log {
		if err := h.Apply(e); err != nil {
			t.Fatal(err)
		}
		tb.Apply(e)
	}
	if got, want := h.Current(), tb.Rows(); !reflect.DeepEqual(got, want) || len(want) == 0 {
		t.Fatalf("the SCD2 history's current rows (%d) differ from the replicated table (%d)", len(got), len(want))
	}
	// and the past is queryable: replaying a prefix of the log into a Table must equal AsOf at that moment
	cut := 700
	prefix := cdc.NewTable()
	prefix.ApplyAll(log[:cut])
	asOf := log[cut-1].TS // the timestamp of the last event in the prefix
	for key, want := range prefix.Rows() {
		got, ok := h.AsOf(key, asOf)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("AsOf(%s, %d) = (%v, %v), the table after %d events says %v", key, asOf, got, ok, cut, want)
		}
	}
	for k := 0; k < 20; k++ {
		key := fmt.Sprintf("key-%02d", k)
		if _, alive := prefix.Get(key); !alive {
			if got, ok := h.AsOf(key, asOf); ok {
				t.Fatalf("AsOf(%s, %d) = %v but the table after %d events has no such row", key, asOf, got, cut)
			}
		}
	}
}
