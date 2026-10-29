package lsm_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"dataflow/internal/lsm"
	"dataflow/internal/testutil"
)

// Chapter 20 -- a mini LSM-tree.   Run:  go test ./internal/lsm -run Ch20 -race -v
// (Needs your solutions to Chapters 10, 15 and 19: the LSM-tree is built from them.)

// ---------------- memtable ----------------

func TestCh20_Memtable_PutsOverwritesAndTombstones(t *testing.T) {
	m := lsm.NewMemtable()
	m.Put("b", "2")
	m.Put("a", "1")
	m.Put("c", "3")
	m.Put("b", "22") // overwrite
	m.Delete("c")    // a tombstone replaces the put
	m.Delete("zzz")  // deleting a key never written still records a tombstone: an OLDER table may hold it

	if m.Len() != 4 {
		t.Fatalf("Len = %d, want 4 distinct keys (tombstones count)", m.Len())
	}
	if e, ok := m.Get("b"); !ok || e != (lsm.Entry{Key: "b", Value: "22"}) {
		t.Fatalf("Get(b) = (%+v, %v)", e, ok)
	}
	if e, ok := m.Get("c"); !ok || !e.Tombstone || e.Value != "" {
		t.Fatalf("Get(c) = (%+v, %v), want a tombstone that is FOUND (the caller must learn 'deleted here')", e, ok)
	}
	if _, ok := m.Get("nope"); ok {
		t.Fatal("Get of a key never touched must report not found")
	}
	want := []lsm.Entry{{Key: "a", Value: "1"}, {Key: "b", Value: "22"}, {Key: "c", Tombstone: true}, {Key: "zzz", Tombstone: true}}
	if got := m.Entries(); !slices.Equal(got, want) {
		t.Fatalf("Entries() = %+v, want %+v (sorted by key)", got, want)
	}
}

// ---------------- sstable ----------------

func makeEntries(n int) []lsm.Entry {
	out := make([]lsm.Entry, n)
	for i := range out {
		out[i] = lsm.Entry{Key: fmt.Sprintf("key-%05d", i), Value: fmt.Sprintf("value-%d", i)}
		switch {
		case i%7 == 3:
			out[i] = lsm.Entry{Key: out[i].Key, Tombstone: true}
		case i%11 == 5:
			out[i].Value = "" // an empty VALUE is legal and is not a tombstone
		}
	}
	return out
}

func writeAndOpen(t *testing.T, entries []lsm.Entry) (*lsm.SSTable, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := lsm.WriteSSTable(path, entries, 0.01); err != nil {
		t.Fatalf("WriteSSTable: %v", err)
	}
	tbl, err := lsm.OpenSSTable(path, 0.01)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	t.Cleanup(func() { tbl.Close() })
	return tbl, path
}

func TestCh20_SSTable_RoundTripsEveryEntry_IncludingTombstonesAndEmptyValues(t *testing.T) {
	entries := makeEntries(1000)
	tbl, path := writeAndOpen(t, entries)
	if tbl.Len() != 1000 || tbl.Path() != path {
		t.Fatalf("Len = %d, Path = %q", tbl.Len(), tbl.Path())
	}
	for _, want := range entries {
		got, ok, err := tbl.Get(want.Key)
		if err != nil || !ok || got != want {
			t.Fatalf("Get(%q) = (%+v, %v, %v), want %+v", want.Key, got, ok, err, want)
		}
	}
	all, err := tbl.Entries()
	if err != nil || !slices.Equal(all, entries) {
		t.Fatalf("Entries() differs from what was written (err=%v, %d entries)", err, len(all))
	}
	for _, absent := range []string{"", "a", "key-00000x", "key-99999", "zzzz", "key-0000"} {
		if _, ok, err := tbl.Get(absent); ok || err != nil {
			t.Fatalf("Get(%q) = found=%v err=%v, want not found", absent, ok, err)
		}
	}
}

func TestCh20_SSTable_FileStartsWithTheDocumentedHeader(t *testing.T) {
	_, path := writeAndOpen(t, makeEntries(37))
	data, err := os.ReadFile(path)
	if err != nil || len(data) < 8 {
		t.Fatalf("ReadFile: %v (%d bytes)", err, len(data))
	}
	if string(data[:4]) != lsm.SSTableMagic || binary.LittleEndian.Uint32(data[4:8]) != 37 {
		t.Fatalf("header = %q + count %d, want %q + 37", data[:4], binary.LittleEndian.Uint32(data[4:8]), lsm.SSTableMagic)
	}
}

func TestCh20_SSTable_RejectsUnsortedOrDuplicateEntries_LeavingNoFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")
	for name, entries := range map[string][]lsm.Entry{
		"unsorted":  {{Key: "b"}, {Key: "a"}},
		"duplicate": {{Key: "a"}, {Key: "a", Value: "again"}},
	} {
		err := lsm.WriteSSTable(path, entries, 0.01)
		testutil.ExpectError(t, err)
		if names := testutil.DirEntries(t, dir); len(names) != 0 {
			t.Fatalf("%s: a failed write left files behind: %v", name, names)
		}
	}
}

func TestCh20_SSTable_AnEmptyTableIsValid(t *testing.T) {
	tbl, _ := writeAndOpen(t, nil)
	if tbl.Len() != 0 {
		t.Fatalf("Len = %d", tbl.Len())
	}
	if _, ok, err := tbl.Get("x"); ok || err != nil {
		t.Fatalf("Get on an empty table = (%v, %v)", ok, err)
	}
	if all, err := tbl.Entries(); err != nil || len(all) != 0 {
		t.Fatalf("Entries = (%v, %v)", all, err)
	}
}

func TestCh20_SSTable_ALookupReadsAtMostOneBlock(t *testing.T) {
	entries := makeEntries(1600) // 100 blocks of 16
	tbl, _ := writeAndOpen(t, entries)
	for _, e := range entries[:500] {
		before := tbl.BlockReads()
		if _, ok, err := tbl.Get(e.Key); !ok || err != nil {
			t.Fatalf("Get(%q) = (%v, %v)", e.Key, ok, err)
		}
		if d := tbl.BlockReads() - before; d != 1 {
			t.Fatalf("Get(%q) read %d blocks, want exactly 1: the sparse index must pinpoint the block (a file this size has 100)", e.Key, d)
		}
	}
}

// The counter above is reported by the code under test, so it cannot prove WHERE the code reads. This test observes it from
// the outside: after the table is open (its index is in memory), we trash the FIRST block on disk. A lookup that really jumps
// to its own block never notices; one that scans from the start of the file trips over the garbage.
func TestCh20_SSTable_ALookupNeverTouchesTheBlocksBeforeItsOwn(t *testing.T) {
	entries := makeEntries(1600)
	tbl, path := writeAndOpen(t, entries)

	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	garbage := make([]byte, 200)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	if _, err := f.WriteAt(garbage, 8); err != nil { // the start of block 0, right after the header
		t.Fatal(err)
	}
	f.Close()

	for _, e := range entries[16*3:] { // every key from block 3 onwards
		got, ok, err := tbl.Get(e.Key)
		if err != nil || !ok || got != e {
			t.Fatalf("Get(%q) = (%+v, %v, %v): it must read only its own block, yet something read the damaged block 0", e.Key, got, ok, err)
		}
	}
}

func TestCh20_SSTable_BloomFilterAnswersMostAbsentKeysWithoutTouchingTheFile(t *testing.T) {
	tbl, _ := writeAndOpen(t, makeEntries(2000))
	const probes = 3000
	for i := 0; i < probes; i++ {
		if _, ok, err := tbl.Get(fmt.Sprintf("key-%05d-absent", i)); ok || err != nil {
			t.Fatalf("an absent key was found or errored: %v %v", ok, err)
		}
	}
	if skips := tbl.BloomSkips(); skips < probes*95/100 {
		t.Fatalf("only %d of %d absent lookups were answered by the Bloom filter, want at least 95%% (false-positive rate is 1%%)", skips, probes)
	}
	if reads := tbl.BlockReads(); reads > probes*5/100 {
		t.Fatalf("%d block reads for %d absent lookups: the Bloom filter should have prevented nearly all of them", reads, probes)
	}
	if tbl.BloomSkips()+tbl.BlockReads() < probes*9/10 { // every lookup either skipped or read (or fell before the first key)
		t.Fatalf("skips %d + reads %d do not account for the lookups", tbl.BloomSkips(), tbl.BlockReads())
	}
}

func TestCh20_SSTable_OpenRejectsDamagedFiles(t *testing.T) {
	_, path := writeAndOpen(t, makeEntries(50))
	good, _ := os.ReadFile(path)
	dir := t.TempDir()

	check := func(name string, data []byte) {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		tbl, err := lsm.OpenSSTable(p, 0.01)
		if err == nil {
			tbl.Close()
			t.Fatalf("%s: OpenSSTable accepted a damaged file", name)
		}
		if !errors.Is(err, lsm.ErrCorruptTable) {
			t.Fatalf("%s: error %v does not wrap ErrCorruptTable", name, err)
		}
	}
	check("badmagic.sst", append([]byte("XXXX"), good[4:]...))
	check("short.sst", good[:5])
	check("truncated.sst", good[:len(good)-3])
	check("trailing.sst", append(slices.Clone(good), 1, 2, 3))
	check("lies-about-count.sst", func() []byte {
		b := slices.Clone(good)
		binary.LittleEndian.PutUint32(b[4:8], 51)
		return b
	}())
}

// ---------------- the database ----------------

func openDB(t *testing.T, dir string, mod func(*lsm.Options)) *lsm.DB {
	t.Helper()
	opts := lsm.Options{Dir: dir}
	if mod != nil {
		mod(&opts)
	}
	db, err := lsm.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustGet(t *testing.T, db *lsm.DB, key string) (string, bool) {
	t.Helper()
	v, ok, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v, ok
}

func mustPut(t *testing.T, db *lsm.DB, key, value string) {
	t.Helper()
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func mustDelete(t *testing.T, db *lsm.DB, key string) {
	t.Helper()
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

func TestCh20_DB_PutGetOverwriteDelete(t *testing.T) {
	db := openDB(t, t.TempDir(), nil)
	mustPut(t, db, "name", "ada")
	mustPut(t, db, "city", "london")
	mustPut(t, db, "name", "grace")
	if v, ok := mustGet(t, db, "name"); !ok || v != "grace" {
		t.Fatalf("Get(name) = (%q, %v), want grace", v, ok)
	}
	mustDelete(t, db, "city")
	if _, ok := mustGet(t, db, "city"); ok {
		t.Fatal("a deleted key is still found")
	}
	mustPut(t, db, "empty", "")
	if v, ok := mustGet(t, db, "empty"); !ok || v != "" {
		t.Fatalf("a key with an empty value: (%q, %v), want found with an empty value", v, ok)
	}
	if _, ok := mustGet(t, db, "never"); ok {
		t.Fatal("a key never written is found")
	}
	if err := db.Put("", "x"); !errors.Is(err, lsm.ErrEmptyKey) {
		t.Fatalf("Put with an empty key: %v, want ErrEmptyKey", err)
	}
	if err := db.Delete(""); !errors.Is(err, lsm.ErrEmptyKey) {
		t.Fatalf("Delete with an empty key: %v, want ErrEmptyKey", err)
	}
	if _, _, err := db.Get(""); !errors.Is(err, lsm.ErrEmptyKey) {
		t.Fatalf("Get with an empty key: %v, want ErrEmptyKey", err)
	}
}

func TestCh20_DB_FlushMovesTheMemtableIntoAnSSTable(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, nil)
	for i := 0; i < 10; i++ {
		mustPut(t, db, fmt.Sprintf("k%02d", i), fmt.Sprint("v", i))
	}
	if st := db.Stats(); st.MemtableEntries != 10 || st.Tables != 0 {
		t.Fatalf("before flush: %+v", st)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if st := db.Stats(); st.MemtableEntries != 0 || st.Tables != 1 {
		t.Fatalf("after flush: %+v, want an empty memtable and one table", st)
	}
	if sst, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(sst) != 1 || filepath.Base(sst[0]) != "000001.sst" {
		t.Fatalf("sstable files: %v, want 000001.sst", sst)
	}
	for i := 0; i < 10; i++ {
		if v, ok := mustGet(t, db, fmt.Sprintf("k%02d", i)); !ok || v != fmt.Sprint("v", i) {
			t.Fatalf("after flush Get(k%02d) = (%q, %v)", i, v, ok)
		}
	}
	if err := db.Flush(); err != nil || db.Stats().Tables != 1 {
		t.Fatalf("flushing an empty memtable must change nothing (err=%v, tables=%d)", err, db.Stats().Tables)
	}
}

func TestCh20_DB_TheMemtableFlushesItselfWhenFull(t *testing.T) {
	db := openDB(t, t.TempDir(), func(o *lsm.Options) { o.MemtableMaxEntries = 100 })
	for i := 0; i < 250; i++ {
		mustPut(t, db, fmt.Sprintf("key-%04d", i), "x")
	}
	if st := db.Stats(); st.Tables != 2 || st.MemtableEntries != 50 {
		t.Fatalf("after 250 puts with a limit of 100: %+v, want 2 tables and 50 entries in the memtable", st)
	}
}

func TestCh20_DB_NewestVersionWinsAcrossTables_AndTombstonesHideOlderValues(t *testing.T) {
	db := openDB(t, t.TempDir(), nil)
	mustPut(t, db, "k", "v1")
	mustPut(t, db, "gone", "still here?")
	mustPut(t, db, "survivor", "yes")
	db.Flush()
	mustPut(t, db, "k", "v2")
	mustDelete(t, db, "gone")
	db.Flush()
	mustPut(t, db, "k", "v3") // newest, still in the memtable

	if v, _ := mustGet(t, db, "k"); v != "v3" {
		t.Fatalf("k = %q, want v3 (memtable beats both tables)", v)
	}
	db.Flush()
	if v, _ := mustGet(t, db, "k"); v != "v3" {
		t.Fatalf("k = %q, want v3 (newest table beats older ones)", v)
	}
	if _, ok := mustGet(t, db, "gone"); ok {
		t.Fatal("'gone' came back: the tombstone in a newer table must shadow the value in an older one")
	}
	if v, ok := mustGet(t, db, "survivor"); !ok || v != "yes" {
		t.Fatalf("survivor = (%q, %v)", v, ok)
	}
	if db.Stats().Tables != 3 {
		t.Fatalf("Tables = %d, want 3", db.Stats().Tables)
	}
}

func TestCh20_DB_RecoversUnflushedWritesFromTheWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := lsm.Open(lsm.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		mustPut(t, db, fmt.Sprintf("k%02d", i), fmt.Sprint("v", i))
	}
	mustDelete(t, db, "k07")
	mustPut(t, db, "k08", "rewritten")
	if err := db.Close(); err != nil { // never flushed: the data lives only in wal.log
		t.Fatal(err)
	}

	db2 := openDB(t, dir, nil)
	if st := db2.Stats(); st.Tables != 0 || st.MemtableEntries != 50 {
		t.Fatalf("after reopening: %+v, want the 50 keys replayed into the memtable", st)
	}
	if _, ok := mustGet(t, db2, "k07"); ok {
		t.Fatal("a deletion in the WAL was not replayed")
	}
	if v, _ := mustGet(t, db2, "k08"); v != "rewritten" {
		t.Fatalf("k08 = %q: later WAL records must override earlier ones", v)
	}
	if v, _ := mustGet(t, db2, "k49"); v != "v49" {
		t.Fatalf("k49 = %q", v)
	}
}

func TestCh20_DB_AfterAFlushTheOldWALIsNotReplayedAgain_AndTableNumbersContinue(t *testing.T) {
	dir := t.TempDir()
	db, _ := lsm.Open(lsm.Options{Dir: dir})
	mustPut(t, db, "a", "1")
	db.Flush()
	mustPut(t, db, "b", "2")
	db.Close()

	db2 := openDB(t, dir, nil)
	if st := db2.Stats(); st.Tables != 1 || st.MemtableEntries != 1 {
		t.Fatalf("reopened: %+v, want 1 table and only 'b' in the memtable", st)
	}
	db2.Flush()
	sst, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	sort.Strings(sst)
	if len(sst) != 2 || filepath.Base(sst[1]) != "000002.sst" {
		t.Fatalf("sstables %v: the second flush must create 000002.sst, not overwrite 000001.sst", sst)
	}
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		if v, ok := mustGet(t, db2, k); !ok || v != want {
			t.Fatalf("Get(%q) = (%q, %v)", k, v, ok)
		}
	}
}

func TestCh20_DB_CompactionMergesEverythingIntoOneTable_AndPhysicallyDropsDeadData(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, nil)
	for round := 0; round < 4; round++ { // 4 versions of each of 100 keys, one table per round
		for i := 0; i < 100; i++ {
			mustPut(t, db, fmt.Sprintf("key-%03d", i), fmt.Sprintf("round-%d", round))
		}
		db.Flush()
	}
	for i := 0; i < 100; i += 2 { // delete the even keys
		mustDelete(t, db, fmt.Sprintf("key-%03d", i))
	}
	if st := db.Stats(); st.Tables != 4 {
		t.Fatalf("before compaction: %+v", st)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if st := db.Stats(); st.Tables != 1 || st.MemtableEntries != 0 {
		t.Fatalf("after compaction: %+v, want one table and an empty memtable (Compact flushes first)", st)
	}
	sst, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(sst) != 1 {
		t.Fatalf("%d .sst files on disk after compaction, want 1 (old ones must be deleted): %v", len(sst), sst)
	}
	tbl, err := lsm.OpenSSTable(sst[0], 0.01)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	entries, _ := tbl.Entries()
	if len(entries) != 50 {
		t.Fatalf("the compacted table has %d entries, want exactly the 50 live keys (no old versions, no tombstones)", len(entries))
	}
	for _, e := range entries {
		if e.Tombstone || e.Value != "round-3" {
			t.Fatalf("compacted table holds %+v: only the newest, live version may remain", e)
		}
	}
	for i := 0; i < 100; i++ {
		v, ok := mustGet(t, db, fmt.Sprintf("key-%03d", i))
		if i%2 == 0 && ok {
			t.Fatalf("key-%03d was deleted but is back", i)
		}
		if i%2 == 1 && (!ok || v != "round-3") {
			t.Fatalf("key-%03d = (%q, %v), want round-3", i, v, ok)
		}
	}
}

func TestCh20_DB_CompactingAwayEverythingLeavesNoTables(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	db.Flush()
	mustDelete(t, db, "a")
	mustDelete(t, db, "b")
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if st := db.Stats(); st.Tables != 0 {
		t.Fatalf("Tables = %d, want 0 when no live key remains", st.Tables)
	}
	if sst, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(sst) != 0 {
		t.Fatalf("leftover tables: %v", sst)
	}
	if err := db.Compact(); err != nil { // compacting nothing is fine
		t.Fatalf("Compact on an empty store: %v", err)
	}
}

func TestCh20_DB_CompactsAutomaticallyAfterEnoughTables(t *testing.T) {
	db := openDB(t, t.TempDir(), func(o *lsm.Options) { o.MemtableMaxEntries = 20; o.CompactAfter = 3 })
	for i := 0; i < 130; i++ {
		mustPut(t, db, fmt.Sprintf("k%04d", i%70), fmt.Sprint("v", i))
	}
	if st := db.Stats(); st.Tables >= 3 {
		t.Fatalf("%+v: with CompactAfter = 3 the table count must never stay at 3 or more", st)
	}
	for i := 60; i < 130; i++ { // the newest value of each key survived
		if v, ok := mustGet(t, db, fmt.Sprintf("k%04d", i%70)); !ok || v != fmt.Sprint("v", i) {
			t.Fatalf("k%04d = (%q, %v), want v%d", i%70, v, ok, i)
		}
	}
}

func TestCh20_DB_ScanReturnsTheLiveKeysInRangeInOrder(t *testing.T) {
	db := openDB(t, t.TempDir(), nil)
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		mustPut(t, db, k, "old-"+k)
	}
	db.Flush()
	mustPut(t, db, "c", "new-c") // overrides a flushed value
	mustDelete(t, db, "d")       // hides a flushed value
	db.Flush()
	mustPut(t, db, "bb", "mem") // memtable only
	mustDelete(t, db, "f")

	pairs := func(kvs []lsm.KV) string {
		var sb strings.Builder
		for _, kv := range kvs {
			fmt.Fprintf(&sb, "%s=%s ", kv.Key, kv.Value)
		}
		return strings.TrimSpace(sb.String())
	}
	cases := []struct{ from, to, want string }{
		{"", "", "a=old-a b=old-b bb=mem c=new-c e=old-e"},
		{"b", "e", "b=old-b bb=mem c=new-c"}, // [b, e): 'e' excluded, 'd' deleted
		{"c", "", "c=new-c e=old-e"},
		{"x", "", ""},
		{"", "b", "a=old-a"},
		{"bb", "bb", ""}, // an empty range
	}
	for _, c := range cases {
		got, err := db.Scan(c.from, c.to)
		if err != nil {
			t.Fatalf("Scan(%q, %q): %v", c.from, c.to, err)
		}
		if p := pairs(got); p != c.want {
			t.Errorf("Scan(%q, %q) = %q, want %q", c.from, c.to, p, c.want)
		}
	}
}

func TestCh20_DB_BloomFiltersMakeAbsentLookupsCheapAcrossManyTables(t *testing.T) {
	db := openDB(t, t.TempDir(), nil)
	for table := 0; table < 5; table++ {
		for i := 0; i < 400; i++ {
			mustPut(t, db, fmt.Sprintf("t%d-key-%03d", table, i), "v")
		}
		db.Flush()
	}
	before := db.Stats()
	const lookups = 500
	for i := 0; i < lookups; i++ {
		if _, ok := mustGet(t, db, fmt.Sprintf("absent-%04d", i)); ok {
			t.Fatal("an absent key was found")
		}
	}
	after := db.Stats()
	skipped, read := after.BloomSkips-before.BloomSkips, after.BlockReads-before.BlockReads
	if want := uint64(lookups * 5 * 90 / 100); skipped < want {
		t.Fatalf("%d table probes skipped by Bloom filters out of %d, want at least %d", skipped, lookups*5, want)
	}
	if read > lookups*5*5/100 {
		t.Fatalf("%d block reads for %d absent lookups over 5 tables", read, lookups)
	}
	// a key that exists in the OLDEST table must still be found (and newer tables are skipped by their filters)
	if _, ok := mustGet(t, db, "t0-key-123"); !ok {
		t.Fatal("a key in the oldest table was not found")
	}
}

func TestCh20_DB_ClosedDatabaseRefusesEverything_AndCloseIsIdempotent(t *testing.T) {
	db, err := lsm.Open(lsm.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "k", "v")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if err := db.Put("a", "b"); !errors.Is(err, lsm.ErrClosed) {
		t.Fatalf("Put after Close: %v", err)
	}
	if _, _, err := db.Get("k"); !errors.Is(err, lsm.ErrClosed) {
		t.Fatalf("Get after Close: %v", err)
	}
	if _, err := db.Scan("", ""); !errors.Is(err, lsm.ErrClosed) {
		t.Fatalf("Scan after Close: %v", err)
	}
	if err := db.Flush(); !errors.Is(err, lsm.ErrClosed) {
		t.Fatalf("Flush after Close: %v", err)
	}
	if err := db.Compact(); !errors.Is(err, lsm.ErrClosed) {
		t.Fatalf("Compact after Close: %v", err)
	}
}

func TestCh20_DB_SafeForConcurrentUse(t *testing.T) {
	db := openDB(t, t.TempDir(), func(o *lsm.Options) { o.MemtableMaxEntries = 50; o.CompactAfter = 4 })
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				key, val := fmt.Sprintf("g%d-%03d", g, i), fmt.Sprint("v", i)
				if err := db.Put(key, val); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if v, ok, err := db.Get(key); err != nil || !ok || v != val {
					t.Errorf("Get(%q) = (%q, %v, %v) right after its Put", key, v, ok, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for g := 0; g < 4; g++ {
		for i := 0; i < 250; i++ {
			if v, ok := mustGet(t, db, fmt.Sprintf("g%d-%03d", g, i)); !ok || v != fmt.Sprint("v", i) {
				t.Fatalf("g%d-%03d = (%q, %v)", g, i, v, ok)
			}
		}
	}
}

// The strongest test: thousands of random operations, including flushes, compactions and restarts, checked against a plain map.
func TestCh20_DB_BehavesLikeAMapUnderThousandsOfRandomOperations(t *testing.T) {
	dir := t.TempDir()
	rnd := testutil.Rand(20)
	open := func() *lsm.DB {
		db, err := lsm.Open(lsm.Options{Dir: dir, MemtableMaxEntries: 40, CompactAfter: 5})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	db := open()
	t.Cleanup(func() { db.Close() })
	model := map[string]string{}
	key := func() string { return fmt.Sprintf("key-%03d", rnd.Intn(300)) }

	for step := 0; step < 4000; step++ {
		switch r := rnd.Intn(100); {
		case r < 55:
			k, v := key(), fmt.Sprintf("value-%d", step)
			if err := db.Put(k, v); err != nil {
				t.Fatalf("step %d: Put: %v", step, err)
			}
			model[k] = v
		case r < 75:
			k := key()
			if err := db.Delete(k); err != nil {
				t.Fatalf("step %d: Delete: %v", step, err)
			}
			delete(model, k)
		case r < 93:
			k := key()
			got, ok, err := db.Get(k)
			want, wantOK := model[k]
			if err != nil || ok != wantOK || got != want {
				t.Fatalf("step %d: Get(%q) = (%q, %v, %v), the map says (%q, %v)", step, k, got, ok, err, want, wantOK)
			}
		case r < 96:
			if err := db.Flush(); err != nil {
				t.Fatalf("step %d: Flush: %v", step, err)
			}
		case r < 98:
			if err := db.Compact(); err != nil {
				t.Fatalf("step %d: Compact: %v", step, err)
			}
		default: // restart: everything must come back from the tables and the WAL
			if err := db.Close(); err != nil {
				t.Fatalf("step %d: Close: %v", step, err)
			}
			db = open()
		}
	}

	var wantKeys []string
	for k := range model {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(wantKeys)
	got, err := db.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("final Scan returned %d pairs, the map has %d", len(got), len(wantKeys))
	}
	for i, kv := range got {
		if kv.Key != wantKeys[i] || kv.Value != model[kv.Key] {
			t.Fatalf("final Scan pair %d = %+v, the map says %s=%s", i, kv, wantKeys[i], model[wantKeys[i]])
		}
	}
}
