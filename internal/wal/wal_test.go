package wal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"dataflow/internal/testutil"
	"dataflow/internal/wal"
)

// Chapter 19 -- the write-ahead log.   Run:  go test ./internal/wal -run Ch19 -race -v

// ref is an INDEPENDENT encoder written straight from the documented format, so the tests of Open/ScanRecords do not rely
// on the learner's EncodeRecord being right.
func ref(payload []byte) []byte {
	out := make([]byte, wal.HeaderSize+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(payload)))
	copy(out[8:], payload)
	table := crc32.MakeTable(crc32.Castagnoli)
	crc := crc32.Update(crc32.Update(0, table, out[0:4]), table, payload)
	binary.LittleEndian.PutUint32(out[4:8], crc)
	return out
}

func refLog(payloads ...string) []byte {
	var out []byte
	for _, p := range payloads {
		out = append(out, ref([]byte(p))...)
	}
	return out
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func strs(recs [][]byte) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r)
	}
	return out
}

func openLog(t *testing.T, path string, o wal.Options) *wal.WAL {
	t.Helper()
	w, err := wal.Open(path, o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

// ---------------- the record format ----------------

func TestCh19_EncodeRecord_HasTheDocumentedLayout(t *testing.T) {
	for _, payload := range []string{"hello", "", "x", string(bytes.Repeat([]byte{0, 255}, 300))} {
		got := wal.EncodeRecord([]byte(payload))
		if want := ref([]byte(payload)); !bytes.Equal(got, want) {
			t.Fatalf("payload of %d bytes: encoding differs from the spec\n got % x\nwant % x", len(payload), got[:min(len(got), 16)], want[:min(len(want), 16)])
		}
	}
}

func TestCh19_DecodeRecord_RoundTrips_AndConsumesExactlyOneRecord(t *testing.T) {
	data := append(ref([]byte("first")), ref([]byte("second"))...)
	payload, n, err := wal.DecodeRecord(data)
	if err != nil || string(payload) != "first" || n != wal.HeaderSize+5 {
		t.Fatalf("DecodeRecord = (%q, %d, %v), want (first, %d, nil)", payload, n, err, wal.HeaderSize+5)
	}
	payload[0] = 'X' // the payload must be a copy of the input
	if data[wal.HeaderSize] != 'f' {
		t.Fatal("DecodeRecord returned a slice aliasing the input buffer; it must copy")
	}
	empty, n, err := wal.DecodeRecord(ref(nil))
	if err != nil || len(empty) != 0 || n != wal.HeaderSize {
		t.Fatalf("empty payload: (%q, %d, %v)", empty, n, err)
	}
}

func TestCh19_DecodeRecord_EveryPrefixOfARecordIsTruncated(t *testing.T) {
	rec := ref([]byte("some payload"))
	if _, _, err := wal.DecodeRecord(rec); err != nil {
		t.Fatalf("the whole record must decode: %v", err)
	}
	for cut := 0; cut < len(rec); cut++ {
		if _, _, err := wal.DecodeRecord(rec[:cut]); !errors.Is(err, wal.ErrTruncated) {
			t.Fatalf("record cut to %d of %d bytes: error = %v, want ErrTruncated", cut, len(rec), err)
		}
	}
}

func TestCh19_DecodeRecord_AnySingleBitFlipIsDetected(t *testing.T) {
	rec := ref([]byte("some payload worth protecting"))
	if _, _, err := wal.DecodeRecord(rec); err != nil {
		t.Fatalf("the intact record must decode: %v", err)
	}
	for byteIdx := range rec {
		for bit := 0; bit < 8; bit++ {
			bad := slices.Clone(rec)
			bad[byteIdx] ^= 1 << bit
			payload, _, err := wal.DecodeRecord(bad)
			if err == nil {
				t.Fatalf("flipping bit %d of byte %d went UNDETECTED (payload %q)", bit, byteIdx, payload)
			}
			if !errors.Is(err, wal.ErrCorrupt) && !errors.Is(err, wal.ErrTruncated) {
				t.Fatalf("flipping bit %d of byte %d: error %v is neither ErrCorrupt nor ErrTruncated", bit, byteIdx, err)
			}
		}
	}
}

func TestCh19_DecodeRecord_AHugeLengthIsCorrupt_NotMerelyTruncated(t *testing.T) {
	bad := make([]byte, wal.HeaderSize)
	binary.LittleEndian.PutUint32(bad[0:4], 0xFFFFFFFF)
	_, _, err := wal.DecodeRecord(bad)
	if !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("a length of 4 GiB: error = %v, want ErrCorrupt (a length above MaxRecordSize can never be a legitimate torn write)", err)
	}
}

func TestCh19_ScanRecords_ReportsTheValidPrefixAndWhyItStopped(t *testing.T) {
	clean := refLog("a", "bb", "ccc")
	recs, valid, err := wal.ScanRecords(clean)
	if err != nil || valid != len(clean) || !slices.Equal(strs(recs), []string{"a", "bb", "ccc"}) {
		t.Fatalf("clean log: (%v, %d, %v)", strs(recs), valid, err)
	}

	torn := append(slices.Clone(clean), ref([]byte("dddd"))[:6]...) // 4th record only half written
	recs, valid, err = wal.ScanRecords(torn)
	if !errors.Is(err, wal.ErrTruncated) || valid != len(clean) || len(recs) != 3 {
		t.Fatalf("torn tail: (%d records, valid=%d, %v), want (3, %d, ErrTruncated)", len(recs), valid, err, len(clean))
	}

	corrupt := refLog("a", "bb", "ccc")
	corrupt[len(refLog("a"))+wal.HeaderSize] ^= 0xFF // damage the payload of the 2nd record
	recs, valid, err = wal.ScanRecords(corrupt)
	if !errors.Is(err, wal.ErrCorrupt) || valid != len(refLog("a")) || !slices.Equal(strs(recs), []string{"a"}) {
		t.Fatalf("corruption in record 2: (%v, valid=%d, %v), want only record 1 and ErrCorrupt", strs(recs), valid, err)
	}
}

// ---------------- the log file ----------------

func TestCh19_Log_AppendsAreNumberedFromOne_AndSurviveReopening(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w := openLog(t, path, wal.Options{})
	for i, p := range []string{"alpha", "beta", "gamma"} {
		seq, err := w.Append([]byte(p))
		if err != nil || seq != uint64(i+1) {
			t.Fatalf("Append(%q) = (%d, %v), want seq %d", p, seq, err, i+1)
		}
	}
	if w.Len() != 3 {
		t.Fatalf("Len = %d, want 3", w.Len())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2 := openLog(t, path, wal.Options{})
	if n, cut := w2.Recovered(); n != 3 || cut != 0 {
		t.Fatalf("Recovered = (%d, %d), want (3, 0)", n, cut)
	}
	if seq, _ := w2.Append([]byte("delta")); seq != 4 {
		t.Fatalf("after reopening the next seq is %d, want 4", seq)
	}
	all, err := wal.ReadAll(path)
	if err != nil || !slices.Equal(strs(all), []string{"alpha", "beta", "gamma", "delta"}) {
		t.Fatalf("ReadAll = (%v, %v)", strs(all), err)
	}
	if _, err := wal.ReadAll(filepath.Join(t.TempDir(), "missing.log")); err == nil {
		t.Fatal("ReadAll of a missing file must be an error")
	}
}

func TestCh19_Log_RecoversFromATornTail_AndAppendsCleanlyAfterwards(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	full := refLog("one", "two", "three", "four", "five")
	writeFile(t, path, full[:len(full)-4]) // the crash cut the last record short

	w := openLog(t, path, wal.Options{})
	n, cut := w.Recovered()
	wantValid := len(refLog("one", "two", "three", "four"))
	if n != 4 || cut != int64(len(full)-4-wantValid) || cut <= 0 || w.Len() != 4 {
		t.Fatalf("Recovered = (%d, %d), Len = %d; want 4 records and %d trailing bytes cut", n, cut, w.Len(), len(full)-4-wantValid)
	}
	if st, _ := os.Stat(path); st.Size() != int64(wantValid) {
		t.Fatalf("file is %d bytes after recovery, want %d: the torn tail must be truncated away", st.Size(), wantValid)
	}
	if seq, err := w.Append([]byte("fifth-again")); err != nil || seq != 5 {
		t.Fatalf("Append after recovery = (%d, %v), want seq 5", seq, err)
	}
	w.Close()
	all, _ := wal.ReadAll(path)
	if !slices.Equal(strs(all), []string{"one", "two", "three", "four", "fifth-again"}) {
		t.Fatalf("after recovery + append the log reads %v: a new record landed behind garbage", strs(all))
	}
}

func TestCh19_Log_CorruptionInTheMiddleDropsTheDamagedRecordAndEverythingAfterIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	data := refLog("r1", "r2", "r3", "r4", "r5")
	data[len(refLog("r1", "r2"))+wal.HeaderSize+1] ^= 0x40 // damage r3's payload
	writeFile(t, path, data)

	w := openLog(t, path, wal.Options{})
	if n, cut := w.Recovered(); n != 2 || cut != int64(len(refLog("r3", "r4", "r5"))) {
		t.Fatalf("Recovered = (%d, %d), want 2 records and %d bytes cut", n, cut, len(refLog("r3", "r4", "r5")))
	}
}

func TestCh19_Log_ARecoveryAtEveryPossibleCrashPoint_KeepsExactlyTheCompleteRecords(t *testing.T) {
	payloads := []string{"first", "", "third record", "4", "the fifth one is a bit longer than the others"}
	full := refLog(payloads...)
	var ends []int // byte offset where each record ends
	off := 0
	for _, p := range payloads {
		off += wal.HeaderSize + len(p)
		ends = append(ends, off)
	}
	dir := t.TempDir()
	for cut := 0; cut <= len(full); cut++ {
		path := filepath.Join(dir, fmt.Sprintf("crash-%03d.log", cut))
		writeFile(t, path, full[:cut])
		w, err := wal.Open(path, wal.Options{})
		if err != nil {
			t.Fatalf("crash at byte %d: Open: %v", cut, err)
		}
		complete := 0
		for _, e := range ends {
			if e <= cut {
				complete++
			}
		}
		if w.Len() != uint64(complete) {
			t.Fatalf("crash at byte %d of %d: recovered %d records, want %d (the ones fully on disk)", cut, len(full), w.Len(), complete)
		}
		if _, err := w.Append([]byte("after")); err != nil {
			t.Fatalf("crash at byte %d: Append after recovery: %v", cut, err)
		}
		w.Close()
		all, _ := wal.ReadAll(path)
		if len(all) != complete+1 || string(all[len(all)-1]) != "after" {
			t.Fatalf("crash at byte %d: after recovery and one append the log has %d records, want %d", cut, len(all), complete+1)
		}
	}
}

func TestCh19_Log_GroupCommit_OneFsyncForAWholeBatch(t *testing.T) {
	var syncs atomic.Int64
	opts := wal.Options{SyncEveryAppend: true, SyncFunc: func(f *os.File) error { syncs.Add(1); return f.Sync() }}
	w := openLog(t, filepath.Join(t.TempDir(), "wal.log"), opts)

	for i := 0; i < 3; i++ {
		if _, err := w.Append([]byte("single")); err != nil {
			t.Fatal(err)
		}
	}
	if syncs.Load() != 3 {
		t.Fatalf("%d syncs for 3 single appends with SyncEveryAppend, want 3", syncs.Load())
	}

	batch := make([][]byte, 100)
	for i := range batch {
		batch[i] = []byte(fmt.Sprint("rec-", i))
	}
	first, err := w.AppendBatch(batch)
	if err != nil || first != 4 {
		t.Fatalf("AppendBatch = (%d, %v), want first seq 4", first, err)
	}
	if syncs.Load() != 4 {
		t.Fatalf("%d syncs after a 100-record batch, want exactly 1 more (group commit)", syncs.Load())
	}
	if w.Len() != 103 {
		t.Fatalf("Len = %d, want 103", w.Len())
	}
}

func TestCh19_Log_WithoutSyncEveryAppend_FsyncHappensOnlyWhenAsked(t *testing.T) {
	var syncs atomic.Int64
	opts := wal.Options{SyncFunc: func(f *os.File) error { syncs.Add(1); return f.Sync() }}
	w := openLog(t, filepath.Join(t.TempDir(), "wal.log"), opts)
	for i := 0; i < 5; i++ {
		if _, err := w.Append([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if syncs.Load() != 0 {
		t.Fatalf("%d syncs happened without being asked", syncs.Load())
	}
	if err := w.Sync(); err != nil || syncs.Load() != 1 {
		t.Fatalf("Sync() = %v, syncs = %d, want one", err, syncs.Load())
	}
	if err := w.Close(); err != nil || syncs.Load() != 2 {
		t.Fatalf("Close() = %v, syncs = %d, want Close to sync once more", err, syncs.Load())
	}
}

func TestCh19_Log_AFailedFsyncPoisonsTheLog(t *testing.T) {
	boom := errors.New("disk on fire")
	var calls atomic.Int64
	opts := wal.Options{SyncEveryAppend: true, SyncFunc: func(*os.File) error {
		if calls.Add(1) == 2 {
			return boom
		}
		return nil
	}}
	w := openLog(t, filepath.Join(t.TempDir(), "wal.log"), opts)
	if _, err := w.Append([]byte("fine")); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if _, err := w.Append([]byte("doomed")); !errors.Is(err, boom) {
		t.Fatalf("append whose fsync failed: error = %v, want the sync error", err)
	}
	if _, err := w.Append([]byte("later")); !errors.Is(err, boom) {
		t.Fatalf("append after a failed fsync: error = %v: the log must stay poisoned (a retried fsync can falsely succeed)", err)
	}
	if err := w.Sync(); !errors.Is(err, boom) {
		t.Fatalf("Sync after poisoning: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("fsync was called %d times; after the failure it must NOT be retried", calls.Load())
	}
}

func TestCh19_Log_RejectsOversizeRecords_AndWorksNotAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w := openLog(t, path, wal.Options{})
	if _, err := w.Append([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	huge := make([]byte, wal.MaxRecordSize+1)
	if _, err := w.AppendBatch([][]byte{[]byte("before"), huge}); !errors.Is(err, wal.ErrTooLarge) {
		t.Fatalf("oversize payload: error = %v, want ErrTooLarge", err)
	}
	if w.Len() != 1 {
		t.Fatalf("Len = %d: a rejected batch must write NOTHING, not even the records before the oversize one", w.Len())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil (idempotent)", err)
	}
	if _, err := w.Append([]byte("late")); !errors.Is(err, wal.ErrClosed) {
		t.Fatalf("Append after Close: %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, wal.ErrClosed) {
		t.Fatalf("Sync after Close: %v, want ErrClosed", err)
	}
}

func TestCh19_Log_ConcurrentAppendersNeverInterleaveOrLoseRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w := openLog(t, path, wal.Options{})
	const goroutines, each = 8, 100

	seqs := make(chan uint64, goroutines*each)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				seq, err := w.Append([]byte(fmt.Sprintf("g%d-%03d", g, i)))
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				seqs <- seq
			}
		}()
	}
	wg.Wait()
	close(seqs)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	seen := map[uint64]bool{}
	for s := range seqs {
		if seen[s] || s < 1 || s > goroutines*each {
			t.Fatalf("sequence number %d is duplicated or out of range", s)
		}
		seen[s] = true
	}
	if len(seen) != goroutines*each {
		t.Fatalf("%d distinct sequence numbers, want %d", len(seen), goroutines*each)
	}
	all, err := wal.ReadAll(path)
	if err != nil || len(all) != goroutines*each {
		t.Fatalf("ReadAll = (%d records, %v), want %d", len(all), err, goroutines*each)
	}
	last := make([]int, goroutines)
	for i := range last {
		last[i] = -1
	}
	for _, r := range all {
		var g, i int
		if _, err := fmt.Sscanf(string(r), "g%d-%d", &g, &i); err != nil || i != last[g]+1 {
			t.Fatalf("record %q: a goroutine's own records must stay in order (expected index %d)", r, last[g]+1)
		}
		last[g] = i
	}
}

func TestCh19_Log_BinaryPayloadsAndEmptyPayloadsRoundTripThroughTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	rnd := testutil.Rand(19)
	var want [][]byte
	w := openLog(t, path, wal.Options{})
	for i := 0; i < 200; i++ {
		p := make([]byte, rnd.Intn(300))
		rnd.Read(p)
		want = append(want, p)
		if _, err := w.Append(p); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	got, err := wal.ReadAll(path)
	if err != nil || len(got) != len(want) {
		t.Fatalf("ReadAll = (%d records, %v), want %d", len(got), err, len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d changed in the round trip", i)
		}
	}
}
