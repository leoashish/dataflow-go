package checkpoint_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dataflow/internal/checkpoint"
	"dataflow/internal/testutil"
)

// Chapter 07 -- checkpoints, atomic writes, idempotence.   Run:  go test ./internal/checkpoint -run Ch07 -race -v

func newStore(t *testing.T) (*checkpoint.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "cp")
	s, err := checkpoint.NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s, dir
}

// ---------------- Store ----------------

func TestCh07_Store_SavesAndLoads_AndOverwrites(t *testing.T) {
	s, _ := newStore(t)
	if err := s.Save("state", []byte("v1")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save("state", []byte("version two")); err != nil {
		t.Fatalf("Save again: %v", err)
	}
	got, err := s.Load("state")
	if err != nil || string(got) != "version two" {
		t.Fatalf("Load = (%q, %v), want the latest content", got, err)
	}
}

func TestCh07_Store_LoadOfAnUnknownName_IsErrNotFound(t *testing.T) {
	s, _ := newStore(t)
	if _, err := s.Load("never-saved"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

func TestCh07_Store_IgnoresLeftoverTemporaryFilesFromACrashedSave(t *testing.T) {
	s, dir := newStore(t)
	if err := s.Save("state", []byte("good")); err != nil {
		t.Fatal(err)
	}
	// A previous process died halfway through a Save: only the temp file got (partial) data.
	testutil.WriteFile(t, dir, "state.12345.tmp", "garbage that was never renamed")
	testutil.WriteFile(t, dir, "other.tmp", "more garbage")

	got, err := s.Load("state")
	if err != nil || string(got) != "good" {
		t.Fatalf("Load = (%q, %v): the old, complete checkpoint must survive a crashed Save", got, err)
	}
	if _, err := s.Load("other"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("a leftover temp file must not count as a checkpoint: %v", err)
	}
}

func TestCh07_Store_LeavesNoTemporaryFilesBehind(t *testing.T) {
	s, dir := newStore(t)
	for i := range 5 {
		if err := s.Save("state", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if names := testutil.DirEntries(t, dir); len(names) != 1 || names[0] != "state" {
		t.Fatalf("directory contains %v, want only [state]", names)
	}
}

func TestCh07_Store_RejectsNamesThatEscapeTheDirectory(t *testing.T) {
	s, _ := newStore(t)
	for _, name := range []string{"", ".", "..", "../evil", "a/b", `a\b`} {
		testutil.ExpectError(t, s.Save(name, []byte("x")))
		_, err := s.Load(name)
		testutil.ExpectError(t, err)
	}
}

func TestCh07_Store_ReadersNeverSeeAHalfWrittenCheckpoint(t *testing.T) {
	s, _ := newStore(t)
	payload := func(b byte) []byte { return bytes.Repeat([]byte{b}, 200_000) }
	if err := s.Save("state", payload('a')); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // a writer that keeps replacing the checkpoint
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.Save("state", payload(byte('a'+i%26))); err != nil {
				t.Errorf("Save: %v", err)
				return
			}
		}
	}()

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		data, err := s.Load("state")
		if err != nil {
			t.Errorf("Load: %v", err)
			break
		}
		if len(data) != 200_000 || !bytes.Equal(data, bytes.Repeat(data[:1], len(data))) {
			t.Errorf("a reader saw a torn checkpoint: len=%d (os.WriteFile truncates first; write a temp file and rename)", len(data))
			break
		}
	}
	close(stop)
	wg.Wait()
}

// ---------------- Deduper ----------------

func TestCh07_Deduper_RecognisesRepeats(t *testing.T) {
	d := checkpoint.NewDeduper(10)
	if d.Seen("a") || d.Seen("b") {
		t.Fatal("first sightings must report false")
	}
	if !d.Seen("a") || !d.Seen("b") {
		t.Fatal("second sightings must report true")
	}
	if d.Len() != 2 {
		t.Fatalf("Len = %d, want 2", d.Len())
	}
}

func TestCh07_Deduper_ForgetsTheLeastRecentlySeen_WhenFull(t *testing.T) {
	d := checkpoint.NewDeduper(3)
	d.Seen("a")
	d.Seen("b")
	d.Seen("c")
	d.Seen("d") // evicts a
	if d.Len() != 3 {
		t.Fatalf("Len = %d, want 3", d.Len())
	}
	if d.Seen("a") {
		t.Fatal("a was evicted: it must look new again (the documented trade-off of a bounded window)")
	}
}

func TestCh07_Deduper_ADuplicateRefreshesItsPosition(t *testing.T) {
	d := checkpoint.NewDeduper(3)
	d.Seen("a")
	d.Seen("b")
	d.Seen("c")
	d.Seen("a") // a is now the most recent; b is the oldest
	d.Seen("d") // evicts b, not a
	if !d.Seen("a") {
		t.Fatal("a was refreshed by its duplicate and must still be remembered")
	}
	if d.Seen("b") {
		t.Fatal("b was the least recently seen and must have been evicted")
	}
}

func TestCh07_Deduper_IsSafeForConcurrentUse(t *testing.T) {
	d := checkpoint.NewDeduper(100)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				d.Seen(fmt.Sprint((g*31 + i) % 250))
			}
		}()
	}
	wg.Wait()
	if d.Len() < 90 || d.Len() > 100 { // 250 distinct ids went through a window of 100
		t.Fatalf("Len = %d, want about 100 (full, and never more than 100)", d.Len())
	}
}

// ---------------- Run ----------------

func records(n int) []checkpoint.Record {
	recs := make([]checkpoint.Record, n)
	for i := range recs {
		recs[i] = checkpoint.Record{Offset: int64(i), Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i)}
	}
	return recs
}

// upsertSink is IDEMPOTENT: writing the same record twice leaves the same state.
type upsertSink struct {
	mu        sync.Mutex
	state     map[string]string
	writes    int
	failAtNth int // fail the nth write overall (1-based); 0 = never
}

func newUpsertSink() *upsertSink { return &upsertSink{state: map[string]string{}} }

func (s *upsertSink) Write(ctx context.Context, rec checkpoint.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	if s.failAtNth != 0 && s.writes == s.failAtNth {
		return errors.New("simulated crash")
	}
	s.state[rec.Key] = rec.Value
	return nil
}

// appendSink is NOT idempotent: every write adds a line.
type appendSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *appendSink) Write(ctx context.Context, rec checkpoint.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, rec.Key)
	return nil
}

func savedOffset(t *testing.T, s *checkpoint.Store, name string) int {
	t.Helper()
	data, err := s.Load(name)
	if errors.Is(err, checkpoint.ErrNotFound) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if _, err := fmt.Sscan(string(data), &n); err != nil {
		t.Fatalf("checkpoint %q is not a number: %v", data, err)
	}
	return n
}

func TestCh07_Run_CopiesEverything_AndASecondRunHasNothingToDo(t *testing.T) {
	store, _ := newStore(t)
	src := checkpoint.NewMemorySource(records(23))
	sink := newUpsertSink()
	opts := checkpoint.Options{Name: "job", BatchSize: 4, CheckpointEvery: 5}

	n, err := checkpoint.Run(context.Background(), src, sink, store, opts)
	if err != nil || n != 23 || len(sink.state) != 23 {
		t.Fatalf("first run: n=%d err=%v state=%d, want 23 records", n, err, len(sink.state))
	}
	if got := savedOffset(t, store, "job"); got != 23 {
		t.Fatalf("final checkpoint = %d, want 23 (save one when the source is exhausted)", got)
	}

	n, err = checkpoint.Run(context.Background(), src, sink, store, opts)
	if err != nil || n != 0 || sink.writes != 23 {
		t.Fatalf("second run: n=%d err=%v writes=%d, want no new writes", n, err, sink.writes)
	}
}

func TestCh07_Run_AfterACrash_ResumesFromTheLastCheckpoint(t *testing.T) {
	store, _ := newStore(t)
	src := checkpoint.NewMemorySource(records(12))
	sink := newUpsertSink()
	sink.failAtNth = 7 // the 7th write dies: records 0-5 are written
	opts := checkpoint.Options{Name: "job", BatchSize: 3, CheckpointEvery: 5}

	_, err := checkpoint.Run(context.Background(), src, sink, store, opts)
	if err == nil {
		t.Fatal("the first run should fail")
	}
	if got := savedOffset(t, store, "job"); got != 5 {
		t.Fatalf("checkpoint after the crash = %d, want 5 (records 0-4 were checkpointed; 5 was written but not yet covered)", got)
	}

	sink.failAtNth = 0
	n, err := checkpoint.Run(context.Background(), src, sink, store, opts) // the restart
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if n != 7 { // offsets 5..11
		t.Fatalf("the restart wrote %d records, want 7 (resume at offset 5)", n)
	}
	if len(sink.state) != 12 {
		t.Fatalf("the sink has %d distinct records, want all 12: nothing may be LOST", len(sink.state))
	}
	for i := range 12 {
		if sink.state[fmt.Sprintf("k%d", i)] != fmt.Sprintf("v%d", i) {
			t.Fatalf("record %d has the wrong value", i)
		}
	}
	// writes counts every call, the failed one included: 7 before the crash (6 ok + 1 failed) + 7 after the restart.
	if sink.writes != 14 {
		t.Fatalf("total write calls = %d, want 14 (record 5 was written twice: replay after a crash)", sink.writes)
	}
}

func TestCh07_Run_NeverCheckpointsPastWhatTheSinkAccepted(t *testing.T) {
	store, _ := newStore(t)
	sink := newUpsertSink()
	sink.failAtNth = 3
	_, err := checkpoint.Run(context.Background(), checkpoint.NewMemorySource(records(10)), sink, store,
		checkpoint.Options{Name: "job", BatchSize: 10, CheckpointEvery: 1})
	if err == nil {
		t.Fatal("expected the simulated crash")
	}
	if got := savedOffset(t, store, "job"); got != 2 { // records 0 and 1 were written; the 3rd failed
		t.Fatalf("checkpoint = %d, want 2: the failed record must not be covered (that would LOSE it)", got)
	}
}

func TestCh07_Run_WithANonIdempotentSink_ARestartProducesDuplicates(t *testing.T) {
	store, _ := newStore(t)
	src := checkpoint.NewMemorySource(records(6))
	flaky := &crashOnceSink{}
	opts := checkpoint.Options{Name: "job", BatchSize: 6, CheckpointEvery: 4}

	_, _ = checkpoint.Run(context.Background(), src, flaky, store, opts) // crashes on the 6th write; checkpoint = 4
	_, err := checkpoint.Run(context.Background(), src, flaky, store, opts)
	if err != nil {
		t.Fatal(err)
	}

	count := map[string]int{}
	for _, l := range flaky.lines {
		count[l]++
	}
	if count["k4"] != 2 || count["k0"] != 1 {
		t.Fatalf("counts=%v: records written after the last checkpoint (k4) are delivered twice -- at-least-once, not exactly-once", count)
	}
}

type crashOnceSink struct {
	lines   []string
	crashed bool
}

func (s *crashOnceSink) Write(ctx context.Context, rec checkpoint.Record) error {
	if !s.crashed && len(s.lines) == 5 {
		s.crashed = true
		return errors.New("crash")
	}
	s.lines = append(s.lines, rec.Key)
	return nil
}

type failingSource struct{ err error }

func (f failingSource) Read(context.Context, int64, int) ([]checkpoint.Record, error) {
	return nil, f.err
}

func TestCh07_Run_ReturnsSourceErrors_WithoutTouchingTheCheckpoint(t *testing.T) {
	store, _ := newStore(t)
	boom := errors.New("source unavailable")
	_, err := checkpoint.Run(context.Background(), failingSource{boom}, newUpsertSink(), store, checkpoint.Options{Name: "job"})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want the source error", err)
	}
	if _, err := store.Load("job"); !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("a checkpoint was saved although nothing was processed: %v", err)
	}
}

func TestCh07_Run_ACancelledContext_StopsBeforeDoingAnything(t *testing.T) {
	store, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink := newUpsertSink()
	n, err := checkpoint.Run(ctx, checkpoint.NewMemorySource(records(5)), sink, store, checkpoint.Options{Name: "job"})
	if !errors.Is(err, context.Canceled) || n != 0 || sink.writes != 0 {
		t.Fatalf("n=%d err=%v writes=%d, want context.Canceled and no writes", n, err, sink.writes)
	}
}

func TestCh07_Run_ASlowSinkCanBeInterrupted_AndTheNextRunContinuesWhereItStopped(t *testing.T) {
	store, _ := newStore(t)
	src := checkpoint.NewMemorySource(records(50))
	var written atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	sink := sinkFunc(func(c context.Context, rec checkpoint.Record) error {
		if written.Add(1) == 20 {
			cancel() // "SIGTERM" arrives while processing
		}
		return nil
	})
	opts := checkpoint.Options{Name: "job", BatchSize: 5, CheckpointEvery: 5}

	_, err := checkpoint.Run(ctx, src, sink, store, opts)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	cp := savedOffset(t, store, "job")
	if cp == 0 || cp > 25 {
		t.Fatalf("checkpoint = %d, want progress was saved (somewhere around 20)", cp)
	}

	idem := newUpsertSink()
	n, err := checkpoint.Run(context.Background(), src, idem, store, opts)
	if err != nil || int64(n) != 50-int64(cp) {
		t.Fatalf("resume: n=%d err=%v, want it to process exactly the %d records after the checkpoint", n, err, 50-cp)
	}
}

type sinkFunc func(context.Context, checkpoint.Record) error

func (f sinkFunc) Write(ctx context.Context, r checkpoint.Record) error { return f(ctx, r) }
