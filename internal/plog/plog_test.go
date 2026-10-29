package plog_test

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"

	"dataflow/internal/plog"
	"dataflow/internal/testutil"
)

// Chapter 08 -- a partitioned log with consumer groups.   Run:  go test ./internal/plog -run Ch08 -race -v

func newLog(t *testing.T, n int) *plog.Log {
	t.Helper()
	l, err := plog.New(n)
	if err != nil {
		t.Fatalf("plog.New(%d): %v", n, err)
	}
	return l
}

func TestCh08_New_RejectsFewerThanOnePartition(t *testing.T) {
	for _, n := range []int{0, -1} {
		_, err := plog.New(n)
		testutil.ExpectError(t, err)
	}
	if got := newLog(t, 5).Partitions(); got != 5 {
		t.Fatalf("Partitions = %d, want 5", got)
	}
}

func TestCh08_Append_SameKeyAlwaysSamePartition_WithConsecutiveOffsets(t *testing.T) {
	l := newLog(t, 8)
	p0, o0 := l.Append("user-42", []byte("a"))
	for i := int64(1); i < 20; i++ {
		p, o := l.Append("user-42", []byte("x"))
		if p != p0 || o != o0+i {
			t.Fatalf("append #%d went to partition %d offset %d, want partition %d offset %d", i, p, o, p0, o0+i)
		}
	}
}

func TestCh08_Append_SpreadsDifferentKeysAcrossPartitions(t *testing.T) {
	l := newLog(t, 4)
	counts := make([]int, 4)
	for i := range 4000 {
		p, _ := l.Append(fmt.Sprintf("user-%d", i), nil)
		counts[p]++
	}
	for p, c := range counts {
		if c < 800 || c > 1200 { // 1000 each, ±20%
			t.Fatalf("partition %d got %d of 4000 keys (%v): the spread is too uneven", p, c, counts)
		}
	}
}

func TestCh08_Append_WithoutAKey_IsRoundRobin(t *testing.T) {
	l := newLog(t, 3)
	var got []int
	for range 7 {
		p, _ := l.Append("", []byte("x"))
		got = append(got, p)
	}
	if want := []int{0, 1, 2, 0, 1, 2, 0}; !slices.Equal(got, want) {
		t.Fatalf("partitions = %v, want %v", got, want)
	}
}

func TestCh08_Append_CopiesTheValue(t *testing.T) {
	l := newLog(t, 1)
	buf := []byte("hello")
	l.Append("k", buf)
	buf[0] = 'J' // the producer reuses its buffer
	recs, err := l.Read(0, 0, 10)
	if err != nil || len(recs) != 1 || string(recs[0].Value) != "hello" {
		t.Fatalf("recs=%v err=%v, want the log to keep its own copy (\"hello\")", recs, err)
	}
}

func TestCh08_Read_FromAnOffset_WithALimit(t *testing.T) {
	l := newLog(t, 1)
	for i := range 10 {
		l.Append("k", []byte{byte('a' + i)})
	}
	recs, err := l.Read(0, 3, 4)
	if err != nil || len(recs) != 4 {
		t.Fatalf("recs=%v err=%v, want 4 records", recs, err)
	}
	for i, r := range recs {
		if r.Offset != int64(3+i) || r.Partition != 0 || r.Key != "k" || r.Value[0] != byte('a'+3+i) {
			t.Fatalf("record %d = %+v", i, r)
		}
	}
}

func TestCh08_Read_PastTheEnd_IsEmpty_NegativeFromIsZero_BadPartitionIsAnError(t *testing.T) {
	l := newLog(t, 2)
	p, _ := l.Append("k", []byte("v"))
	if recs, err := l.Read(p, 5, 10); err != nil || len(recs) != 0 {
		t.Fatalf("past the end: (%v, %v), want empty and no error", recs, err)
	}
	if recs, err := l.Read(p, -3, 10); err != nil || len(recs) != 1 {
		t.Fatalf("negative from: (%v, %v), want the one record", recs, err)
	}
	if _, err := l.Read(7, 0, 1); !errors.Is(err, plog.ErrInvalidPartition) {
		t.Fatalf("invalid partition: error = %v, want ErrInvalidPartition", err)
	}
	if _, err := l.Read(-1, 0, 1); !errors.Is(err, plog.ErrInvalidPartition) {
		t.Fatalf("negative partition: error = %v, want ErrInvalidPartition", err)
	}
}

func TestCh08_EndOffset_CountsTheRecordsOfAPartition(t *testing.T) {
	l := newLog(t, 1)
	for range 3 {
		l.Append("k", nil)
	}
	if end, err := l.EndOffset(0); err != nil || end != 3 {
		t.Fatalf("EndOffset = (%d, %v), want 3", end, err)
	}
	if _, err := l.EndOffset(9); !errors.Is(err, plog.ErrInvalidPartition) {
		t.Fatalf("error = %v, want ErrInvalidPartition", err)
	}
}

func TestCh08_Append_FromManyGoroutines_LosesNothing_AndKeepsOffsetsContiguous(t *testing.T) {
	l := newLog(t, 4)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				l.Append(fmt.Sprintf("key-%d-%d", g, i), nil)
			}
		}()
	}
	wg.Wait()
	total := int64(0)
	for p := range 4 {
		recs, _ := l.Read(p, 0, 1_000_000)
		for i, r := range recs {
			if r.Offset != int64(i) {
				t.Fatalf("partition %d: record %d has offset %d (gaps or duplicates)", p, i, r.Offset)
			}
		}
		total += int64(len(recs))
	}
	if total != 8000 {
		t.Fatalf("%d records stored, want 8000", total)
	}
}

// ---------------- consumer groups ----------------

func TestCh08_Group_AssignsPartitionsRoundRobinBySortedMemberName(t *testing.T) {
	g := plog.NewGroup(newLog(t, 4))
	g.Join("b") // joining order must not matter: members are sorted by name
	g.Join("a")
	if got := g.Assignment("a"); !slices.Equal(got, []int{0, 2}) {
		t.Fatalf("a owns %v, want [0 2]", got)
	}
	if got := g.Assignment("b"); !slices.Equal(got, []int{1, 3}) {
		t.Fatalf("b owns %v, want [1 3]", got)
	}
	if got := g.Assignment("stranger"); got != nil {
		t.Fatalf("a non-member owns %v, want nil", got)
	}
}

func TestCh08_Group_RebalancesWhenMembersJoinAndLeave(t *testing.T) {
	g := plog.NewGroup(newLog(t, 4))
	g.Join("a")
	if got := g.Assignment("a"); !slices.Equal(got, []int{0, 1, 2, 3}) {
		t.Fatalf("a alone owns %v, want everything", got)
	}
	g.Join("a") // joining twice is harmless
	g.Join("b")
	g.Join("c")
	var all []int
	for _, m := range []string{"a", "b", "c"} {
		all = append(all, g.Assignment(m)...)
	}
	sort.Ints(all)
	if !slices.Equal(all, []int{0, 1, 2, 3}) {
		t.Fatalf("with 3 members the assignments cover %v, want every partition exactly once", all)
	}
	g.Leave("b")
	g.Leave("c")
	if got := g.Assignment("a"); !slices.Equal(got, []int{0, 1, 2, 3}) {
		t.Fatalf("after the others left, a owns %v, want everything", got)
	}
}

func fillOnePartition(t *testing.T, l *plog.Log, n int) {
	t.Helper()
	for i := range n {
		l.Append("same-key", []byte(fmt.Sprint(i)))
	}
}

func TestCh08_Group_PollStartsAtTheCommittedOffset_AndRedeliversUntilCommitted(t *testing.T) {
	l := newLog(t, 1)
	fillOnePartition(t, l, 10)
	g := plog.NewGroup(l)
	g.Join("a")

	first, err := g.Poll("a", 4)
	if err != nil || len(first) != 4 || first[0].Offset != 0 {
		t.Fatalf("first poll = (%d records, %v)", len(first), err)
	}
	again, _ := g.Poll("a", 4) // nothing was committed: the SAME records come back (at-least-once)
	if len(again) != 4 || again[0].Offset != 0 {
		t.Fatalf("an uncommitted poll must redeliver; got offsets starting at %d", again[0].Offset)
	}

	if err := g.Commit(0, 4); err != nil {
		t.Fatal(err)
	}
	if g.Committed(0) != 4 {
		t.Fatalf("Committed = %d, want 4", g.Committed(0))
	}
	next, _ := g.Poll("a", 4)
	if len(next) != 4 || next[0].Offset != 4 {
		t.Fatalf("after commit the poll starts at offset %d, want 4", next[0].Offset)
	}
}

func TestCh08_Group_PollRespectsTheLimit_AndRejectsStrangers(t *testing.T) {
	l := newLog(t, 3)
	for i := range 30 {
		l.Append(fmt.Sprintf("k%d", i), nil)
	}
	g := plog.NewGroup(l)
	g.Join("a")
	recs, err := g.Poll("a", 7)
	if err != nil || len(recs) != 7 {
		t.Fatalf("got (%d records, %v), want exactly 7", len(recs), err)
	}
	if _, err := g.Poll("nobody", 5); !errors.Is(err, plog.ErrUnknownConsumer) {
		t.Fatalf("error = %v, want ErrUnknownConsumer", err)
	}
	if err := g.Commit(99, 1); !errors.Is(err, plog.ErrInvalidPartition) {
		t.Fatalf("error = %v, want ErrInvalidPartition", err)
	}
	if g.Committed(2) != 0 {
		t.Fatal("an uncommitted partition must report 0")
	}
}

func TestCh08_Group_ANewOwnerResumesFromTheCommittedOffset(t *testing.T) {
	l := newLog(t, 1)
	fillOnePartition(t, l, 10)
	g := plog.NewGroup(l)
	g.Join("a")
	recs, _ := g.Poll("a", 3)
	_ = g.Commit(0, recs[len(recs)-1].Offset+1) // a processed offsets 0-2

	g.Leave("a") // a crashes
	g.Join("b")  // b takes over the partition
	next, err := g.Poll("b", 100)

	if err != nil || len(next) != 7 || next[0].Offset != 3 {
		t.Fatalf("b got (%d records starting at %d, %v), want the 7 records from offset 3", len(next), func() int64 {
			if len(next) == 0 {
				return -1
			}
			return next[0].Offset
		}(), err)
	}
}

func TestCh08_Group_TwoConsumers_ReadEveryRecordExactlyOnce(t *testing.T) {
	l := newLog(t, 4)
	for i := range 200 {
		l.Append(fmt.Sprintf("key-%d", i), []byte(fmt.Sprint(i)))
	}
	g := plog.NewGroup(l)
	g.Join("worker-1")
	g.Join("worker-2")

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for _, name := range []string{"worker-1", "worker-2"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				recs, err := g.Poll(name, 16)
				if err != nil {
					t.Errorf("Poll: %v", err)
					return
				}
				if len(recs) == 0 {
					return
				}
				last := map[int]int64{}
				mu.Lock()
				for _, r := range recs {
					seen[fmt.Sprintf("%d/%d", r.Partition, r.Offset)]++
					last[r.Partition] = r.Offset
				}
				mu.Unlock()
				for p, off := range last {
					if err := g.Commit(p, off+1); err != nil {
						t.Errorf("Commit: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != 200 {
		t.Fatalf("%d distinct records read, want 200", len(seen))
	}
	for rec, n := range seen {
		if n != 1 {
			t.Fatalf("record %s was read %d times; each partition has ONE owner, so exactly once", rec, n)
		}
	}
}
