package sketch

import "dataflow/internal/todo"

// Counter is one monitored item of a SpaceSaving summary.
// The TRUE frequency f of Item satisfies  Count - Err <= f <= Count.
type Counter struct {
	Item  string
	Count uint64 // an upper bound on the true count
	Err   uint64 // how much of Count may be an over-estimate inherited from an evicted item
}

// SpaceSaving finds the HEAVY HITTERS ("top-K most frequent items") of a stream with only k counters -- the Space-Saving
// algorithm of Metwally et al. Count-Min can tell you how often a key you NAME occurred, but it cannot tell you WHICH keys
// are the frequent ones; this does.
//
// ALGORITHM -- keep at most k counters {item, count, err}. For each item x of the stream (Offer):
//
//  1. x already has a counter           -> count++
//  2. there is a free counter (< k used) -> new counter {x, count 1, err 0}
//  3. otherwise EVICT the counter with the SMALLEST count (call it m): reuse it for x with
//     count = m.count + 1,  err = m.count
//     (x might really have occurred up to m.count times before we started watching it: that is the error.)
//
// GUARANTEES: any item whose true frequency is greater than N/k is certainly among the k counters; and for every counter,
// Count - Err <= true count <= Count. When the stream has at most k distinct items, all counts are EXACT (Err = 0).
//
//   - NewSpaceSaving(k): k < 1 -> an error wrapping ErrBadParameter.
//   - Offer(item) counts one occurrence. N() = how many were offered.
//   - Top() returns the monitored counters sorted by Count descending, ties by Item ascending (a copy).
//   - Finding the minimum with a linear scan over the k counters is fine here (a heap or the "stream-summary" list makes it O(1);
//     see the stretch challenges). Use a map[string]*Counter plus the scan.
type SpaceSaving struct{}

func NewSpaceSaving(k int) (*SpaceSaving, error) {
	return nil, todo.NotImplemented("Ch16: NewSpaceSaving")
}

func (s *SpaceSaving) Offer(item string) {} // TODO Ch16

func (s *SpaceSaving) N() uint64 { return 0 } // TODO Ch16

func (s *SpaceSaving) Top() []Counter { return nil } // TODO Ch16
