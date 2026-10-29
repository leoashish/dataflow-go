package extsort

import "dataflow/internal/todo"

// KWayMerge merges k already-SORTED sequences into one sorted stream, using a min-heap of "the current head of each
// sequence". That is the heart of every external sort, every database sort-merge join and every LSM-tree compaction (you will use it in Chapter 20).
//
//	sequences:   [1 4 9]   [2 3 10]   [5]
//	heap holds:   1(seq0)    2(seq1)    5(seq2)     -> pop 1, refill from seq0 -> 4 ... -> 1 2 3 4 5 9 10
//
// Cost: O(n log k) for n items in total, and only k items in memory at a time.
//
//   - Each iterator returns (value, true) until exhausted, then (zero, false) forever. A nil entry in iters is treated as empty.
//   - less(a, b) is the ordering. The merge must be STABLE: when two heads are EQUAL under less (neither is less than the
//     other), the one from the LOWER-numbered iterator comes first. So the heap needs the iterator index as a tie-break.
//   - emit is called for every item in order. If it returns an error, stop and return that error.
//   - Implement container/heap on a slice of {value, source index} (see the Top-K chapter's notes if you need a refresher).
func KWayMerge[T any](iters []func() (T, bool), less func(a, b T) bool, emit func(T) error) error {
	return todo.NotImplemented("Ch10: KWayMerge")
}
