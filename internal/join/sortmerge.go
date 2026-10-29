package join

// SortMergeJoin sorts both inputs by the join key and walks them together like a zipper.
//
//	left  keys:  a a b d d        pointers move forward; when keys are equal, EVERY left row of that key group pairs with
//	right keys:  a b b c d          EVERY right row of the same key group (the cross product of the two groups).
//
// Why bother when HashJoin exists? Because sorted data merges in O(1) memory and streams: when both inputs are ALREADY
// sorted (a sorted file, a clustered index, the output of Chapter 10's external sort) you need no hash table at all.
//
// Steps:
//  1. Split each side into rows that HAVE a key and rows that don't. Rows without a key never match.
//  2. sort.SliceStable each keyed side by key (plain string comparison: "10" < "9").
//  3. Merge with two indexes. If lk < rk advance left (Inner: drop it; LeftOuter: emit it alone), if lk > rk advance right,
//     if equal find the end of both key groups and emit merge(l, r) for every pair, left rows major (left order, then right order).
//  4. LeftOuter: after the merge emit the left rows WITHOUT a key, alone, in input order.
//
// Output order: ascending by key (so for Inner the output is sorted by the join key).
func SortMergeJoin(left, right []Row, leftKey, rightKey string, kind Kind) []Row {
	return nil // TODO Ch12
}
