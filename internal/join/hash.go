package join

// HashJoin is the workhorse: BUILD a hash table of one side, PROBE it with the other.
//
//  1. BUILD: put every right row that has a key into map[key][]Row (keep the right rows' input order inside each slice).
//  2. PROBE: for each left row IN ORDER, look its key up -- O(1) -- and emit merge(left, match) for EVERY match, in the
//     right side's order.
//     - no match (or the left row has no key): Inner drops it; LeftOuter emits copyRow(left) alone.
//
// Cost O(|left| + |right|) -- compare a nested loop at O(|left| x |right|), hopeless beyond a few thousand rows.
// The catch: the build side must FIT IN MEMORY (build the SMALLER input). When it doesn't, see GraceHashJoin.
//
// Output order: by left row, then by right input order. Many-to-many keys produce the cross product.
func HashJoin(left, right []Row, leftKey, rightKey string, kind Kind) []Row {
	return nil // TODO Ch12
}
