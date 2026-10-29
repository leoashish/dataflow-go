package join

import "dataflow/internal/todo"

// GraceHashJoin joins inputs that are BOTH too big for memory.
//
// The idea: if keys hash to the same partition number they are the ONLY ones that can match. So cut BOTH inputs into P
// partitions by hash(key) % P, spilling each partition to its own file. Matching rows now sit in the SAME partition
// number on both sides, and each partition is (hopefully) small enough for an ordinary in-memory HashJoin:
//
//	left  ──hash(key)%P──▶ left-0  left-1  ... left-P-1        pass 1: partition (sequential writes only)
//	right ──hash(key)%P──▶ right-0 right-1 ... right-P-1
//	pass 2, for each p: load right-p into a hash table (BUILD), stream left-p through it (PROBE), emit.
//
// Memory use is the size of ONE right partition, ~|right|/P, instead of |right|.
//
// Details:
//   - Partitions < 1 -> a real error. Use hashx.Sum64(key) % uint64(P). Use ONE private temp dir
//     (os.MkdirTemp(o.TmpDir, "grace-")) and remove it always (defer os.RemoveAll), also on errors.
//   - Write spilled rows as JSON lines (encoding/json) through a bufio.Writer; read them back with a json.Decoder.
//   - Rows without a key never match and are NOT spilled: a LeftOuter left row without a key is emitted at once (copyRow),
//     an Inner one is dropped, a right row without a key is dropped.
//   - Fill GraceStats: SpilledLeft / SpilledRight count the rows that were written; MaxBuildRows is the largest number of
//     right rows that were in a build table at once.
//   - emit's error stops everything and is returned. Emission order is not specified (tests compare as sets).
//   - If the keys are heavily skewed (one key = most of the data) one partition is still huge: no partitioning scheme can
//     split ONE key. (Stretch: recursion, or a nested loop for the hot key.)
func GraceHashJoin(left, right Iter, leftKey, rightKey string, kind Kind, o GraceOptions, emit func(Row) error) (GraceStats, error) {
	return GraceStats{}, todo.NotImplemented("Ch12: GraceHashJoin")
}
