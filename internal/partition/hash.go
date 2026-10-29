package partition

import "dataflow/internal/todo"

// Hash assigns a key to one of n partitions: hash(key) mod n.
//
//   - n < 1 -> an error wrapping ErrBadPartitionCount.
//   - Use hashx.Sum64(key) % uint64(n): the same key ALWAYS gets the same partition (that is what lets every worker agree,
//     with no coordination, on where a record belongs), and a GOOD hash spreads keys evenly.
//
// Hash partitioning is perfect for equality lookups and group-by/join (all rows of a key meet), useless for range queries
// ("keys between a and f" could be anywhere), and it has two weaknesses the rest of this chapter is about:
// SKEW (one hot key overloads one partition) and RESHARDING (change n and almost every key moves).
func Hash(key string, n int) (int, error) {
	return 0, todo.NotImplemented("Ch13: Hash")
}
