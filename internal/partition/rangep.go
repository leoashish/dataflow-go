package partition

import "dataflow/internal/todo"

// RangePartitioner splits the SORTED key space into contiguous ranges, like the volumes of an encyclopedia.
//
// With boundaries b0 < b1 < ... < b(k-1) there are k+1 partitions:
//
//	partition 0:      key <  b0
//	partition i:      b(i-1) <= key < b(i)
//	partition k:      key >= b(k-1)          (a key EQUAL to a boundary belongs to the UPPER partition)
//
// Range partitioning keeps neighbours together, so range scans and "sorted output files" are cheap (this is how a
// distributed sort, HBase and Bigtable shard data). But where do the boundaries come from? Evenly spaced letters are a
// disaster for real data (70% of your keys may start with the same letter): see BoundariesFromSample.
//
//   - NewRangePartitioner: boundaries must be STRICTLY ascending (an error otherwise); an empty list means ONE partition.
//   - Partition(key): binary search -- sort.SearchStrings gives the first boundary >= key; adjust for the "equal goes up" rule.
//   - Partitions() = len(boundaries) + 1.
type RangePartitioner struct{}

func NewRangePartitioner(boundaries []string) (*RangePartitioner, error) {
	return nil, todo.NotImplemented("Ch13: NewRangePartitioner")
}

func (r *RangePartitioner) Partition(key string) int { return -1 }

func (r *RangePartitioner) Partitions() int { return 0 }

// BoundariesFromSample picks boundaries so that each of n partitions receives about the SAME NUMBER of keys, using a random
// SAMPLE of the data (reading everything just to choose boundaries would defeat the purpose). This is "equi-depth"
// partitioning -- the same trick as TeraSort's sampler.
//
//   - Sort a copy of the sample. Boundary j (j = 1 .. n-1) is sorted[j * len(sample) / n].
//   - The boundaries must be strictly ascending: drop a boundary equal to the previous one (heavy duplicates collapse
//     partitions; that is the honest outcome: one key cannot be split by ranges).
//   - len(sample) == 0 or n < 2 -> nil.
func BoundariesFromSample(sample []string, n int) []string {
	return nil // TODO Ch13
}
