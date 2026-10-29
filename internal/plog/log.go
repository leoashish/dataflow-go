package plog

import "dataflow/internal/todo"

// Log is an append-only log split into partitions.
//
// WHY PARTITIONS? One log is one writer and one reader: a bottleneck. Split it into N independent logs and N consumers
// can work in parallel. The price: ORDER is only guaranteed WITHIN a partition. So records that must stay in order
// (all events of one user) must land in the SAME partition -- that is what the KEY is for:
//
//	partition = hash(key) % N      (same key -> same partition, always -> per-key ordering)
//
// Rules:
//   - New(partitions): partitions < 1 -> a real error.
//   - Append(key, value): a non-empty key goes to partition int(hashx.Sum64(key) % uint64(N)). An EMPTY key (nothing to
//     keep together) is spread round-robin: 0, 1, 2, ... N-1, 0, 1, ... Returns the partition and the new record's
//     offset within it. The value is COPIED (the caller may reuse its slice).
//   - Read(partition, from, max): up to max records with Offset >= from, in order. from < 0 is treated as 0; from at or
//     beyond the end gives an empty slice (not an error); an invalid partition -> ErrInvalidPartition.
//   - EndOffset(partition): the offset the NEXT record will get (= how many records the partition holds).
//   - Safe for concurrent use by many goroutines (sync.RWMutex: appends take the write lock, reads the read lock).
type Log struct{}

func New(partitions int) (*Log, error) { return nil, todo.NotImplemented("Ch08: plog.New") }

func (l *Log) Partitions() int { return 0 }

func (l *Log) Append(key string, value []byte) (partition int, offset int64) { return -1, -1 }

func (l *Log) Read(partition int, from int64, max int) ([]Record, error) {
	return nil, todo.NotImplemented("Ch08: Log.Read")
}

func (l *Log) EndOffset(partition int) (int64, error) {
	return 0, todo.NotImplemented("Ch08: Log.EndOffset")
}
