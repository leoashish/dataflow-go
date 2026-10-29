package plog

import (
	"fmt"
	"sync"

	"dataflow/internal/hashx"
)

type Log struct {
	mu         sync.RWMutex
	partitions [][]Record
	roundRobin int
}

func New(partitions int) (*Log, error) {
	if partitions < 1 {
		return nil, fmt.Errorf("plog: need at least one partition, got %d", partitions)
	}
	return &Log{partitions: make([][]Record, partitions)}, nil
}

func (l *Log) Partitions() int { return len(l.partitions) }

func (l *Log) Append(key string, value []byte) (int, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var p int
	if key == "" {
		p = l.roundRobin % len(l.partitions)
		l.roundRobin++
	} else {
		p = int(hashx.Sum64(key) % uint64(len(l.partitions)))
	}
	offset := int64(len(l.partitions[p]))
	l.partitions[p] = append(l.partitions[p], Record{Partition: p, Offset: offset, Key: key, Value: append([]byte(nil), value...)})
	return p, offset
}

func (l *Log) valid(partition int) bool { return partition >= 0 && partition < len(l.partitions) }

func (l *Log) Read(partition int, from int64, max int) ([]Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.valid(partition) {
		return nil, fmt.Errorf("%w: %d", ErrInvalidPartition, partition)
	}
	recs := l.partitions[partition]
	from = max64(from, 0)
	if from >= int64(len(recs)) || max <= 0 {
		return nil, nil
	}
	end := min(int64(len(recs)), from+int64(max))
	return append([]Record(nil), recs[from:end]...), nil
}

func (l *Log) EndOffset(partition int) (int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.valid(partition) {
		return 0, fmt.Errorf("%w: %d", ErrInvalidPartition, partition)
	}
	return int64(len(l.partitions[partition])), nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
