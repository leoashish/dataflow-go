// Package plog is a miniature partitioned, append-only log with consumer groups -- the core idea of Kafka.
package plog

import "errors"

var (
	ErrInvalidPartition = errors.New("plog: invalid partition")
	ErrUnknownConsumer  = errors.New("plog: consumer is not a member of the group")
)

// Record is one entry in a partition. Within a partition offsets are 0, 1, 2, ... with no gaps.
type Record struct {
	Partition int
	Offset    int64
	Key       string
	Value     []byte
}
