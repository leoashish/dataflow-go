// Package partition decides WHERE data lives: which of N workers, shards or files gets a given key.
package partition

import "errors"

// ErrBadPartitionCount is returned when the number of partitions is below 1. GIVEN.
var ErrBadPartitionCount = errors.New("partition: need at least one partition")

// SkewRatio is the load of the busiest partition divided by the average load: 1.0 is perfectly even, 3.0 means the
// busiest partition does three times its fair share (and therefore decides how long the whole job takes). GIVEN.
func SkewRatio(counts []int) float64 {
	if len(counts) == 0 {
		return 0
	}
	total, busiest := 0, 0
	for _, c := range counts {
		total += c
		busiest = max(busiest, c)
	}
	if total == 0 {
		return 0
	}
	return float64(busiest) / (float64(total) / float64(len(counts)))
}
