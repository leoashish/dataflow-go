package partition

import (
	"fmt"
	"sort"
)

type RangePartitioner struct {
	bounds []string
}

func NewRangePartitioner(boundaries []string) (*RangePartitioner, error) {
	for i := 1; i < len(boundaries); i++ {
		if boundaries[i-1] >= boundaries[i] {
			return nil, fmt.Errorf("partition: boundaries must be strictly ascending (%q then %q)", boundaries[i-1], boundaries[i])
		}
	}
	return &RangePartitioner{bounds: append([]string(nil), boundaries...)}, nil
}

func (r *RangePartitioner) Partition(key string) int {
	i := sort.SearchStrings(r.bounds, key) // first boundary >= key
	if i < len(r.bounds) && r.bounds[i] == key {
		i++ // a key equal to a boundary belongs to the UPPER partition
	}
	return i
}

func (r *RangePartitioner) Partitions() int { return len(r.bounds) + 1 }

func BoundariesFromSample(sample []string, n int) []string {
	if len(sample) == 0 || n < 2 {
		return nil
	}
	sorted := append([]string(nil), sample...)
	sort.Strings(sorted)
	var bounds []string
	for j := 1; j < n; j++ {
		b := sorted[j*len(sorted)/n]
		if len(bounds) == 0 || b > bounds[len(bounds)-1] { // strictly ascending: collapse duplicates
			bounds = append(bounds, b)
		}
	}
	return bounds
}
