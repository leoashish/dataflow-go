package partition

import (
	"fmt"

	"dataflow/internal/hashx"
)

func Hash(key string, n int) (int, error) {
	if n < 1 {
		return 0, fmt.Errorf("%w: got %d", ErrBadPartitionCount, n)
	}
	return int(hashx.Sum64(key) % uint64(n)), nil
}
