package sketch

import (
	"fmt"
	"math"

	"dataflow/internal/hashx"
)

type CountMin struct {
	width, depth int
	cells        []uint64 // depth rows of width counters, row-major
	total        uint64
}

func NewCountMin(epsilon, delta float64) (*CountMin, error) {
	if !(epsilon > 0 && epsilon < 1) || !(delta > 0 && delta < 1) {
		return nil, fmt.Errorf("%w: epsilon and delta must be strictly between 0 and 1 (got %v, %v)", ErrBadParameter, epsilon, delta)
	}
	width := int(math.Ceil(math.E / epsilon))
	depth := int(math.Ceil(math.Log(1 / delta)))
	return &CountMin{width: width, depth: depth, cells: make([]uint64, width*depth)}, nil
}

func (c *CountMin) column(key string, row int) int {
	return int(hashx.Sum64Seed(key, uint64(row)) % uint64(c.width))
}

func (c *CountMin) Add(key string, n uint64) {
	for row := 0; row < c.depth; row++ {
		c.cells[row*c.width+c.column(key, row)] += n
	}
	c.total += n
}

func (c *CountMin) Estimate(key string) uint64 {
	est := uint64(math.MaxUint64)
	for row := 0; row < c.depth; row++ {
		est = min(est, c.cells[row*c.width+c.column(key, row)])
	}
	return est
}

func (c *CountMin) Total() uint64 { return c.total }
func (c *CountMin) Width() int    { return c.width }
func (c *CountMin) Depth() int    { return c.depth }

func (c *CountMin) Merge(o *CountMin) error {
	if c.width != o.width || c.depth != o.depth {
		return fmt.Errorf("%w: %dx%d vs %dx%d", ErrShapeMismatch, c.depth, c.width, o.depth, o.width)
	}
	for i, v := range o.cells {
		c.cells[i] += v
	}
	c.total += o.total
	return nil
}
