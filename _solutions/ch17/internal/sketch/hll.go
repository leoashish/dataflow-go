package sketch

import (
	"fmt"
	"math"
	"math/bits"

	"dataflow/internal/hashx"
)

type HLL struct {
	p         uint
	registers []uint8
}

func NewHLL(p int) (*HLL, error) {
	if p < 4 || p > 16 {
		return nil, fmt.Errorf("%w: precision p must be between 4 and 16, got %d", ErrBadParameter, p)
	}
	return &HLL{p: uint(p), registers: make([]uint8, 1<<uint(p))}, nil
}

func (h *HLL) Add(s string) {
	x := hashx.Sum64(s)
	idx := x >> (64 - h.p)                   // the top p bits choose the register
	w := x<<h.p | 1<<(h.p-1)                 // the rest, shifted up; the guard bit keeps it from being all zero
	rho := uint8(bits.LeadingZeros64(w)) + 1 // position of the first 1 bit
	if rho > h.registers[idx] {
		h.registers[idx] = rho
	}
}

func alpha(m float64) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	}
	return 0.7213 / (1 + 1.079/m)
}

func (h *HLL) Count() uint64 {
	m := float64(len(h.registers))
	sum, zeros := 0.0, 0
	for _, r := range h.registers {
		sum += math.Pow(2, -float64(r))
		if r == 0 {
			zeros++
		}
	}
	estimate := alpha(m) * m * m / sum
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros)) // linear counting: nearly exact while most registers are empty
	}
	return uint64(math.Round(estimate))
}

func (h *HLL) Merge(o *HLL) error {
	if h.p != o.p {
		return fmt.Errorf("%w: precision %d vs %d", ErrShapeMismatch, h.p, o.p)
	}
	for i, r := range o.registers {
		if r > h.registers[i] {
			h.registers[i] = r
		}
	}
	return nil
}

func (h *HLL) Precision() int { return int(h.p) }

func (h *HLL) SizeBytes() int { return len(h.registers) }
