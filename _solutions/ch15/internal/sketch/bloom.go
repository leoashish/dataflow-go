package sketch

import (
	"fmt"
	"math"
	"math/bits"

	"dataflow/internal/hashx"
)

type Bloom struct {
	words []uint64
	m     uint64 // number of bits
	k     int    // number of hash functions
}

func NewBloom(expectedItems int, fpRate float64) (*Bloom, error) {
	if expectedItems < 1 {
		return nil, fmt.Errorf("%w: expectedItems must be at least 1, got %d", ErrBadParameter, expectedItems)
	}
	if !(fpRate > 0 && fpRate < 1) {
		return nil, fmt.Errorf("%w: fpRate must be strictly between 0 and 1, got %v", ErrBadParameter, fpRate)
	}
	n := float64(expectedItems)
	m := math.Ceil(-n * math.Log(fpRate) / (math.Ln2 * math.Ln2))
	k := max(int(math.Round(m/n*math.Ln2)), 1)
	return &Bloom{words: make([]uint64, (uint64(m)+63)/64), m: uint64(m), k: k}, nil
}

// indexes yields the k bit positions of s using double hashing: h1 + i*h2.
func (b *Bloom) forEachIndex(s string, fn func(idx uint64) bool) bool {
	h1 := hashx.Sum64(s)
	h2 := hashx.Sum64Seed(s, 1) | 1
	for i := 0; i < b.k; i++ {
		if !fn((h1 + uint64(i)*h2) % b.m) {
			return false
		}
	}
	return true
}

func (b *Bloom) Add(s string) {
	b.forEachIndex(s, func(idx uint64) bool {
		b.words[idx/64] |= 1 << (idx % 64)
		return true
	})
}

func (b *Bloom) Contains(s string) bool {
	return b.forEachIndex(s, func(idx uint64) bool { return b.words[idx/64]&(1<<(idx%64)) != 0 })
}

func (b *Bloom) Bits() int   { return int(b.m) }
func (b *Bloom) Hashes() int { return b.k }

func (b *Bloom) FillRatio() float64 {
	set := 0
	for _, w := range b.words {
		set += bits.OnesCount64(w)
	}
	return float64(set) / float64(b.m)
}

func (b *Bloom) EstimatedFPR() float64 { return math.Pow(b.FillRatio(), float64(b.k)) }

func (b *Bloom) Merge(o *Bloom) error {
	if b.m != o.m || b.k != o.k {
		return fmt.Errorf("%w: (%d bits, %d hashes) vs (%d bits, %d hashes)", ErrShapeMismatch, b.m, b.k, o.m, o.k)
	}
	for i, w := range o.words {
		b.words[i] |= w
	}
	return nil
}
