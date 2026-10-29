package sketch

import (
	"fmt"
	"math/rand"
)

type Reservoir[T any] struct {
	k      int
	rnd    *rand.Rand
	sample []T
	seen   int
}

func NewReservoir[T any](k int, rnd *rand.Rand) (*Reservoir[T], error) {
	if k < 1 {
		return nil, fmt.Errorf("%w: k must be at least 1, got %d", ErrBadParameter, k)
	}
	if rnd == nil {
		return nil, fmt.Errorf("%w: a random source is required", ErrBadParameter)
	}
	return &Reservoir[T]{k: k, rnd: rnd, sample: make([]T, 0, k)}, nil
}

func (r *Reservoir[T]) Add(v T) {
	r.seen++
	if len(r.sample) < r.k {
		r.sample = append(r.sample, v)
		return
	}
	if j := r.rnd.Intn(r.seen); j < r.k { // keep the n-th item with probability k/n
		r.sample[j] = v
	}
}

func (r *Reservoir[T]) Sample() []T { return append([]T(nil), r.sample...) }

func (r *Reservoir[T]) Seen() int { return r.seen }
