package sketch

import (
	"fmt"
	"math"
	"sort"
)

type P2 struct {
	q       float64
	n       int
	initial []float64
	h       [5]float64 // marker heights
	pos     [5]float64 // marker positions (actual ranks)
	desired [5]float64
	inc     [5]float64
}

func NewP2(q float64) (*P2, error) {
	if !(q > 0 && q < 1) {
		return nil, fmt.Errorf("%w: q must be strictly between 0 and 1, got %v", ErrBadParameter, q)
	}
	return &P2{q: q}, nil
}

func (p *P2) Count() int { return p.n }

func (p *P2) Add(x float64) {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return
	}
	p.n++
	if p.n <= 5 {
		p.initial = append(p.initial, x)
		if p.n == 5 {
			sort.Float64s(p.initial)
			copy(p.h[:], p.initial)
			p.pos = [5]float64{1, 2, 3, 4, 5}
			q := p.q
			p.desired = [5]float64{1, 1 + 2*q, 1 + 4*q, 3 + 2*q, 5}
			p.inc = [5]float64{0, q / 2, q, (1 + q) / 2, 1}
		}
		return
	}

	var k int
	switch {
	case x < p.h[0]:
		p.h[0], k = x, 0
	case x < p.h[1]:
		k = 0
	case x < p.h[2]:
		k = 1
	case x < p.h[3]:
		k = 2
	case x <= p.h[4]:
		k = 3
	default:
		p.h[4], k = x, 3
	}
	for i := k + 1; i < 5; i++ {
		p.pos[i]++
	}
	for i := range p.desired {
		p.desired[i] += p.inc[i]
	}
	for i := 1; i <= 3; i++ {
		d := p.desired[i] - p.pos[i]
		if (d >= 1 && p.pos[i+1]-p.pos[i] > 1) || (d <= -1 && p.pos[i-1]-p.pos[i] < -1) {
			sign := 1.0
			if d < 0 {
				sign = -1
			}
			if candidate := p.parabolic(i, sign); p.h[i-1] < candidate && candidate < p.h[i+1] {
				p.h[i] = candidate
			} else {
				p.h[i] = p.linear(i, sign)
			}
			p.pos[i] += sign
		}
	}
}

func (p *P2) parabolic(i int, d float64) float64 {
	h, n := p.h, p.pos
	return h[i] + d/(n[i+1]-n[i-1])*((n[i]-n[i-1]+d)*(h[i+1]-h[i])/(n[i+1]-n[i])+(n[i+1]-n[i]-d)*(h[i]-h[i-1])/(n[i]-n[i-1]))
}

func (p *P2) linear(i int, d float64) float64 {
	j := i + int(d)
	return p.h[i] + d*(p.h[j]-p.h[i])/(p.pos[j]-p.pos[i])
}

func (p *P2) Value() float64 {
	switch {
	case p.n == 0:
		return math.NaN()
	case p.n < 5:
		sorted := append([]float64(nil), p.initial...)
		sort.Float64s(sorted)
		idx := int(math.Ceil(p.q*float64(len(sorted)))) - 1
		return sorted[max(idx, 0)]
	}
	return p.h[2]
}
