package agg

import "math"

type Stat struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64

	mean, m2 float64
}

func (s *Stat) Add(v float64) {
	if s.Count == 0 {
		s.Min, s.Max = v, v
	} else {
		s.Min = math.Min(s.Min, v)
		s.Max = math.Max(s.Max, v)
	}
	s.Count++
	s.Sum += v
	delta := v - s.mean
	s.mean += delta / float64(s.Count)
	s.m2 += delta * (v - s.mean)
}

func (s *Stat) Merge(o Stat) {
	if o.Count == 0 {
		return
	}
	if s.Count == 0 {
		*s = o
		return
	}
	n := float64(s.Count + o.Count)
	delta := o.mean - s.mean
	s.m2 += o.m2 + delta*delta*float64(s.Count)*float64(o.Count)/n
	s.mean += delta * float64(o.Count) / n
	s.Count += o.Count
	s.Sum += o.Sum
	s.Min = math.Min(s.Min, o.Min)
	s.Max = math.Max(s.Max, o.Max)
}

func (s Stat) Mean() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.mean
}

func (s Stat) Variance() float64 {
	if s.Count < 2 {
		return 0
	}
	return s.m2 / float64(s.Count-1)
}

func (s Stat) StdDev() float64 { return math.Sqrt(s.Variance()) }
