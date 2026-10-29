package sketch

import (
	"fmt"
	"sort"
)

type Counter struct {
	Item  string
	Count uint64
	Err   uint64
}

type SpaceSaving struct {
	k        int
	counters map[string]*Counter
	n        uint64
}

func NewSpaceSaving(k int) (*SpaceSaving, error) {
	if k < 1 {
		return nil, fmt.Errorf("%w: k must be at least 1, got %d", ErrBadParameter, k)
	}
	return &SpaceSaving{k: k, counters: make(map[string]*Counter, k)}, nil
}

func (s *SpaceSaving) Offer(item string) {
	s.n++
	if c, ok := s.counters[item]; ok {
		c.Count++
		return
	}
	if len(s.counters) < s.k {
		s.counters[item] = &Counter{Item: item, Count: 1}
		return
	}
	// full: evict the counter with the smallest count (ties: the largest item name, to be deterministic)
	var victim *Counter
	for _, c := range s.counters {
		if victim == nil || c.Count < victim.Count || (c.Count == victim.Count && c.Item > victim.Item) {
			victim = c
		}
	}
	delete(s.counters, victim.Item)
	s.counters[item] = &Counter{Item: item, Count: victim.Count + 1, Err: victim.Count}
}

func (s *SpaceSaving) N() uint64 { return s.n }

func (s *SpaceSaving) Top() []Counter {
	out := make([]Counter, 0, len(s.counters))
	for _, c := range s.counters {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Item < out[j].Item
	})
	return out
}
