package plog

import (
	"fmt"
	"sort"
	"sync"
)

type Group struct {
	mu        sync.Mutex
	log       *Log
	members   map[string]bool
	committed map[int]int64
}

func NewGroup(l *Log) *Group {
	return &Group{log: l, members: map[string]bool{}, committed: map[int]int64{}}
}

func (g *Group) Join(consumer string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.members[consumer] = true
}

func (g *Group) Leave(consumer string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.members, consumer)
}

func (g *Group) Assignment(consumer string) []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.assignment(consumer)
}

// assignment must be called with g.mu held.
func (g *Group) assignment(consumer string) []int {
	if !g.members[consumer] {
		return nil
	}
	names := make([]string, 0, len(g.members))
	for name := range g.members {
		names = append(names, name)
	}
	sort.Strings(names)
	var owned []int
	for p := 0; p < g.log.Partitions(); p++ {
		if names[p%len(names)] == consumer {
			owned = append(owned, p)
		}
	}
	return owned
}

func (g *Group) Poll(consumer string, max int) ([]Record, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.members[consumer] {
		return nil, fmt.Errorf("%w: %q", ErrUnknownConsumer, consumer)
	}
	var out []Record
	for _, p := range g.assignment(consumer) {
		if len(out) >= max {
			break
		}
		recs, err := g.log.Read(p, g.committed[p], max-len(out))
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

func (g *Group) Commit(partition int, next int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if partition < 0 || partition >= g.log.Partitions() {
		return fmt.Errorf("%w: %d", ErrInvalidPartition, partition)
	}
	g.committed[partition] = next
	return nil
}

func (g *Group) Committed(partition int) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.committed[partition]
}
