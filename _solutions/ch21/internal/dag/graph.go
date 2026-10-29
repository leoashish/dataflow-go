package dag

import (
	"fmt"
	"sort"
	"strings"
)

type Graph struct {
	tasks      map[string]Task
	names      []string            // sorted
	dependents map[string][]string // task -> tasks that depend on it (sorted)
}

func New(tasks []Task) (*Graph, error) {
	g := &Graph{tasks: map[string]Task{}, dependents: map[string][]string{}}
	for _, t := range tasks {
		switch {
		case t.Name == "":
			return nil, fmt.Errorf("%w: empty name", ErrInvalidTask)
		case t.Run == nil:
			return nil, fmt.Errorf("%w: task %q has no Run function", ErrInvalidTask, t.Name)
		}
		if _, dup := g.tasks[t.Name]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateTask, t.Name)
		}
		g.tasks[t.Name] = t
		g.names = append(g.names, t.Name)
	}
	sort.Strings(g.names)
	for _, name := range g.names {
		for _, dep := range g.tasks[name].Deps {
			if _, ok := g.tasks[dep]; !ok {
				return nil, fmt.Errorf("%w: task %q depends on %q", ErrUnknownDependency, name, dep)
			}
			g.dependents[dep] = append(g.dependents[dep], name)
		}
	}
	for _, list := range g.dependents {
		sort.Strings(list)
	}
	if _, rest := g.kahn(); len(rest) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrCycle, g.findCycle(rest))
	}
	return g, nil
}

// kahn returns the deterministic topological order and the tasks that could not be ordered (they sit on or behind a cycle).
func (g *Graph) kahn() (order []string, rest map[string]bool) {
	remaining := map[string]int{}
	var ready []string
	for _, name := range g.names {
		unique := map[string]bool{}
		for _, d := range g.tasks[name].Deps {
			unique[d] = true
		}
		remaining[name] = len(unique)
		if len(unique) == 0 {
			ready = append(ready, name)
		}
	}
	for len(ready) > 0 {
		sort.Strings(ready)
		name := ready[0]
		ready = ready[1:]
		order = append(order, name)
		done := map[string]bool{}
		for _, dep := range g.dependents[name] {
			if done[dep] {
				continue
			}
			done[dep] = true
			if remaining[dep]--; remaining[dep] == 0 {
				ready = append(ready, dep)
			}
		}
	}
	rest = map[string]bool{}
	for _, name := range g.names {
		if remaining[name] > 0 {
			rest[name] = true
		}
	}
	return order, rest
}

func (g *Graph) findCycle(rest map[string]bool) string {
	var start string
	for _, name := range g.names { // alphabetically first leftover task
		if rest[name] {
			start = name
			break
		}
	}
	path := []string{}
	index := map[string]int{}
	cur := start
	for {
		if i, seen := index[cur]; seen {
			return strings.Join(append(path[i:], cur), " -> ")
		}
		index[cur] = len(path)
		path = append(path, cur)
		deps := append([]string(nil), g.tasks[cur].Deps...)
		sort.Strings(deps)
		for _, d := range deps {
			if rest[d] {
				cur = d
				break
			}
		}
	}
}

func (g *Graph) Order() []string {
	order, _ := g.kahn()
	return order
}

func (g *Graph) Levels() [][]string {
	level := map[string]int{}
	var levels [][]string
	for _, name := range g.Order() { // dependencies always come first
		l := 0
		for _, d := range g.tasks[name].Deps {
			l = max(l, level[d]+1)
		}
		level[name] = l
		for len(levels) <= l {
			levels = append(levels, nil)
		}
		levels[l] = append(levels[l], name)
	}
	for _, names := range levels {
		sort.Strings(names)
	}
	return levels
}
