package partition

import (
	"fmt"
	"sort"

	"dataflow/internal/hashx"
)

type point struct {
	hash uint64
	node string
}

type Ring struct {
	vnodes int
	points []point // sorted by (hash, node)
	nodes  map[string]bool
}

func NewRing(vnodes int) *Ring {
	return &Ring{vnodes: max(vnodes, 1), nodes: map[string]bool{}}
}

func (r *Ring) sortPoints() {
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].node < r.points[j].node
	})
}

func (r *Ring) Add(node string) {
	if r.nodes[node] {
		return
	}
	r.nodes[node] = true
	for i := 0; i < r.vnodes; i++ {
		r.points = append(r.points, point{hash: hashx.Sum64(fmt.Sprintf("%s#%d", node, i)), node: node})
	}
	r.sortPoints()
}

func (r *Ring) Remove(node string) {
	if !r.nodes[node] {
		return
	}
	delete(r.nodes, node)
	kept := r.points[:0]
	for _, p := range r.points {
		if p.node != node {
			kept = append(kept, p)
		}
	}
	r.points = kept
}

func (r *Ring) Get(key string) (string, bool) {
	if len(r.points) == 0 {
		return "", false
	}
	h := hashx.Sum64(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0 // past the last point: wrap around the circle
	}
	return r.points[i].node, true
}

func (r *Ring) Nodes() []string {
	names := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
