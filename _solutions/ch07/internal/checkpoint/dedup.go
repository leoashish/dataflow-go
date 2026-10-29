package checkpoint

import (
	"container/list"
	"sync"
)

type Deduper struct {
	mu       sync.Mutex
	capacity int
	order    *list.List               // front = most recently seen
	items    map[string]*list.Element // id -> its element
}

func NewDeduper(capacity int) *Deduper {
	return &Deduper{capacity: max(capacity, 1), order: list.New(), items: map[string]*list.Element{}}
}

func (d *Deduper) Seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.items[id]; ok {
		d.order.MoveToFront(el)
		return true
	}
	d.items[id] = d.order.PushFront(id)
	if d.order.Len() > d.capacity {
		oldest := d.order.Back()
		d.order.Remove(oldest)
		delete(d.items, oldest.Value.(string))
	}
	return false
}

func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.order.Len()
}
