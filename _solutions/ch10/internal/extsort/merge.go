package extsort

import "container/heap"

type head[T any] struct {
	value T
	src   int
}

type mergeHeap[T any] struct {
	items []head[T]
	less  func(a, b T) bool
}

func (h *mergeHeap[T]) Len() int { return len(h.items) }
func (h *mergeHeap[T]) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.less(a.value, b.value) {
		return true
	}
	if h.less(b.value, a.value) {
		return false
	}
	return a.src < b.src // equal values: the lower-numbered sequence first (this is what makes the merge stable)
}
func (h *mergeHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap[T]) Push(x any)    { h.items = append(h.items, x.(head[T])) }
func (h *mergeHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[:n-1]
	return x
}

func KWayMerge[T any](iters []func() (T, bool), less func(a, b T) bool, emit func(T) error) error {
	h := &mergeHeap[T]{less: less}
	for i, next := range iters {
		if next == nil {
			continue
		}
		if v, ok := next(); ok {
			h.items = append(h.items, head[T]{value: v, src: i})
		}
	}
	heap.Init(h)

	for h.Len() > 0 {
		smallest := h.items[0]
		if err := emit(smallest.value); err != nil {
			return err
		}
		if v, ok := iters[smallest.src](); ok {
			h.items[0] = head[T]{value: v, src: smallest.src} // replace the root and sift it down: cheaper than Pop + Push
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}
	}
	return nil
}
