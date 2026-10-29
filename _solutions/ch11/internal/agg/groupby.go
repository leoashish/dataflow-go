package agg

import (
	"context"
	"sync"
)

func GroupBy[T any](items []T, key func(T) string, val func(T) float64) map[string]Stat {
	out := map[string]Stat{}
	for _, it := range items {
		k := key(it)
		s := out[k]
		s.Add(val(it))
		out[k] = s
	}
	return out
}

func ParallelGroupBy[T any](ctx context.Context, items []T, workers int, key func(T) string, val func(T) float64) (map[string]Stat, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	workers = max(workers, 1)
	partials := make([]map[string]Stat, workers)

	var wg sync.WaitGroup
	chunk := (len(items) + workers - 1) / workers // ceiling division
	for w := range workers {
		lo, hi := min(w*chunk, len(items)), min((w+1)*chunk, len(items))
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := map[string]Stat{} // private to this worker: no lock needed
			for i := lo; i < hi; i++ {
				if i%1024 == 0 && ctx.Err() != nil {
					return
				}
				k := key(items[i])
				s := local[k]
				s.Add(val(items[i]))
				local[k] = s
			}
			partials[w] = local
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result := map[string]Stat{}
	for _, p := range partials {
		for k, s := range p {
			merged := result[k]
			merged.Merge(s)
			result[k] = merged
		}
	}
	return result, nil
}
