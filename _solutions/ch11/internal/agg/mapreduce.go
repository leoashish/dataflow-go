package agg

import (
	"context"
	"sync"

	"dataflow/internal/hashx"
)

func MapReduce[I, V, R any](
	ctx context.Context,
	inputs []I,
	mapFn func(in I, emit func(key string, value V)),
	reduceFn func(key string, values []V) R,
	o MapReduceOptions,
) (map[string]R, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mappers, reducers := max(o.Mappers, 1), max(o.Reducers, 1)

	// buckets[m][r] holds what mapper m produced for reducer r: private to mapper m during the map phase.
	buckets := make([][]map[string][]V, mappers)
	var wg sync.WaitGroup
	for m := range mappers {
		buckets[m] = make([]map[string][]V, reducers)
		for r := range reducers {
			buckets[m][r] = map[string][]V{}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			emit := func(key string, value V) {
				r := int(hashx.Sum64(key) % uint64(reducers))
				buckets[m][r][key] = append(buckets[m][r][key], value)
			}
			for i := m; i < len(inputs); i += mappers { // mapper m takes inputs m, m+mappers, ...
				if ctx.Err() != nil {
					return
				}
				mapFn(inputs[i], emit)
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	partials := make([]map[string]R, reducers)
	for r := range reducers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gathered := map[string][]V{}
			for m := range mappers { // the shuffle: collect this reducer's bucket from every mapper
				for key, values := range buckets[m][r] {
					gathered[key] = append(gathered[key], values...)
				}
			}
			out := make(map[string]R, len(gathered))
			for key, values := range gathered {
				if ctx.Err() != nil {
					return
				}
				out[key] = reduceFn(key, values)
			}
			partials[r] = out
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result := map[string]R{}
	for _, p := range partials {
		for k, v := range p {
			result[k] = v // keys are disjoint between reducers
		}
	}
	return result, nil
}
