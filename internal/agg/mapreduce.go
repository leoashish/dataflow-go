package agg

import (
	"context"

	"dataflow/internal/todo"
)

// MapReduce is the model that started the "big data" era -- in one function.
//
//		inputs ──▶ MAP (parallel) ──▶ SHUFFLE (group by key) ──▶ REDUCE (parallel) ──▶ results
//		 doc1       emit(word, 1)        "the" -> [1 1 1 1]          reduce("the", [...]) = 4
//		 doc2       emit(word, 1)        "cat" -> [1 1]
//
//	 1. MAP: `Mappers` goroutines take the inputs between them (each input is mapped exactly once). mapFn calls
//	    emit(key, value) as often as it likes (zero times is fine).
//	 2. SHUFFLE: every key must end up at ONE reducer: reducer = hashx.Sum64(key) % Reducers. To avoid locks, each mapper
//	    writes into its OWN set of per-reducer buckets (e.g. []map[string][]V, one map per reducer); a reducer later reads
//	    its bucket from every mapper.
//	 3. REDUCE: `Reducers` goroutines; reducer r handles the keys that hash to r: for each key it calls
//	    reduceFn(key, allValuesForThatKey) EXACTLY ONCE, with the values gathered from ALL mappers.
//	 4. The result maps each key to its reduceFn result. (Keys are disjoint between reducers, so merging the reducers'
//	    partial result maps is trivial.)
//
// reduceFn must not depend on the ORDER of values (mappers run concurrently, so the order is not fixed).
// Cancelling ctx stops the work and returns ctx.Err(). No inputs -> an empty (non-nil) map.
func MapReduce[I, V, R any](
	ctx context.Context,
	inputs []I,
	mapFn func(in I, emit func(key string, value V)),
	reduceFn func(key string, values []V) R,
	o MapReduceOptions,
) (map[string]R, error) {
	return nil, todo.NotImplemented("Ch11: MapReduce")
}
