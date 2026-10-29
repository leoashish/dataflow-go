// Package agg aggregates big data in parallel: partial results per worker, merged at the end.
package agg

// MapReduceOptions sizes the two phases of MapReduce. Values < 1 mean 1. (Given.)
type MapReduceOptions struct {
	Mappers  int
	Reducers int
}
