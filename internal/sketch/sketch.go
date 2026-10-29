// Package sketch holds probabilistic data structures: tiny summaries of huge data that answer questions APPROXIMATELY,
// with a known, controllable error. Bloom filter (membership), Count-Min (frequency), HyperLogLog (distinct count),
// reservoir sampling and P-square (quantiles).
package sketch

import "errors"

// ErrShapeMismatch is returned when you try to merge two sketches that were built with different parameters. GIVEN.
var ErrShapeMismatch = errors.New("sketch: cannot merge sketches with different parameters")

// ErrBadParameter is returned (wrapped) by constructors given an invalid parameter. GIVEN.
var ErrBadParameter = errors.New("sketch: invalid parameter")
