package agg

import (
	"math"
)

// Stat is a MERGEABLE running summary of a stream of numbers: count, sum, min, max, mean and variance in O(1) memory.
// The zero value is an empty, ready-to-use Stat.
//
// WHY "MERGEABLE" IS THE WHOLE GAME: to aggregate in parallel each worker summarises its own slice of the data, and then
// the partial summaries are combined. That only works if combining two summaries gives EXACTLY what one pass over all
// the data would have given. Count, sum, min and max merge trivially. Mean and variance need care:
//
//	NAIVE variance:  E[x^2] - E[x]^2   -> catastrophic cancellation (subtracting two huge, nearly equal numbers).
//	                 For values near 1e9 it can return garbage or even a NEGATIVE variance.
//	WELFORD'S online algorithm (numerically stable), keeping mean and M2 = sum of squared deviations from the mean:
//	    Add(x):   count++;  delta = x - mean;  mean += delta / count;  M2 += delta * (x - mean)
//	MERGE of two summaries A and B (Chan et al.):
//	    n = nA + nB;  delta = meanB - meanA
//	    mean = meanA + delta * nB / n
//	    M2   = M2A + M2B + delta^2 * nA * nB / n
//	SAMPLE variance = M2 / (count - 1) (0 when count < 2).
type Stat struct {
	Count int64
	Sum   float64
	Min   float64 // only meaningful when Count > 0
	Max   float64 // only meaningful when Count > 0

	mean, m2 float64 // Welford state
}

// Add folds one value into the summary. The first value sets Min and Max.
func (s *Stat) Add(v float64) {} // TODO Ch11

// Merge folds another summary into this one (see the formulas above). Merging an empty Stat changes nothing; merging
// into an empty Stat copies the other one. Must not modify o.
func (s *Stat) Merge(o Stat) {} // TODO Ch11

// Mean is Sum/Count (use the Welford mean), 0 for an empty Stat.
func (s Stat) Mean() float64 { return 0 } // TODO Ch11

// Variance is the sample variance, 0 when Count < 2.
func (s Stat) Variance() float64 { return 0 } // TODO Ch11

// StdDev is the square root of Variance. GIVEN.
func (s Stat) StdDev() float64 { return math.Sqrt(s.Variance()) }
