package sketch

import (
	"math/rand"

	"dataflow/internal/todo"
)

// Reservoir keeps a UNIFORM RANDOM SAMPLE of k items from a stream whose length you do not know in advance and cannot store:
// every item seen so far has exactly the same chance, k/n, of being in the sample. (Debugging a pipeline? Keep 1 000 random
// records from the 4 billion that flowed through, not the first 1 000.)
//
// ALGORITHM R (Vitter, 1985) -- the whole trick:
//
//   - The first k items go straight into the reservoir.
//   - The n-th item (n > k) is kept with probability k/n: draw j = a random integer in [0, n); if j < k, it REPLACES slot j.
//
// WHY IT IS UNIFORM (induction): suppose after n-1 items each has probability k/(n-1) of being in. The new item enters with
// probability k/n. An old item survives iff it was in AND is not the one replaced: k/(n-1) * (1 - (k/n)(1/k)) = k/(n-1) * (n-1)/n = k/n.
// Everyone is at k/n again. The test runs 20 000 trials and checks every position's inclusion frequency.
//
//   - NewReservoir(k, rnd): k < 1, or a nil rnd, -> an error wrapping ErrBadParameter. (The caller supplies the random
//     source so that runs are REPRODUCIBLE in tests: determinism is a feature. Use rnd.Intn(n).)
//   - Add(v) offers one item. Seen() = how many items were offered.
//   - Sample() returns a COPY of the current sample (length min(k, Seen())), in no particular order guaranteed.
type Reservoir[T any] struct{}

func NewReservoir[T any](k int, rnd *rand.Rand) (*Reservoir[T], error) {
	return nil, todo.NotImplemented("Ch18: NewReservoir")
}

func (r *Reservoir[T]) Add(v T) {} // TODO Ch18

func (r *Reservoir[T]) Sample() []T { return nil } // TODO Ch18

func (r *Reservoir[T]) Seen() int { return 0 } // TODO Ch18
