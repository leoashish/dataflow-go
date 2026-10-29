// Package hashx is the one hash function the whole course shares. GIVEN -- read it, it is the subject of
// docs/prereq/04-algorithms-foundations.md.
//
// A good hash for data structures must (1) be deterministic (tests depend on it), (2) be fast, and (3) AVALANCHE:
// flipping one input bit should flip about half of the output bits. FNV-1a is fast and simple but its low bits are
// weak for similar keys ("node#1", "node#2"), so we finish it with the splitmix64 mixer, which fixes that.
package hashx

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// Mix is the splitmix64 finaliser: it scrambles all 64 bits so that every output bit depends on every input bit.
func Mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Sum64 hashes a string: FNV-1a followed by Mix.
func Sum64(s string) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return Mix(h)
}

// Sum64Bytes is Sum64 for a byte slice.
func Sum64Bytes(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return Mix(h)
}

// Sum64Seed gives you a whole FAMILY of independent-looking hash functions, one per seed. Sketches need several.
func Sum64Seed(s string, seed uint64) uint64 {
	return Mix(Sum64(s) ^ Mix(seed+0x9e3779b97f4a7c15))
}
