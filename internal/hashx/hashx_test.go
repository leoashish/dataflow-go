package hashx

import (
	"math/bits"
	"testing"
)

func TestDeterministicAndDistinct(t *testing.T) {
	if Sum64("hello") != Sum64("hello") || Sum64("hello") == Sum64("hellp") {
		t.Fatal("hash must be deterministic and distinguish similar strings")
	}
	if Sum64("x") != Sum64Bytes([]byte("x")) {
		t.Fatal("string and byte versions must agree")
	}
	if Sum64Seed("x", 1) == Sum64Seed("x", 2) {
		t.Fatal("different seeds must give different hashes")
	}
}

func TestAvalanche(t *testing.T) {
	// Hash "key0".."key999" and its neighbour; on average about 32 of 64 bits should differ.
	total, n := 0, 0
	for i := 0; i < 1000; i++ {
		a := Sum64("node#" + string(rune('a'+i%26)) + string(rune('A'+i/26%26)))
		b := Sum64("node#" + string(rune('a'+i%26)) + string(rune('A'+(i/26%26+1)%26)))
		total += bits.OnesCount64(a ^ b)
		n++
	}
	if avg := float64(total) / float64(n); avg < 28 || avg > 36 {
		t.Fatalf("average differing bits = %.1f, want about 32", avg)
	}
}
