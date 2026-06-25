package memtable

// This file implements a small, fast pseudo-random number generator from
// scratch, rather than using math/rand, specifically for deciding how many
// levels a new skip-list node gets promoted to.
//
// Why not math/rand: it would work fine, but the whole point of building
// this engine is to understand the pieces rather than borrow them. The
// level-generation coin flip doesn't need cryptographic randomness or even
// particularly high-quality statistical randomness — it just needs to be
// fast (it runs on every single insert) and reasonably uniform. A xorshift
// generator is the textbook choice for exactly this kind of "fast, good
// enough, not security-sensitive" use case; Go's own runtime uses a
// related xorshift-family generator internally for map iteration order,
// for the same reason.
//
// Algorithm: xorshift64* (Marsaglia, 2003, with Vigna's multiplicative
// finishing step). The state is a single uint64 that must never be zero
// (an all-zero state is a fixed point — xorshift will produce zero
// forever), so New seeds with a fallback if zero is passed in.

type xorshift64 struct {
	state uint64
}

func newXorshift64(seed uint64) *xorshift64 {
	if seed == 0 {
		// A zero seed would make the generator emit zero forever, since
		// 0 XOR-shifted with itself is always 0. Any nonzero constant
		// works as a fallback; this one has no special meaning beyond
		// "not zero."
		seed = 0x9E3779B97F4A7C15
	}
	return &xorshift64{state: seed}
}

// next advances the generator and returns the next pseudo-random uint64.
func (x *xorshift64) next() uint64 {
	s := x.state
	s ^= s << 13
	s ^= s >> 7
	s ^= s << 17
	x.state = s
	// Multiplicative finishing step improves the statistical quality of
	// the output bits (plain xorshift has known weaknesses in the low
	// bits without this).
	return s * 0x2545F4914F6CDD1D
}

// float64 returns a pseudo-random value in [0, 1).
func (x *xorshift64) float64() float64 {
	// Use the top 53 bits, which is the mantissa width of a float64, so
	// every bit of precision in the result is actually random.
	return float64(x.next()>>11) / float64(1<<53)
}
