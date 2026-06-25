// Package bloom implements a bloom filter: a small, fixed-size
// probabilistic structure that can answer "is this key definitely NOT
// present?" with zero false negatives. If the filter says "maybe
// present," the key might or might not actually be there (a false
// positive) and the caller must check the real data to be sure. If the
// filter says "definitely not present," that's guaranteed correct, and
// the caller can skip the expensive real check entirely.
//
// This is the win for an SSTable reader: a Get for a key with no
// matching filter bit can skip decompressing any chunk in that file at
// all — the most common case is exactly this, a true negative, on any
// workload with a meaningful miss rate.
package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// Filter is a fixed-size bit array plus the parameters needed to hash a
// key into k bit positions.
type Filter struct {
	bits      []byte // bits[i/8] bit (i%8) is the i-th bit
	numBits   uint64
	numHashes int
}

// New creates an empty filter sized for expectedKeys items at the given
// target false-positive rate (e.g. 0.01 for 1%).
//
// Derivation (the math worth being able to recite): for n expected keys
// and a target false-positive probability p, the optimal bit array size
// is m = -n*ln(p) / (ln(2))^2, and the optimal number of hash functions
// is k = (m/n)*ln(2). At p=1%, this works out to roughly 9.6 bits per
// key and k≈7 — both derived here rather than hardcoded, so the filter
// adapts correctly if expectedKeys or falsePositiveRate change.
func New(expectedKeys int, falsePositiveRate float64) *Filter {
	if expectedKeys < 1 {
		expectedKeys = 1
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01
	}

	n := float64(expectedKeys)
	ln2 := math.Ln2

	m := math.Ceil(-n * math.Log(falsePositiveRate) / (ln2 * ln2))
	if m < 8 {
		m = 8
	}
	k := int(math.Round((m / n) * ln2))
	if k < 1 {
		k = 1
	}

	numBits := uint64(m)
	numBytes := (numBits + 7) / 8

	return &Filter{
		bits:      make([]byte, numBytes),
		numBits:   numBits,
		numHashes: k,
	}
}

// hashPair computes two independent-enough base hashes of key, which are
// then combined to simulate numHashes distinct hash functions via double
// hashing: position_i = (h1 + i*h2) mod numBits. This is the standard
// technique (Kirsch & Mitzenmacher, 2006) for getting k hash functions'
// worth of bit-independence from just two real hash computations, rather
// than needing k genuinely different hash algorithms.
func hashPair(key []byte) (h1, h2 uint64) {
	hasher1 := fnv.New64a()
	hasher1.Write(key)
	h1 = hasher1.Sum64()

	// A different seed/salt fed through the same hash family produces a
	// second, sufficiently independent value without needing a second
	// hash algorithm implementation.
	hasher2 := fnv.New64a()
	var salt [8]byte
	binary.LittleEndian.PutUint64(salt[:], 0x9E3779B97F4A7C15)
	hasher2.Write(salt[:])
	hasher2.Write(key)
	h2 = hasher2.Sum64()

	return h1, h2
}

func (f *Filter) bitPositions(key []byte) []uint64 {
	h1, h2 := hashPair(key)
	positions := make([]uint64, f.numHashes)
	for i := 0; i < f.numHashes; i++ {
		combined := h1 + uint64(i)*h2
		positions[i] = combined % f.numBits
	}
	return positions
}

// Add records key in the filter.
func (f *Filter) Add(key []byte) {
	for _, pos := range f.bitPositions(key) {
		f.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MightContain returns false only if key is DEFINITELY not in the
// filter (i.e. it was never Add-ed) — this is the guarantee callers rely
// on to safely skip real work. It returns true both for keys that were
// genuinely added AND for false positives; callers must treat true as
// "go check the real data," never as confirmation by itself.
func (f *Filter) MightContain(key []byte) bool {
	for _, pos := range f.bitPositions(key) {
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Bytes returns the filter's raw bit array, for serialization into an
// SSTable file.
func (f *Filter) Bytes() []byte {
	return f.bits
}

// NumHashes returns the number of hash functions (k) this filter uses —
// needed alongside the raw bytes to deserialize a filter correctly,
// since bitPositions depends on it.
func (f *Filter) NumHashes() int {
	return f.numHashes
}

// NumBits returns the size of the bit array.
func (f *Filter) NumBits() uint64 {
	return f.numBits
}

// FromBytes reconstructs a Filter from previously serialized bytes plus
// its parameters. Used when opening an SSTable that already has a
// filter written to disk.
func FromBytes(bits []byte, numBits uint64, numHashes int) *Filter {
	return &Filter{bits: bits, numBits: numBits, numHashes: numHashes}
}
