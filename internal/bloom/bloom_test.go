package bloom

import (
	"fmt"
	"testing"
)

// TestNoFalseNegatives is the one guarantee a bloom filter can never
// violate: every key that was Add-ed must report MightContain == true,
// always, no exceptions. This is tested across a large key set because
// a single missed case here would mean the filter is unsound, not just
// imprecise.
func TestNoFalseNegatives(t *testing.T) {
	const n = 50000
	f := New(n, 0.01)

	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		f.Add(keys[i])
	}

	for i, k := range keys {
		if !f.MightContain(k) {
			t.Fatalf("MightContain(%q) [key %d] = false, want true — this is a false negative, which must never happen", k, i)
		}
	}
}

// TestMeasuredFalsePositiveRateNearTarget adds n keys at a target rate,
// then probes with a large set of keys GUARANTEED never to have been
// added, measuring how many incorrectly report MightContain == true.
// The measured rate should land in the right ballpark of the target —
// not exact (it's a probabilistic structure), but a real implementation
// bug (wrong bit count, wrong hash count, broken hashing) would show up
// as an order-of-magnitude difference, which this test would catch.
func TestMeasuredFalsePositiveRateNearTarget(t *testing.T) {
	const n = 10000
	const targetRate = 0.01

	f := New(n, targetRate)
	for i := 0; i < n; i++ {
		f.Add([]byte(fmt.Sprintf("present-%08d", i)))
	}

	const probes = 100000
	falsePositives := 0
	for i := 0; i < probes; i++ {
		// Disjoint key namespace from what was added, so every hit here
		// is unambiguously a false positive, never an accidental real
		// match.
		key := []byte(fmt.Sprintf("absent-%08d", i))
		if f.MightContain(key) {
			falsePositives++
		}
	}

	measuredRate := float64(falsePositives) / float64(probes)
	t.Logf("target FP rate: %.4f, measured FP rate: %.4f (%d/%d), bits=%d, hashes=%d",
		targetRate, measuredRate, falsePositives, probes, f.NumBits(), f.NumHashes())

	// Generous bounds (0.3x to 3x target) — this isn't trying to verify
	// precise bloom filter theory, just that the implementation is in
	// the right neighborhood and not, say, returning true unconditionally
	// or storing nothing at all.
	if measuredRate < targetRate*0.3 || measuredRate > targetRate*3 {
		t.Errorf("measured FP rate %.4f is far from target %.4f — possible implementation bug", measuredRate, targetRate)
	}
}

// TestEmptyFilterRejectsEverything confirms a filter with nothing added
// correctly reports every key as absent.
func TestEmptyFilterRejectsEverything(t *testing.T) {
	f := New(1000, 0.01)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("never-added-%d", i))
		if f.MightContain(key) {
			t.Errorf("MightContain(%q) on empty filter = true, want false", key)
		}
	}
}

// TestSerializationRoundTrip confirms a filter rebuilt from Bytes() +
// NumBits() + NumHashes() behaves identically to the original — this is
// exactly the path an SSTable Open will exercise.
func TestSerializationRoundTrip(t *testing.T) {
	f := New(1000, 0.01)
	added := make([][]byte, 0, 500)
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		f.Add(key)
		added = append(added, key)
	}

	rebuilt := FromBytes(f.Bytes(), f.NumBits(), f.NumHashes())

	for _, key := range added {
		if !rebuilt.MightContain(key) {
			t.Fatalf("rebuilt filter: MightContain(%q) = false, want true (was added before serialization)", key)
		}
	}
}

// TestParameterDerivationProducesReasonableSizes sanity-checks the New()
// math directly: at p=1%, bits-per-key should be close to the textbook
// ~9.6, and numHashes close to ~7 — these are the two numbers worth being
// able to recite, so it's worth a test that would catch a derivation bug.
func TestParameterDerivationProducesReasonableSizes(t *testing.T) {
	const n = 100000
	f := New(n, 0.01)

	bitsPerKey := float64(f.NumBits()) / float64(n)
	if bitsPerKey < 8 || bitsPerKey > 11 {
		t.Errorf("bits per key = %.2f, want roughly 9.6 (textbook value for p=1%%)", bitsPerKey)
	}
	if f.NumHashes() < 5 || f.NumHashes() > 9 {
		t.Errorf("numHashes = %d, want roughly 7 (textbook value for p=1%%)", f.NumHashes())
	}
	t.Logf("for n=%d, p=1%%: bits=%d (%.2f bits/key), hashes=%d", n, f.NumBits(), bitsPerKey, f.NumHashes())
}
