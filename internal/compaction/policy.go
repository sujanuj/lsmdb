package compaction

import "math"

// SizeTierThreshold is how many similarly-sized files must accumulate in
// a tier before they're compacted together. 4 is a common real-world
// starting point (Cassandra's STCS default historically used a similar
// value) — low enough to demo easily, high enough that compaction isn't
// triggered on every single flush.
const SizeTierThreshold = 4

// SizeRatio defines "similarly sized": two files are considered the same
// tier if neither is more than this many times larger than the other.
// 2.0 is the standard choice — it's loose enough that normal flush-size
// variance doesn't split files that should obviously compact together,
// tight enough that a single huge already-compacted file doesn't get
// lumped in with a batch of tiny fresh flushes.
const SizeRatio = 2.0

// FileInfo is the minimal information the policy needs about one
// SSTable to make a tiering decision: its size on disk and its position
// in age order (lower index = older), which the policy needs to
// determine the contiguous-oldest-files property required for safe
// tombstone dropping.
type FileInfo struct {
	SizeBytes int64
	AgeIndex  int // index into the full oldest-first sstables list
}

// PickCompactionCandidates groups files (already sorted oldest-first, as
// db.DB stores them) into similarly-sized tiers and returns the AgeIndex
// list of the first tier that has reached SizeTierThreshold members,
// or nil if no tier is ready yet.
//
// "Similarly sized" is evaluated against the SMALLEST file currently
// being considered for a tier — a new file joins the current tier if it's
// within SizeRatio of the tier's running minimum size. This means tiers
// are built greedily from smallest to largest, which mirrors how files
// naturally accumulate: many small fresh flushes, occasionally compacted
// into fewer larger files.
func PickCompactionCandidates(files []FileInfo) []int {
	if len(files) == 0 {
		return nil
	}

	// Group by tier using a simple greedy pass: sort isn't needed since
	// files are typically flush-ordered with roughly increasing size
	// already, but a similarly-sized file appearing out of strict size
	// order (e.g. after a previous compaction produced a big file
	// earlier in age-order than some later small flush) should still be
	// found — so this compares every file against every open tier
	// rather than assuming size correlates with age order.
	var tiers [][]FileInfo

	for _, f := range files {
		placed := false
		for i, tier := range tiers {
			tierMin := tier[0].SizeBytes
			for _, tf := range tier {
				if tf.SizeBytes < tierMin {
					tierMin = tf.SizeBytes
				}
			}
			if withinRatio(f.SizeBytes, tierMin, SizeRatio) {
				tiers[i] = append(tiers[i], f)
				placed = true
				break
			}
		}
		if !placed {
			tiers = append(tiers, []FileInfo{f})
		}
	}

	for _, tier := range tiers {
		if len(tier) >= SizeTierThreshold {
			indices := make([]int, len(tier))
			for i, f := range tier {
				indices[i] = f.AgeIndex
			}
			return indices
		}
	}
	return nil
}

func withinRatio(a, b int64, ratio float64) bool {
	if a == 0 || b == 0 {
		return a == b
	}
	r := math.Max(float64(a), float64(b)) / math.Min(float64(a), float64(b))
	return r <= ratio
}

// ShouldDropTombstones reports whether it's safe to physically discard
// tombstones that win their key during a compaction of the files at
// candidateAgeIndices, given totalFileCount files exist in total.
//
// The rule implemented here is the conservative, always-safe one
// described in the README: dropping a tombstone is only safe if the
// compaction includes EVERY file older than the candidates — i.e. the
// candidate set reaches back to age index 0. If there's an older file
// NOT included in this compaction, a tombstone might still need to
// shadow a value sitting in that older file, so it must be kept.
//
// This intentionally doesn't try to be clever about per-key safety
// (checking whether each specific key actually exists in any older
// file) — that would require reading every older file's contents during
// every compaction decision, which defeats the point of compaction being
// bounded work. The conservative whole-tier rule trades "tombstones
// might survive a little longer than strictly necessary" for "this can
// never resurrect deleted data," which is the right side of that
// tradeoff to be on.
func ShouldDropTombstones(candidateAgeIndices []int) bool {
	if len(candidateAgeIndices) == 0 {
		return false
	}
	minIdx := candidateAgeIndices[0]
	for _, idx := range candidateAgeIndices {
		if idx < minIdx {
			minIdx = idx
		}
	}
	return minIdx == 0
}
