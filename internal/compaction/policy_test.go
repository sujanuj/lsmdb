package compaction

import (
	"reflect"
	"sort"
	"testing"
)

func TestPickCompactionCandidatesNoneBelowThreshold(t *testing.T) {
	files := []FileInfo{
		{SizeBytes: 100, AgeIndex: 0},
		{SizeBytes: 110, AgeIndex: 1},
		{SizeBytes: 95, AgeIndex: 2},
	}
	got := PickCompactionCandidates(files)
	if got != nil {
		t.Errorf("got %v, want nil (only 3 similarly-sized files, threshold is %d)", got, SizeTierThreshold)
	}
}

func TestPickCompactionCandidatesTriggersAtThreshold(t *testing.T) {
	files := []FileInfo{
		{SizeBytes: 100, AgeIndex: 0},
		{SizeBytes: 105, AgeIndex: 1},
		{SizeBytes: 98, AgeIndex: 2},
		{SizeBytes: 102, AgeIndex: 3},
	}
	got := PickCompactionCandidates(files)
	if got == nil {
		t.Fatal("got nil, want a candidate set — 4 similarly-sized files should trigger at threshold=4")
	}
	sort.Ints(got)
	want := []int{0, 1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPickCompactionCandidatesSeparatesDifferentTiers(t *testing.T) {
	// 4 small files (should trigger) + 2 large files (should NOT, below
	// threshold for their own tier) — the policy must only return the
	// small tier's indices, not mix tiers together.
	files := []FileInfo{
		{SizeBytes: 100, AgeIndex: 0},
		{SizeBytes: 105, AgeIndex: 1},
		{SizeBytes: 98, AgeIndex: 2},
		{SizeBytes: 102, AgeIndex: 3},
		{SizeBytes: 10000, AgeIndex: 4},
		{SizeBytes: 10500, AgeIndex: 5},
	}
	got := PickCompactionCandidates(files)
	if got == nil {
		t.Fatal("got nil, want the small-file tier to trigger")
	}
	sort.Ints(got)
	want := []int{0, 1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v (should only include the small tier, not the large files)", got, want)
	}
}

func TestPickCompactionCandidatesRatioBoundary(t *testing.T) {
	// A file more than 2x the tier's minimum should NOT join that tier.
	files := []FileInfo{
		{SizeBytes: 100, AgeIndex: 0},
		{SizeBytes: 100, AgeIndex: 1},
		{SizeBytes: 100, AgeIndex: 2},
		{SizeBytes: 201, AgeIndex: 3}, // > 2x of 100, should start its own tier
	}
	got := PickCompactionCandidates(files)
	if got != nil {
		t.Errorf("got %v, want nil — only 3 files in the size-100 tier, the 4th is a different tier", got)
	}
}

func TestPickCompactionCandidatesEmptyInput(t *testing.T) {
	got := PickCompactionCandidates(nil)
	if got != nil {
		t.Errorf("got %v, want nil for empty input", got)
	}
}

func TestShouldDropTombstonesTrueWhenReachesOldest(t *testing.T) {
	if !ShouldDropTombstones([]int{0, 1, 2, 3}) {
		t.Error("ShouldDropTombstones should be true when the candidate set includes age index 0 (the oldest file)")
	}
}

func TestShouldDropTombstonesFalseWhenNotReachingOldest(t *testing.T) {
	if ShouldDropTombstones([]int{2, 3, 4, 5}) {
		t.Error("ShouldDropTombstones should be false when there's an older file (index 0,1) NOT included in this compaction — dropping tombstones here could resurrect data from those older files")
	}
}

func TestShouldDropTombstonesEmptyInput(t *testing.T) {
	if ShouldDropTombstones(nil) {
		t.Error("ShouldDropTombstones(nil) should be false")
	}
}
