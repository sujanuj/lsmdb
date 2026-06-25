package sstable

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestBloomFilterSkipsAbsentKeys is the core claim of wiring bloom
// filters into the reader: a Get for a key that was never written
// should be answered by BloomSkips incrementing, meaning the filter
// ruled it out without any chunk read or decompression.
func TestBloomFilterSkipsAbsentKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	const n = 5000
	entries := makeEntries(n)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if r.BloomSkips() != 0 {
		t.Fatalf("BloomSkips() = %d before any Get calls, want 0", r.BloomSkips())
	}

	// Probe with keys from a disjoint namespace — guaranteed absent.
	const probes = 2000
	for i := 0; i < probes; i++ {
		key := []byte(fmt.Sprintf("absent-key-%08d", i))
		if _, found := r.Get(key); found {
			t.Fatalf("Get(%q) unexpectedly found — key was never written", key)
		}
	}

	skips := r.BloomSkips()
	t.Logf("%d/%d absent-key lookups were resolved by the bloom filter alone (no chunk read)", skips, probes)

	// At a 1% target false-positive rate, the overwhelming majority of
	// these probes should be resolved by the filter. Use a generous
	// threshold (80%) rather than the exact target, since this is a
	// behavioral smoke test, not a precise statistical test (that's
	// what the bloom package's own tests are for).
	if float64(skips) < float64(probes)*0.80 {
		t.Errorf("only %d/%d absent lookups were bloom-skipped — filter may not be wired in correctly", skips, probes)
	}
}

// TestBloomFilterDoesNotFalseNegativeRealKeys is the correctness
// counterpart: every key that genuinely IS in the file must always be
// found, regardless of the bloom filter's presence. A bug that
// incorrectly used the filter's "maybe" as "definitely yes" or that
// somehow filtered out real keys would show up here.
func TestBloomFilterDoesNotFalseNegativeRealKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	const n = 5000
	entries := makeEntries(n)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for _, e := range entries {
		got, found := r.Get(e.Key)
		if !found {
			t.Fatalf("Get(%q): not found, but this key was written — possible bloom filter false negative bug", e.Key)
		}
		if string(got) != string(e.Value) {
			t.Errorf("Get(%q) = %q, want %q", e.Key, got, e.Value)
		}
	}
	// BloomSkips should be exactly 0 here — every key in this loop is
	// genuinely present, so the filter must never have ruled one out.
	if r.BloomSkips() != 0 {
		t.Errorf("BloomSkips() = %d after only looking up real keys, want 0", r.BloomSkips())
	}
}

// TestBloomFilterCoversTombstones confirms a deleted key's tombstone is
// still found via the bloom filter path (the filter was built over ALL
// entries, including deletes, specifically so a lookup for a deleted key
// doesn't get incorrectly bloom-skipped and misreported as "never
// existed" instead of "exists, but deleted").
func TestBloomFilterCoversTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := []Entry{
		{Key: []byte("alive"), Value: []byte("yes"), Op: OpPut},
		{Key: []byte("dead"), Value: nil, Op: OpDelete},
	}
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	_, existsHere, isDeleted := r.GetWithTombstone([]byte("dead"))
	if !existsHere {
		t.Fatal("GetWithTombstone(dead): existsHere = false, want true — the bloom filter should not have caused the tombstone to be skipped")
	}
	if !isDeleted {
		t.Error("GetWithTombstone(dead): isDeleted = false, want true")
	}
}
