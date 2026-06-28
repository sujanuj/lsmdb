package sstable

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestRangeScanFullRangeMatchesAll(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := makeEntries(300) // several chunks (entriesPerChunk=64)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	all, err := r.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	rangeAll, err := r.RangeScan(nil, nil)
	if err != nil {
		t.Fatalf("RangeScan(nil, nil): %v", err)
	}

	if len(all) != len(rangeAll) {
		t.Fatalf("RangeScan(nil,nil) returned %d entries, All() returned %d", len(rangeAll), len(all))
	}
	for i := range all {
		if !bytes.Equal(all[i].Key, rangeAll[i].Key) || !bytes.Equal(all[i].Value, rangeAll[i].Value) {
			t.Errorf("entry %d mismatch: All=%+v RangeScan=%+v", i, all[i], rangeAll[i])
		}
	}
}

func TestRangeScanPartialRangeWithinOneChunk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := makeEntries(300)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Pick a range comfortably within the first chunk (entries 0-63).
	start := entries[10].Key
	end := entries[20].Key
	got, err := r.RangeScan(start, end)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(got) != 11 {
		t.Fatalf("got %d entries, want 11 (indices 10-20 inclusive)", len(got))
	}
	for i, e := range got {
		want := entries[10+i]
		if !bytes.Equal(e.Key, want.Key) {
			t.Errorf("entry %d: key = %q, want %q", i, e.Key, want.Key)
		}
	}
}

func TestRangeScanSpansMultipleChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := makeEntries(300) // chunks: [0-63][64-127][128-191][192-255][256-299]
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Range crossing chunk boundaries: entry 50 (chunk 0) to entry 150 (chunk 2).
	start := entries[50].Key
	end := entries[150].Key
	got, err := r.RangeScan(start, end)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(got) != 101 {
		t.Fatalf("got %d entries, want 101 (indices 50-150 inclusive)", len(got))
	}
	for i, e := range got {
		want := entries[50+i]
		if !bytes.Equal(e.Key, want.Key) {
			t.Errorf("entry %d: key = %q, want %q", i, e.Key, want.Key)
		}
	}
}

func TestRangeScanOnlyStartBound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := makeEntries(200)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got, err := r.RangeScan(entries[150].Key, nil)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(got) != 50 {
		t.Fatalf("got %d entries, want 50 (indices 150-199 inclusive)", len(got))
	}
}

func TestRangeScanOnlyEndBound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := makeEntries(200)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got, err := r.RangeScan(nil, entries[49].Key)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(got) != 50 {
		t.Fatalf("got %d entries, want 50 (indices 0-49 inclusive)", len(got))
	}
}

func TestRangeScanNoMatches(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := makeEntries(100)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Range entirely after the last key.
	got, err := r.RangeScan([]byte("zzz-after-everything"), []byte("zzz-still-after"))
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
}

func TestRangeScanIncludesTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := []Entry{
		{Key: []byte("a"), Value: []byte("1"), Op: OpPut},
		{Key: []byte("b"), Value: nil, Op: OpDelete},
		{Key: []byte("c"), Value: []byte("3"), Op: OpPut},
	}
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	got, err := r.RangeScan(nil, nil)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	foundTombstone := false
	for _, e := range got {
		if string(e.Key) == "b" && e.Op == OpDelete {
			foundTombstone = true
		}
	}
	if !foundTombstone {
		t.Fatal("RangeScan should include the tombstone for 'b' — callers (db.Scan) are responsible for filtering it, not RangeScan itself")
	}
}

// TestRangeScanLargeCrossCheckAgainstAll cross-checks RangeScan against
// a slice of what All() returns, across many different sub-ranges, on a
// dataset large enough to span many chunks.
func TestRangeScanLargeCrossCheckAgainstAll(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	const n = 2000
	entries := makeEntries(n)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	all, err := r.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}

	testRanges := []struct{ startIdx, endIdx int }{
		{0, 10},
		{63, 65}, // straddles the first chunk boundary
		{100, 500},
		{1990, 1999},
		{0, n - 1}, // everything
	}

	for _, tr := range testRanges {
		start := all[tr.startIdx].Key
		end := all[tr.endIdx].Key
		got, err := r.RangeScan(start, end)
		if err != nil {
			t.Fatalf("RangeScan(%d,%d): %v", tr.startIdx, tr.endIdx, err)
		}
		wantLen := tr.endIdx - tr.startIdx + 1
		if len(got) != wantLen {
			t.Fatalf("RangeScan(%d,%d) returned %d entries, want %d", tr.startIdx, tr.endIdx, len(got), wantLen)
		}
		for i, e := range got {
			want := all[tr.startIdx+i]
			if !bytes.Equal(e.Key, want.Key) {
				t.Errorf("range [%d,%d] entry %d: key = %q, want %q", tr.startIdx, tr.endIdx, i, e.Key, want.Key)
			}
		}
	}
}

func TestRangeScanNarrowRangeOnLargeFile(t *testing.T) {
	// Locks in correctness for a narrow range on a file with many
	// chunks — the actual chunk-skipping performance win is measured
	// separately in the benchmark/ package, but this guards against a
	// future change to the chunk-skipping logic breaking correctness
	// for exactly this shape of query.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	const n = 5000 // ~78 chunks at entriesPerChunk=64
	entries := makeEntries(n)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	start := entries[2500].Key
	end := entries[2510].Key
	got, err := r.RangeScan(start, end)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	if len(got) != 11 {
		t.Fatalf("got %d entries, want 11", len(got))
	}
}
