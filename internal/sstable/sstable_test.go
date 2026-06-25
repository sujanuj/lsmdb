package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func makeEntries(n int) []Entry {
	entries := make([]Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = Entry{
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: []byte(fmt.Sprintf("value-%06d", i)),
			Op:    OpPut,
		}
	}
	return entries
}

func TestWriteOpenRoundTripSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Fewer entries than one chunk — exercises the boundary case of a
	// single, partially-filled final chunk.
	entries := makeEntries(10)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if r.NumEntries() != 10 {
		t.Errorf("NumEntries() = %d, want 10", r.NumEntries())
	}

	for _, e := range entries {
		got, found := r.Get(e.Key)
		if !found {
			t.Fatalf("Get(%q): not found", e.Key)
		}
		if !bytes.Equal(got, e.Value) {
			t.Errorf("Get(%q) = %q, want %q", e.Key, got, e.Value)
		}
	}

	if _, found := r.Get([]byte("nonexistent")); found {
		t.Error("Get(nonexistent) should not be found")
	}
}

func TestWriteOpenRoundTripMultiChunk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Several full chunks plus a partial trailing chunk:
	// entriesPerChunk=64, so 200 entries = 3 full chunks + 1 partial (8).
	const n = 200
	entries := makeEntries(n)
	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	wantChunks := (n + entriesPerChunk - 1) / entriesPerChunk
	if len(r.index) != wantChunks {
		t.Errorf("index has %d entries, want %d chunks", len(r.index), wantChunks)
	}

	// Spot-check across chunk boundaries specifically — these are the
	// indices most likely to expose an off-by-one in chunking logic.
	checkIndices := []int{0, 1, 63, 64, 65, 127, 128, n - 1}
	for _, i := range checkIndices {
		got, found := r.Get(entries[i].Key)
		if !found {
			t.Fatalf("Get(%q) [entry %d]: not found", entries[i].Key, i)
		}
		if !bytes.Equal(got, entries[i].Value) {
			t.Errorf("Get(%q) [entry %d] = %q, want %q", entries[i].Key, i, got, entries[i].Value)
		}
	}
}

func TestWriteOpenAllEntriesMatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	entries := makeEntries(150)
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
	if len(all) != len(entries) {
		t.Fatalf("All() returned %d entries, want %d", len(all), len(entries))
	}
	for i := range entries {
		if !bytes.Equal(all[i].Key, entries[i].Key) || !bytes.Equal(all[i].Value, entries[i].Value) {
			t.Errorf("entry %d: got %+v, want %+v", i, all[i], entries[i])
		}
	}

	// All() must return entries in sorted order — this is the property
	// compaction's k-way merge will depend on later.
	for i := 1; i < len(all); i++ {
		if bytes.Compare(all[i-1].Key, all[i].Key) >= 0 {
			t.Fatalf("All() not sorted at index %d: %q >= %q", i, all[i-1].Key, all[i].Key)
		}
	}
}

func TestTombstoneSurvivesWriteAndRead(t *testing.T) {
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

	if _, found := r.Get([]byte("b")); found {
		t.Error("Get(b) should report not found — it's a tombstone")
	}

	_, existsHere, isDeleted := r.GetWithTombstone([]byte("b"))
	if !existsHere {
		t.Error("GetWithTombstone(b): existsHere should be true — the tombstone is a real on-disk entry")
	}
	if !isDeleted {
		t.Error("GetWithTombstone(b): isDeleted should be true")
	}

	// And a key that was genuinely never written should be reported
	// differently from a tombstone.
	_, existsHere2, _ := r.GetWithTombstone([]byte("never-written"))
	if existsHere2 {
		t.Error("GetWithTombstone(never-written): existsHere should be false")
	}
}

func TestOpenRejectsInvalidMagicNumber(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	if err := Write(path, makeEntries(5)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Corrupt just the magic number bytes at the very end of the file.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open should fail on a file with a corrupted magic number")
	}
}

func TestOpenRejectsTooSmallFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tiny.sst")
	if err := os.WriteFile(path, []byte("not a real sstable"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := Open(path)
	if err == nil {
		t.Fatal("Open should fail on a file too small to contain a footer")
	}
}

// TestCompressionActuallyShrinksRepetitiveData verifies the gzip step is
// real and effective, not a no-op — using highly repetitive values, which
// is the kind of data compression should do well on.
func TestCompressionActuallyShrinksRepetitiveData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	const n = 500
	entries := make([]Entry, n)
	repetitiveValue := bytes.Repeat([]byte("AAAAAAAAAA"), 50) // 500 bytes, highly compressible
	uncompressedSize := 0
	for i := 0; i < n; i++ {
		entries[i] = Entry{
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: repetitiveValue,
			Op:    OpPut,
		}
		uncompressedSize += len(entries[i].Key) + len(entries[i].Value)
	}

	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if stat.Size() >= int64(uncompressedSize) {
		t.Errorf("file size %d bytes is not smaller than raw payload %d bytes — compression doesn't seem to be working", stat.Size(), uncompressedSize)
	}
	t.Logf("uncompressed payload: %d bytes, on-disk file: %d bytes (%.1f%% of original)",
		uncompressedSize, stat.Size(), 100*float64(stat.Size())/float64(uncompressedSize))
}

// TestLargeRandomKeysSortedWrite cross-checks Write+Open against a large
// sorted key set, similar in spirit to the memtable's large random
// workload test.
func TestLargeRandomKeysSortedWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	const n = 5000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("k%05d", i)
	}
	sort.Strings(keys) // Write requires sorted input; this is the caller's job

	entries := make([]Entry, n)
	for i, k := range keys {
		entries[i] = Entry{Key: []byte(k), Value: []byte("v" + k), Op: OpPut}
	}

	if err := Write(path, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < n; i += 137 { // sample every 137th key rather than all 5000, for test speed
		got, found := r.Get(entries[i].Key)
		if !found || !bytes.Equal(got, entries[i].Value) {
			t.Fatalf("Get(%q) = %q, found=%v; want %q, true", entries[i].Key, got, found, entries[i].Value)
		}
	}
}
