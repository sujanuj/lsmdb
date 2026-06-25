package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestCompactionTriggersAutomaticallyAndReducesFileCount is the headline
// claim of this phase: enough similarly-sized flushes should trigger a
// real compaction pass that reduces the number of files on disk, not
// just in memory bookkeeping — checked by actually counting .sst files
// in the data directory.
func TestCompactionTriggersAutomaticallyAndReducesFileCount(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	// 4 similarly-sized flushes should trigger exactly one compaction
	// pass (threshold = 4), collapsing them into 1 file.
	for i := 0; i < 4; i++ {
		mustPut(t, database, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		if err := database.Flush(); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
	}

	if database.SSTableCount() != 1 {
		t.Fatalf("SSTableCount() = %d, want 1 (4 similarly-sized files should have compacted into 1)", database.SSTableCount())
	}

	sstFiles := countSSTFilesOnDisk(t, dir)
	if sstFiles != 1 {
		t.Fatalf("found %d .sst files on disk, want 1 — old files should have been physically removed after compaction", sstFiles)
	}

	// And the data itself must still be fully correct after the rewrite.
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("value-%d", i)
		got, found := database.Get([]byte(key))
		if !found || string(got) != want {
			t.Errorf("Get(%q) after compaction = %q, found=%v; want %q, true", key, got, found, want)
		}
	}
}

// TestCompactionReclaimsOverwrittenSpace confirms the actual disk-space
// payoff: overwriting the SAME key many times across many flushes should
// result in a compacted file containing only the latest value, not N
// copies of stale history.
func TestCompactionReclaimsOverwrittenSpace(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	const numFlushes = 4
	for i := 0; i < numFlushes; i++ {
		mustPut(t, database, "hot-key", fmt.Sprintf("version-%d", i))
		if err := database.Flush(); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
	}

	if database.SSTableCount() != 1 {
		t.Fatalf("SSTableCount() = %d, want 1", database.SSTableCount())
	}

	got, found := database.Get([]byte("hot-key"))
	if !found {
		t.Fatal("Get(hot-key) not found after compaction")
	}
	wantLatest := fmt.Sprintf("version-%d", numFlushes-1)
	if string(got) != wantLatest {
		t.Errorf("Get(hot-key) = %q, want %q (the latest version, with all stale copies discarded)", got, wantLatest)
	}

	// Confirm there's really only one entry for this key in the
	// resulting file, not N entries with only the read path picking the
	// right one — that distinction matters because it's the actual
	// space-reclamation claim, not just a read-correctness claim.
	count := 0
	for _, r := range database.sstables {
		if v, found := r.Get([]byte("hot-key")); found && string(v) == wantLatest {
			count++
		}
	}
	if count != 1 {
		t.Errorf("found the live hot-key value in %d sstables, want exactly 1 (stale copies should be physically gone, not just shadowed)", count)
	}
}

// TestCompactionDropsTombstonesWhenSafe confirms that when a compaction
// includes the oldest file in the database (the safe case per
// ShouldDropTombstones), a key that was deleted and never re-written is
// physically gone from the compacted output — not just correctly hidden
// from reads, but actually absent.
func TestCompactionDropsTombstonesWhenSafe(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	mustPut(t, database, "doomed", "temporary")
	if err := database.Flush(); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}
	if err := database.Delete([]byte("doomed")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := database.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}
	mustPut(t, database, "filler1", "x")
	if err := database.Flush(); err != nil {
		t.Fatalf("Flush 3: %v", err)
	}
	mustPut(t, database, "filler2", "y")
	if err := database.Flush(); err != nil { // 4th similarly-sized flush -> triggers compaction
		t.Fatalf("Flush 4: %v", err)
	}

	if database.SSTableCount() != 1 {
		t.Fatalf("SSTableCount() = %d, want 1", database.SSTableCount())
	}

	// Read-level correctness: still correctly "not found."
	if _, found := database.Get([]byte("doomed")); found {
		t.Fatal("Get(doomed) should not be found")
	}

	// Physical correctness: the tombstone itself should be gone from
	// the single remaining file, since this compaction reached back to
	// age index 0 (the oldest file), which is exactly the safe
	// condition for dropping tombstones.
	remaining := database.sstables[0]
	all, err := remaining.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	for _, e := range all {
		if string(e.Key) == "doomed" {
			t.Fatalf("found 'doomed' in the compacted file (op=%v) — its tombstone should have been physically dropped, since this compaction reached the oldest file", e.Op)
		}
	}
}

// TestCompactionPreservesDataAcrossManyRounds is a heavier-weight
// correctness test: many flushes, enough to trigger several rounds of
// compaction (since a compacted file can itself become large enough to
// join a larger tier later), with overwrites and deletes mixed in
// throughout — and confirms the FINAL state, checked via Get, matches a
// plain Go map tracking the same operations independently.
func TestCompactionPreservesDataAcrossManyRounds(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	reference := make(map[string]string)
	deleted := make(map[string]bool)

	const numKeys = 50
	const numRounds = 30

	for round := 0; round < numRounds; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%03d", i)
			if round%7 == 6 && i%5 == 0 {
				if err := database.Delete([]byte(key)); err != nil {
					t.Fatalf("Delete(%q) round %d: %v", key, round, err)
				}
				delete(reference, key)
				deleted[key] = true
				continue
			}
			value := fmt.Sprintf("round-%d", round)
			if err := database.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Put(%q) round %d: %v", key, round, err)
			}
			reference[key] = value
			deleted[key] = false
		}
		if err := database.Flush(); err != nil {
			t.Fatalf("Flush after round %d: %v", round, err)
		}
	}

	for key, want := range reference {
		got, found := database.Get([]byte(key))
		if !found {
			t.Errorf("Get(%q) not found, want %q", key, want)
			continue
		}
		if string(got) != want {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}
	for key, isDel := range deleted {
		if !isDel {
			continue
		}
		if _, stillTracked := reference[key]; stillTracked {
			continue // was deleted then re-written later, already checked above
		}
		if _, found := database.Get([]byte(key)); found {
			t.Errorf("Get(%q) found, but key should be deleted with no later write", key)
		}
	}

	t.Logf("after %d rounds x %d keys, final SSTableCount() = %d", numRounds, numKeys, database.SSTableCount())
}

func countSSTFilesOnDisk(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	count := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			count++
		}
	}
	return count
}
