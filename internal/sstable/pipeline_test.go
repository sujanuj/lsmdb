package sstable

import (
	"path/filepath"
	"testing"

	"github.com/sujanuj/lsmdb/internal/memtable"
	"github.com/sujanuj/lsmdb/internal/wal"
)

// toSSTableEntries converts memtable.Entry (the skip list's iteration
// type) to sstable.Entry (the on-disk type). These two packages
// deliberately don't share a type — the memtable's Entry is an
// in-memory iteration convenience, while sstable's Entry is tied
// specifically to the on-disk encoding (it needs an OpType that survives
// serialization). A real Phase 4 db package would own this conversion;
// it lives here for now since this test is what first needs it.
func toSSTableEntries(entries []memtable.Entry) []Entry {
	out := make([]Entry, len(entries))
	for i, e := range entries {
		op := OpPut
		if e.Deleted {
			op = OpDelete
		}
		out[i] = Entry{Key: e.Key, Value: e.Value, Op: op}
	}
	return out
}

// TestFullPipelineWALToMemtableToSSTable is the end-to-end proof that all
// three phases built so far actually compose correctly:
//
//  1. Writes go to the WAL
//  2. The WAL is replayed into a fresh memtable (simulating a restart)
//  3. The memtable is flushed to an SSTable
//  4. The SSTable is opened fresh and read back
//
// and the final state — including an overwrite and a delete — matches
// what was actually intended, with every intermediate layer involved.
func TestFullPipelineWALToMemtableToSSTable(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.wal")
	sstPath := filepath.Join(dir, "test.sst")

	// Step 1: write through the WAL, as a live engine would.
	log, err := wal.Open(walPath, wal.SyncEveryWrite)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	ops := []wal.Record{
		{Op: wal.OpPut, Key: []byte("alice"), Value: []byte("engineer")},
		{Op: wal.OpPut, Key: []byte("bob"), Value: []byte("designer")},
		{Op: wal.OpPut, Key: []byte("alice"), Value: []byte("senior engineer")}, // overwrite
		{Op: wal.OpDelete, Key: []byte("bob")},                                  // delete
		{Op: wal.OpPut, Key: []byte("carol"), Value: []byte("pm")},
	}
	for _, r := range ops {
		if err := log.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Step 2: simulate a restart — rebuild the memtable purely from the
	// WAL on disk, the same as crash recovery would.
	mt := memtable.New(1)
	err = wal.Replay(walPath, func(r wal.Record) error {
		switch r.Op {
		case wal.OpPut:
			mt.Put(r.Key, r.Value)
		case wal.OpDelete:
			mt.Delete(r.Key)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// Step 3: flush the recovered memtable to an SSTable, exactly as the
	// engine would once the memtable crosses its size threshold.
	entries := toSSTableEntries(mt.All())
	if err := Write(sstPath, entries); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Step 4: open the SSTable as a brand new Reader (no shared state
	// with the memtable or WAL at all) and confirm the final, correct
	// state survived the entire pipeline.
	r, err := Open(sstPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if v, found := r.Get([]byte("alice")); !found || string(v) != "senior engineer" {
		t.Errorf("Get(alice) = %q, found=%v; want \"senior engineer\", true", v, found)
	}
	if _, found := r.Get([]byte("bob")); found {
		t.Error("Get(bob) should not be found — bob was deleted")
	}
	if v, found := r.Get([]byte("carol")); !found || string(v) != "pm" {
		t.Errorf("Get(carol) = %q, found=%v; want \"pm\", true", v, found)
	}

	// And confirm bob is specifically a tombstone on disk, not just
	// absent — this is the detail that matters once there's more than
	// one SSTable and an older one might still have a stale "bob" entry.
	_, existsHere, isDeleted := r.GetWithTombstone([]byte("bob"))
	if !existsHere || !isDeleted {
		t.Error("bob should exist on disk as a tombstone, not be silently dropped")
	}
}
