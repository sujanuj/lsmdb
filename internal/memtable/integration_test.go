package memtable

import (
	"path/filepath"
	"testing"

	"github.com/sujanuj/lsmdb/internal/wal"
)

// TestRebuildFromWALReplay is the actual integration point between the
// two phases built so far: the memtable's Put/Delete methods serve
// directly as the apply callback for wal.Replay. This proves that a
// memtable rebuilt purely from replaying a WAL ends up identical to one
// built by calling Put/Delete directly — which is the whole point of the
// WAL existing in the first place.
func TestRebuildFromWALReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	log, err := wal.Open(path, wal.SyncEveryWrite)
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}

	ops := []wal.Record{
		{Op: wal.OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: wal.OpPut, Key: []byte("b"), Value: []byte("2")},
		{Op: wal.OpPut, Key: []byte("a"), Value: []byte("1-updated")},
		{Op: wal.OpDelete, Key: []byte("b")},
		{Op: wal.OpPut, Key: []byte("c"), Value: []byte("3")},
	}
	for _, r := range ops {
		if err := log.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Rebuild a fresh memtable purely from WAL replay — this is exactly
	// what would happen on process restart after a crash.
	mt := New(1)
	err = wal.Replay(path, func(r wal.Record) error {
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

	// Expected final state: a=1-updated (overwritten), b=deleted, c=3.
	if v, found := mt.Get([]byte("a")); !found || string(v) != "1-updated" {
		t.Errorf("Get(a) = %q, found=%v; want \"1-updated\", true", v, found)
	}
	if _, found := mt.Get([]byte("b")); found {
		t.Error("Get(b) should not be found — it was deleted after being set")
	}
	if v, found := mt.Get([]byte("c")); !found || string(v) != "3" {
		t.Errorf("Get(c) = %q, found=%v; want \"3\", true", v, found)
	}
	if mt.Len() != 2 {
		t.Errorf("Len() = %d, want 2 (a and c are live; b is tombstoned)", mt.Len())
	}

	// Explicitly confirm the tombstone for b survived replay correctly
	// (as opposed to b simply never existing).
	_, existsHere, isDeleted := mt.GetWithTombstone([]byte("b"))
	if !existsHere || !isDeleted {
		t.Error("b should exist as a tombstone after replay, not be absent entirely")
	}
}
