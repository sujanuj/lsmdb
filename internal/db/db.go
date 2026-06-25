// Package db ties the WAL, memtable, and SSTable layers together into a
// single engine with one unified Get/Put/Delete API. This is the layer
// that actually answers "where does this key's current value live" by
// checking, in order: the active memtable (newest data), then every
// flushed SSTable from newest to oldest, stopping at the first layer
// that has ANY entry — live or tombstone — for the key.
package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sujanuj/lsmdb/internal/memtable"
	"github.com/sujanuj/lsmdb/internal/sstable"
	"github.com/sujanuj/lsmdb/internal/wal"
)

// FlushThresholdBytes is the default memtable size (approximate, per
// Memtable.SizeBytes) at which a flush to a new SSTable is triggered.
// Real engines commonly use 4MB-64MB; this is deliberately small so the
// behavior is easy to observe and demo without needing huge datasets.
const FlushThresholdBytes = 1 << 20 // 1 MiB

// DB is the top-level engine. dataDir holds one WAL file and a growing
// set of numbered SSTable files (sstable-000001.sst, sstable-000002.sst,
// ...), with the numeric suffix doubling as age order.
type DB struct {
	mu sync.RWMutex

	dataDir string
	wal     *wal.Log

	memtable *memtable.Memtable

	// sstables is ordered OLDEST first. A flush appends to the end, so
	// the newest SSTable is always sstables[len(sstables)-1] — reads
	// walk this slice backward to check newest-to-oldest, which matches
	// the order flushes naturally produce with zero extra bookkeeping.
	sstables []*sstable.Reader
	nextSST  int // next numeric suffix to assign on flush
}

// Open opens (creating if necessary) a database rooted at dataDir. If a
// WAL file already exists there, its contents are replayed into a fresh
// memtable first — this is the crash-recovery path, exercised identically
// whether the previous process exited cleanly or was killed.
//
// Existing SSTable files in dataDir (named sstable-NNNNNN.sst) are
// discovered and opened in ascending numeric order, which is also their
// age order, so the newest is always the last element of db.sstables.
func Open(dataDir string) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("db: create data dir %s: %w", dataDir, err)
	}

	walPath := filepath.Join(dataDir, "write-ahead.log")

	mt := memtable.New(0)
	err := wal.Replay(walPath, func(r wal.Record) error {
		switch r.Op {
		case wal.OpPut:
			mt.Put(r.Key, r.Value)
		case wal.OpDelete:
			mt.Delete(r.Key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("db: replay WAL: %w", err)
	}

	log, err := wal.Open(walPath, wal.SyncEveryWrite)
	if err != nil {
		return nil, fmt.Errorf("db: open WAL: %w", err)
	}

	sstPaths, maxSeq, err := discoverSSTables(dataDir)
	if err != nil {
		log.Close()
		return nil, err
	}

	var readers []*sstable.Reader
	for _, p := range sstPaths {
		r, err := sstable.Open(p)
		if err != nil {
			log.Close()
			return nil, fmt.Errorf("db: open existing sstable %s: %w", p, err)
		}
		readers = append(readers, r)
	}

	return &DB{
		dataDir:  dataDir,
		wal:      log,
		memtable: mt,
		sstables: readers,
		nextSST:  maxSeq + 1,
	}, nil
}

// Close flushes the WAL's buffered state to disk and releases file
// handles. It does NOT flush the memtable to an SSTable — an unflushed
// memtable is exactly what the WAL exists to make safe; the next Open
// will rebuild it via replay.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.wal.Close()
}

// Put writes key=value, first to the WAL (for durability), then to the
// in-memory memtable. If this push crosses FlushThresholdBytes, the
// memtable is flushed to a new SSTable as part of this call.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Append(wal.Record{Op: wal.OpPut, Key: key, Value: value}); err != nil {
		return fmt.Errorf("db: Put: wal append: %w", err)
	}
	db.memtable.Put(key, value)

	if db.memtable.SizeBytes() >= FlushThresholdBytes {
		return db.flushLocked()
	}
	return nil
}

// Delete tombstones key. Same WAL-then-memtable ordering as Put, for the
// same durability reason.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Append(wal.Record{Op: wal.OpDelete, Key: key}); err != nil {
		return fmt.Errorf("db: Delete: wal append: %w", err)
	}
	db.memtable.Delete(key)

	if db.memtable.SizeBytes() >= FlushThresholdBytes {
		return db.flushLocked()
	}
	return nil
}

// Get is the core multi-level read: check the memtable first (it has the
// newest data), then every SSTable from newest to oldest, stopping at
// the first layer that has ANY entry for key — live value or tombstone.
// A tombstone in a newer layer correctly shadows a real value sitting in
// an older layer, which is exactly why every layer below reports
// existsHere/isDeleted rather than just found/not-found.
func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if value, existsHere, isDeleted := db.memtable.GetWithTombstone(key); existsHere {
		if isDeleted {
			return nil, false
		}
		return value, true
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		value, existsHere, isDeleted := db.sstables[i].GetWithTombstone(key)
		if existsHere {
			if isDeleted {
				return nil, false
			}
			return value, true
		}
	}

	return nil, false
}

// flushLocked writes the current memtable's contents to a new SSTable
// file, appends it as the newest entry in db.sstables, and replaces the
// memtable with a fresh empty one. Caller must hold db.mu.
//
// Note: this does NOT truncate the WAL after a successful flush, even
// though the flushed data is now durably on disk in SSTable form and the
// corresponding WAL entries are technically redundant. Truncating safely
// requires the flush and the truncation to be atomic with respect to a
// crash in between — if the process dies after the SSTable is written
// but before the WAL is truncated, replay would redo a flush that
// already happened, which is harmless (the new memtable would just
// flush again to a new SSTable with the same logical content). If the
// WAL were truncated FIRST and the process died before the SSTable
// write finished, that data would be lost permanently. Getting this
// ordering right with a proper manifest/checkpoint file is exactly the
// kind of thing called out in "what I'd change at scale."
func (db *DB) flushLocked() error {
	entries := db.memtable.All()
	if len(entries) == 0 {
		return nil
	}

	sstEntries := make([]sstable.Entry, len(entries))
	for i, e := range entries {
		op := sstable.OpPut
		if e.Deleted {
			op = sstable.OpDelete
		}
		sstEntries[i] = sstable.Entry{Key: e.Key, Value: e.Value, Op: op}
	}

	path := filepath.Join(db.dataDir, sstableFileName(db.nextSST))
	if err := sstable.Write(path, sstEntries); err != nil {
		return fmt.Errorf("db: flush: write sstable: %w", err)
	}

	reader, err := sstable.Open(path)
	if err != nil {
		return fmt.Errorf("db: flush: reopen freshly written sstable: %w", err)
	}

	db.sstables = append(db.sstables, reader)
	db.nextSST++
	db.memtable = memtable.New(0)

	return nil
}

// Flush forces an immediate flush of the current memtable, even if it
// hasn't crossed FlushThresholdBytes yet. Mainly useful for tests and
// demos where waiting to naturally cross the threshold is impractical.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

// SSTableCount returns how many flushed SSTable files currently exist.
// Exposed for tests/demos that want to show flushing actually happening.
func (db *DB) SSTableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.sstables)
}

func sstableFileName(seq int) string {
	return fmt.Sprintf("sstable-%06d.sst", seq)
}
