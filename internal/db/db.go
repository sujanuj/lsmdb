// Package db ties the WAL, memtable, and SSTable layers together into a
// single engine with one unified Get/Put/Delete API. This is the layer
// that actually answers "where does this key's current value live" by
// checking, in order: the active memtable (newest data), then every
// flushed SSTable from newest to oldest, stopping at the first layer
// that has ANY entry — live or tombstone — for the key.
package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/sujanuj/lsmdb/internal/compaction"
	"github.com/sujanuj/lsmdb/internal/iterator"
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
	sstables     []*sstable.Reader
	sstablePaths []string // parallel to sstables; needed to delete old files during compaction
	nextSST      int      // next numeric suffix to assign on flush/compaction
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
		dataDir:      dataDir,
		wal:          log,
		memtable:     mt,
		sstables:     readers,
		sstablePaths: sstPaths,
		nextSST:      maxSeq + 1,
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
	db.sstablePaths = append(db.sstablePaths, path)
	db.nextSST++
	db.memtable = memtable.New(0)

	return db.maybeCompactLocked()
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

// maybeCompactLocked checks the size-tiered policy against the current
// set of SSTables and, if a tier has accumulated enough files, performs
// exactly one compaction pass. Only one pass per call (not a loop) is
// deliberate: compaction can itself create a file that's now large
// enough to belong to the NEXT tier up, and checking again immediately
// would mean one flush could cascade into compacting every tier in the
// database synchronously. A real engine runs compaction as a background
// process for exactly this reason; doing one pass per flush call here
// keeps the demo's behavior easy to reason about and is explicitly named
// as a scope cut, not an oversight, in the README.
func (db *DB) maybeCompactLocked() error {
	if len(db.sstables) == 0 {
		return nil
	}

	files := make([]compaction.FileInfo, len(db.sstablePaths))
	for i, p := range db.sstablePaths {
		stat, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("db: compaction: stat %s: %w", p, err)
		}
		files[i] = compaction.FileInfo{SizeBytes: stat.Size(), AgeIndex: i}
	}

	candidates := compaction.PickCompactionCandidates(files)
	if candidates == nil {
		return nil
	}

	return db.compactIndicesLocked(candidates)
}

// compactIndicesLocked merges the SSTables at the given age indices into
// one new SSTable, removes the old files (both from db's in-memory state
// and from disk), and inserts the merged result at the correct position
// in age order. Caller must hold db.mu.
func (db *DB) compactIndicesLocked(indices []int) error {
	sort.Ints(indices)

	dropTombstones := compaction.ShouldDropTombstones(indices)

	generations := make([][]sstable.Entry, len(indices))
	oldPaths := make([]string, len(indices))
	for i, idx := range indices {
		entries, err := db.sstables[idx].All()
		if err != nil {
			return fmt.Errorf("db: compaction: read sstable at index %d: %w", idx, err)
		}
		generations[i] = entries
		oldPaths[i] = db.sstablePaths[idx]
	}

	merged := compaction.Merge(generations, dropTombstones)

	newPath := filepath.Join(db.dataDir, sstableFileName(db.nextSST))
	if len(merged) > 0 {
		if err := sstable.Write(newPath, merged); err != nil {
			return fmt.Errorf("db: compaction: write merged sstable: %w", err)
		}
	}
	// If every input key was a droppable tombstone, merged can be empty
	// — there's nothing to write, and nothing to add back to db.sstables
	// either, which is correct: those keys are now genuinely gone
	// everywhere, on disk and in memory.

	removeSet := make(map[int]bool, len(indices))
	for _, idx := range indices {
		removeSet[idx] = true
	}

	var newSSTables []*sstable.Reader
	var newPaths []string
	for i := range db.sstables {
		if !removeSet[i] {
			newSSTables = append(newSSTables, db.sstables[i])
			newPaths = append(newPaths, db.sstablePaths[i])
		}
	}

	if len(merged) > 0 {
		reader, err := sstable.Open(newPath)
		if err != nil {
			return fmt.Errorf("db: compaction: reopen merged sstable: %w", err)
		}
		// The merged file is newer than everything that went into it,
		// but its actual age relative to files NOT involved in this
		// compaction depends on where those indices sat. Appending to
		// the end is correct as long as compaction only ever operates
		// on a contiguous-from-some-point range that doesn't leave
		// newer untouched files "behind" it in the slice — which holds
		// here because PickCompactionCandidates groups by size, and
		// size tiers in this policy are checked oldest-rolled-up first,
		// so a tier being compacted is always older than any files not
		// yet in a same-or-larger tier. This invariant is worth
		// re-checking carefully if the policy ever changes.
		newSSTables = append(newSSTables, reader)
		newPaths = append(newPaths, newPath)
	}

	db.sstables = newSSTables
	db.sstablePaths = newPaths
	db.nextSST++

	for _, p := range oldPaths {
		if err := os.Remove(p); err != nil {
			// Not fatal: the old file is now logically dead (excluded
			// from db.sstables, so nothing will ever read it again),
			// just wasting disk space until cleaned up manually. Worth
			// surfacing, not worth failing the whole compaction over,
			// since the compaction's correctness-relevant work is
			// already done and committed at this point.
			fmt.Fprintf(os.Stderr, "db: compaction: warning: failed to remove old sstable %s: %v\n", p, err)
		}
	}

	return nil
}

// Compact forces an immediate compaction check, even outside the normal
// post-flush trigger. Mainly useful for tests and demos.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.maybeCompactLocked()
}

// ScanIterator yields key/value pairs within a scan's range, in sorted
// order, lazily — each call to Next() does only the work needed to
// produce that one entry, not the whole range. It wraps an
// iterator.MergeIterator (the same shared merge logic compaction uses)
// with two things the raw merge doesn't do on its own: a key-range
// filter and tombstone-skipping (a scan should never surface a deleted
// key, but it also must never PHYSICALLY drop the tombstone — that
// would be compaction's job under very different safety rules, not a
// read operation's).
type ScanIterator struct {
	inner    *iterator.MergeIterator
	start    []byte
	end      []byte
	hasStart bool
	hasEnd   bool
}

// Next returns the next live key/value pair within the scan's range, or
// ok=false once the range is exhausted. Tombstoned keys and keys outside
// [start, end] are skipped internally — never returned to the caller —
// without that skipping making the rest of the scan incorrect, since the
// underlying merge has already resolved which generation wins each key
// before Next ever sees it.
func (s *ScanIterator) Next() (key, value []byte, ok bool) {
	for {
		entry, ok := s.inner.Next()
		if !ok {
			return nil, nil, false
		}

		if s.hasEnd && bytes.Compare(entry.Key, s.end) > 0 {
			// Past the end of the range. Since the merge always yields
			// keys in ascending order, every subsequent entry will also
			// be past the end — safe to stop the whole scan here rather
			// than just skipping this one entry.
			return nil, nil, false
		}
		if s.hasStart && bytes.Compare(entry.Key, s.start) < 0 {
			continue // before the range; keep pulling
		}
		if entry.Op == sstable.OpDelete {
			continue // tombstoned; a scan must never surface a deleted key
		}
		return entry.Key, entry.Value, true
	}
}

// Scan returns a ScanIterator over all live keys in [start, end]
// (inclusive on both ends). Pass nil for start to mean "from the very
// first key" and nil for end to mean "to the very last key" — e.g.
// Scan(nil, nil) scans the entire keyspace.
//
// The returned iterator holds no lock on db and reflects a SNAPSHOT of
// the memtable and SSTable contents at the moment Scan was called —
// concurrent writes that happen while the caller is still iterating are
// not reflected, intentionally. Building a true live-updating scan would
// require either copy-on-write memtable semantics or holding db's lock
// for the entire iteration (blocking all writes for as long as the scan
// takes) — both real options, neither implemented here; see "what I'd
// change at scale."
func (db *DB) Scan(start, end []byte) *ScanIterator {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Build the generations oldest-first, exactly like flush/compaction
	// do: SSTables in their existing age order, then the memtable LAST,
	// since it's always the newest data regardless of how many SSTables
	// exist. This is the only place outside flushLocked that constructs
	// a multi-generation merge input, and getting this ordering wrong
	// would silently make Scan disagree with Get about which value wins
	// — worth being deliberate about for exactly that reason.
	generations := make([][]sstable.Entry, 0, len(db.sstables)+1)
	for _, r := range db.sstables {
		entries, err := r.All()
		if err != nil {
			// An SSTable read failure here indicates disk corruption or
			// a bug. Scan has no error return in its public API (Next
			// just stops), so the safest behavior is to proceed with an
			// empty generation for this file rather than silently
			// fabricate data — this means affected keys may appear
			// missing from the scan rather than wrong, which is the
			// safer failure direction.
			entries = nil
		}
		generations = append(generations, entries)
	}

	mtEntries := db.memtable.All()
	sstEntries := make([]sstable.Entry, len(mtEntries))
	for i, e := range mtEntries {
		op := sstable.OpPut
		if e.Deleted {
			op = sstable.OpDelete
		}
		sstEntries[i] = sstable.Entry{Key: e.Key, Value: e.Value, Op: op}
	}
	generations = append(generations, sstEntries)

	return &ScanIterator{
		inner:    iterator.NewMergeIterator(generations),
		start:    start,
		end:      end,
		hasStart: start != nil,
		hasEnd:   end != nil,
	}
}
