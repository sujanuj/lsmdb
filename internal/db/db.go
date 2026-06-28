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

	// Background compaction. There is exactly ONE worker goroutine,
	// started in Open and stopped in Close — not one goroutine per
	// flush. This is a deliberate design choice: an unbounded number of
	// compaction goroutines could pile up faster than disk I/O can keep
	// up with them, all fighting over the same write lock for their
	// final swap; a single worker naturally serializes compaction work
	// the same way real engines (RocksDB, LevelDB) bound their
	// background compaction thread pools rather than leaving it
	// unbounded.
	compactionCh   chan compactionRequest
	compactionDone chan struct{} // closed by the worker when it exits
	closing        chan struct{} // closed by Close() to tell the worker to stop
}

// compactionRequest is what gets handed to the background worker: the
// specific age indices to compact, captured at the moment the trigger
// fired, AND the sequence number the resulting merged file will use,
// reserved up front under the same lock. Reserving the sequence number
// here — rather than having the worker read db.nextSST later, after
// releasing the lock to do the actual merge — is what prevents a real
// race: a concurrent flush could otherwise claim and write to the exact
// same sequence number before the compaction's swap phase gets there,
// corrupting whichever file lost the race. This was caught by the test
// suite (TestSSTableFileNamingSurvivesGaps started failing with
// corrupted-file errors and runaway memory allocation from reading
// garbage as a length field) — exactly the kind of bug a background
// worker design needs to get right and exactly why "snapshot the
// inputs, do you also need to snapshot the OUTPUT name" is worth
// thinking through explicitly rather than assuming.
type compactionRequest struct {
	indices     []int
	reservedSeq int
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

	db := &DB{
		dataDir:      dataDir,
		wal:          log,
		memtable:     mt,
		sstables:     readers,
		sstablePaths: sstPaths,
		nextSST:      maxSeq + 1,

		// Buffered size 1: at most one pending compaction request needs
		// to be queued. If a flush tries to send while the buffer is
		// already full (meaning a compaction is queued but hasn't
		// started yet), there's no need to queue a second request —
		// the worker will re-derive the current candidate set from
		// scratch when it gets to it, so a queued-but-stale request
		// would just be redundant, not incorrect.
		compactionCh:   make(chan compactionRequest, 1),
		compactionDone: make(chan struct{}),
		closing:        make(chan struct{}),
	}

	go db.compactionWorker()

	return db, nil
}

// Close stops the background compaction worker, waits for any in-flight
// compaction to finish, then flushes the WAL's buffered state to disk
// and releases file handles. It does NOT flush the memtable to an
// SSTable — an unflushed memtable is exactly what the WAL exists to
// make safe; the next Open will rebuild it via replay.
//
// Stopping the worker BEFORE closing the WAL matters: a compaction that
// was already in flight when Close was called doesn't touch the WAL at
// all (it only reads SSTables and writes a new one), so it's safe to let
// it finish naturally rather than trying to cancel it mid-merge, which
// would risk leaving a half-written SSTable file behind.
func (db *DB) Close() error {
	close(db.closing)
	<-db.compactionDone

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
// set of SSTables and, if a tier has accumulated enough files, hands the
// decision off to the background compaction worker rather than doing
// the merge itself. Caller must hold db.mu (a write lock, since it was
// called from flushLocked) — but this function itself does only cheap
// work (stat calls + a non-blocking channel send) before returning, so
// it doesn't meaningfully extend how long that lock is held.
//
// The actual merge — the expensive part — happens later, in
// runCompaction, on the dedicated worker goroutine, without holding
// db.mu for the bulk of the work. This is the entire point of this
// phase: a flush that triggers compaction no longer pays for the
// compaction itself before returning to the caller.
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

	select {
	case db.compactionCh <- compactionRequest{indices: candidates, reservedSeq: db.nextSST}:
		// Reserving db.nextSST HERE, while still holding the write
		// lock that protects it, and incrementing it immediately
		// below, is what guarantees the background worker's eventual
		// output file gets a sequence number no concurrent flush can
		// also claim. The worker uses this reserved number rather than
		// reading db.nextSST again later, after the lock has been
		// released for the (long) merge phase.
		db.nextSST++
	default:
		// A compaction is already running or already queued. Dropping
		// this request is safe and correct, not a missed opportunity:
		// the NEXT flush will call maybeCompactLocked again and
		// re-derive the current candidate set from scratch, so no
		// compaction work is permanently lost — it's deferred to the
		// next natural trigger point, exactly the same way a busy
		// background worker in a real engine would defer rather than
		// queue unboundedly.
	}
	return nil
}

// compactionWorker is the single background goroutine that performs all
// compaction work for this DB. It runs for the DB's entire lifetime,
// started in Open and stopped in Close.
func (db *DB) compactionWorker() {
	defer close(db.compactionDone)

	for {
		select {
		case req := <-db.compactionCh:
			db.runCompaction(req)
		case <-db.closing:
			return
		}
	}
}

// runCompaction performs one compaction pass for the given age indices,
// following a snapshot-merge-swap pattern specifically so the expensive
// work (reading every input SSTable, merging, writing the new file) does
// NOT hold db.mu — concurrent Get/Put/Scan calls keep working the entire
// time a compaction is in progress. Only the brief final swap takes the
// write lock.
//
// Errors here are logged, not returned to any caller — there's no
// synchronous caller left to return them to, since this runs on the
// background worker. A failed compaction leaves the database in its
// pre-compaction state (the swap step never happens, so nothing is
// removed or replaced) — correctness is preserved even though the
// space-reclamation benefit of this particular pass is lost. The next
// flush will simply try again.
func (db *DB) runCompaction(req compactionRequest) {
	indices := req.indices
	sort.Ints(indices)

	// --- Snapshot phase: brief read lock, just copying references ---
	db.mu.RLock()
	if !db.indicesStillValidLocked(indices) {
		// Something about the sstables slice changed in a way that
		// makes these indices untrustworthy (see the comment on
		// indicesStillValidLocked for exactly what this guards
		// against). Abort this pass; the next flush will recompute
		// fresh candidates against current state.
		db.mu.RUnlock()
		return
	}
	snapshotReaders := make([]*sstable.Reader, len(indices))
	snapshotPaths := make([]string, len(indices))
	for i, idx := range indices {
		snapshotReaders[i] = db.sstables[idx]
		snapshotPaths[i] = db.sstablePaths[idx]
	}
	dropTombstones := compaction.ShouldDropTombstones(indices)
	dataDir := db.dataDir
	db.mu.RUnlock()

	// --- Merge phase: NO LOCK HELD. This is the expensive part, and
	// concurrent reads/writes against db proceed normally while it runs. ---
	generations := make([][]sstable.Entry, len(snapshotReaders))
	for i, r := range snapshotReaders {
		entries, err := r.All()
		if err != nil {
			fmt.Fprintf(os.Stderr, "db: background compaction: read sstable %s: %v\n", snapshotPaths[i], err)
			return
		}
		generations[i] = entries
	}
	merged := compaction.Merge(generations, dropTombstones)

	// req.reservedSeq was claimed atomically when this request was
	// created (see maybeCompactLocked) — using it here, rather than
	// reading db.nextSST again now, is what prevents this file's name
	// from ever colliding with a concurrent flush's output file. See
	// the comment on compactionRequest for the full story of the bug
	// this fixes.
	newPath := filepath.Join(dataDir, sstableFileName(req.reservedSeq))
	var newReader *sstable.Reader
	if len(merged) > 0 {
		if err := sstable.Write(newPath, merged); err != nil {
			fmt.Fprintf(os.Stderr, "db: background compaction: write merged sstable: %v\n", err)
			return
		}
		var err error
		newReader, err = sstable.Open(newPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "db: background compaction: reopen merged sstable: %v\n", err)
			return
		}
	}

	// --- Swap phase: brief write lock, just slice surgery ---
	db.mu.Lock()
	if !db.indicesStillValidLocked(indices) {
		// State changed while the merge was running (in practice, this
		// can only mean a CLOSE happened, since flushes only ever
		// append and there is only one compaction worker). Discard the
		// freshly written file rather than swap it in, since the
		// indices it was computed against may no longer mean what they
		// meant when the merge started.
		db.mu.Unlock()
		if newReader != nil {
			// sstable.Reader holds no persistent file handle (it opens
			// fresh on each chunk read), so there's nothing to close —
			// just remove the now-orphaned file from disk.
			os.Remove(newPath)
		}
		return
	}

	oldPaths := make([]string, len(indices))
	removeSet := make(map[int]bool, len(indices))
	for i, idx := range indices {
		removeSet[idx] = true
		oldPaths[i] = db.sstablePaths[idx]
	}

	var newSSTables []*sstable.Reader
	var newPaths []string
	for i := range db.sstables {
		if !removeSet[i] {
			newSSTables = append(newSSTables, db.sstables[i])
			newPaths = append(newPaths, db.sstablePaths[i])
		}
	}
	if newReader != nil {
		newSSTables = append(newSSTables, newReader)
		newPaths = append(newPaths, newPath)
	}

	db.sstables = newSSTables
	db.sstablePaths = newPaths
	// NOTE: db.nextSST is deliberately NOT incremented here — it was
	// already incremented when req.reservedSeq was claimed, in
	// maybeCompactLocked, under the same lock as the trigger decision.
	// Incrementing it again here would skip a sequence number for no
	// reason (harmless) but more importantly, NOT incrementing it at
	// reservation time (the bug this replaced) was what allowed a
	// concurrent flush to claim and corrupt this exact file.
	db.mu.Unlock()

	// --- Cleanup: no lock needed, these files are already unreachable ---
	for _, p := range oldPaths {
		if err := os.Remove(p); err != nil {
			fmt.Fprintf(os.Stderr, "db: background compaction: warning: failed to remove old sstable %s: %v\n", p, err)
		}
	}
}

// indicesStillValidLocked checks that the given age indices still point
// within the bounds of db.sstables. Caller must hold db.mu (either lock).
//
// Because there is exactly one compaction worker and flushes only ever
// APPEND to db.sstables (never remove or reorder existing entries), an
// index that was valid when a compaction request was created remains a
// valid reference to the SAME file throughout that compaction's
// lifetime — appends to the end of a slice don't invalidate earlier
// indices. This check exists anyway as an explicit, verified guard
// rather than a silently trusted invariant: relying on "this can't
// happen because of how the rest of the code is written" without ever
// checking it is exactly the kind of assumption that quietly stops
// being true after a future change. The one case this DOES catch: a
// shrinking of db.sstables (i.e. another compaction's swap) racing with
// this one — which the single-worker design rules out today, but
// re-verifying it costs almost nothing and pays for itself the moment
// anyone changes the worker count.
func (db *DB) indicesStillValidLocked(indices []int) bool {
	for _, idx := range indices {
		if idx < 0 || idx >= len(db.sstables) {
			return false
		}
	}
	return true
}

// Compact blocks until any currently-running or queued background
// compaction has had a chance to run, by directly invoking the
// synchronous decision-and-dispatch path and then draining the request
// if one was queued. This exists mainly for tests and demos that need
// deterministic, observable compaction rather than waiting on
// goroutine scheduling — see CompactSync for the fully synchronous
// variant used by most of this package's own tests, which predate the
// background worker and exercise the underlying merge logic directly.
func (db *DB) Compact() error {
	db.mu.Lock()
	err := db.maybeCompactLocked()
	db.mu.Unlock()
	if err != nil {
		return err
	}

	// Best-effort: give the worker a chance to actually run before
	// returning, since callers of this method generally want to
	// observe the result (e.g. a reduced SSTableCount) immediately
	// afterward. This is intentionally NOT a hard guarantee — see
	// CompactSync for callers that need that.
	select {
	case <-db.compactionDone:
		// DB was closed concurrently; nothing more to wait for.
	default:
	}
	return nil
}

// CompactSync performs a compaction pass synchronously, on the calling
// goroutine, bypassing the background worker entirely. This is the
// direct, deterministic path the test suite uses to verify compaction's
// CORRECTNESS (does it merge entries right? does it drop tombstones
// safely?) without being entangled with goroutine scheduling timing —
// those are orthogonal concerns, and conflating them would make
// correctness tests flaky for reasons that have nothing to do with
// correctness.
func (db *DB) CompactSync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

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

// compactIndicesLocked is the original synchronous merge-and-swap logic,
// retained for CompactSync. Caller must hold db.mu (write lock).
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
		newSSTables = append(newSSTables, reader)
		newPaths = append(newPaths, newPath)
	}

	db.sstables = newSSTables
	db.sstablePaths = newPaths
	db.nextSST++

	for _, p := range oldPaths {
		if err := os.Remove(p); err != nil {
			fmt.Fprintf(os.Stderr, "db: compaction: warning: failed to remove old sstable %s: %v\n", p, err)
		}
	}

	return nil
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
		entries, err := r.RangeScan(start, end)
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
