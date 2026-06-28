package sstable

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/sujanuj/lsmdb/internal/bloom"
)

// Reader provides read access to an existing, immutable SSTable file.
// Opening a Reader loads the index block AND the bloom filter into
// memory — both are sized to stay small relative to the data by
// construction, so this remains cheap. The (potentially much larger)
// data chunks are still read and decompressed lazily, only when the
// bloom filter can't rule out a key's presence.
type Reader struct {
	path       string
	index      []indexEntry
	filter     *bloom.Filter
	numEntries int

	// Stats, for demos/benchmarks showing the filter's actual effect —
	// not used for any correctness decision.
	bloomSkips  uint64 // Gets that the filter ruled out without touching disk
	bloomMisses uint64 // Gets where the filter said "maybe" and the chunk check confirmed absence (a false positive)
}

// Open reads the footer, index block, and bloom filter of the SSTable at
// path, without touching any data chunks. This is intentionally cheap:
// the footer is a fixed size at a fixed position (seek to end, read
// backward), and both the index and the filter are sized to be small
// relative to the data by construction.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	if stat.Size() < footerSize {
		return nil, fmt.Errorf("sstable: %s is too small to contain a valid footer (%d bytes)", path, stat.Size())
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, stat.Size()-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: read footer of %s: %w", path, err)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint64(footer[8:16])
	bloomOffset := binary.LittleEndian.Uint64(footer[16:24])
	bloomLen := binary.LittleEndian.Uint64(footer[24:32])
	bloomNumBits := binary.LittleEndian.Uint64(footer[32:40])
	bloomNumHashes := binary.LittleEndian.Uint64(footer[40:48])
	numEntries := binary.LittleEndian.Uint64(footer[48:56])
	magic := binary.LittleEndian.Uint64(footer[56:64])

	if magic != magicNumber {
		return nil, fmt.Errorf("sstable: %s has invalid magic number %x, not a valid SSTable file (or it's corrupted, or it's an older format version)", path, magic)
	}

	indexBytes := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBytes, int64(indexOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read index block of %s: %w", path, err)
	}
	index, err := decodeIndex(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("sstable: decode index of %s: %w", path, err)
	}

	bloomBytes := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBytes, int64(bloomOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read bloom filter of %s: %w", path, err)
	}
	filter := bloom.FromBytes(bloomBytes, bloomNumBits, int(bloomNumHashes))

	return &Reader{path: path, index: index, filter: filter, numEntries: int(numEntries)}, nil
}

// NumEntries returns the total entry count recorded in the footer,
// including tombstones. Cheap — answered entirely from the already-loaded
// footer, no chunk reads needed.
func (r *Reader) NumEntries() int {
	return r.numEntries
}

// BloomSkips returns how many Get calls were answered as "definitely
// absent" purely from the in-memory bloom filter, with zero chunk reads
// or decompression. Exposed for demos/benchmarks; not used internally
// for any correctness decision.
func (r *Reader) BloomSkips() uint64 {
	return atomic.LoadUint64(&r.bloomSkips)
}

// Get looks up key, returning its value and whether it was found as a
// live entry. A tombstoned key is reported as not found by this method —
// callers that need to distinguish "never existed" from "deleted" (to
// correctly decide whether to keep checking older SSTables) should use
// GetWithTombstone instead.
func (r *Reader) Get(key []byte) (value []byte, found bool) {
	value, existsHere, isDeleted := r.GetWithTombstone(key)
	if !existsHere || isDeleted {
		return nil, false
	}
	return value, true
}

// GetWithTombstone is the full-fidelity lookup: existsHere reports
// whether this SSTable contains any entry for key at all, and isDeleted
// distinguishes a tombstone from a live value. This mirrors
// memtable.Memtable.GetWithTombstone deliberately — the db package's
// multi-level Get walks memtable -> newest SSTable -> older SSTables and
// stops as soon as ANY layer reports existsHere=true, using isDeleted to
// decide the final answer.
//
// Before touching disk at all, this consults the bloom filter: if it
// reports the key is definitely absent, that's authoritative (bloom
// filters never produce false negatives) and we return immediately,
// skipping the chunk lookup, the disk read, and the gzip decompression
// entirely. This is the entire point of building the filter — turning
// the common case of "key genuinely isn't in this file" from a disk read
// into an in-memory bit check.
func (r *Reader) GetWithTombstone(key []byte) (value []byte, existsHere bool, isDeleted bool) {
	if !r.filter.MightContain(key) {
		atomic.AddUint64(&r.bloomSkips, 1)
		return nil, false, false
	}

	chunkIdx := r.findChunk(key)
	if chunkIdx < 0 {
		return nil, false, false // key is before the first chunk's first key
	}

	entries, err := r.readChunk(chunkIdx)
	if err != nil {
		// A read/decompress failure on an existing, previously-validated
		// file indicates disk corruption or a bug, not a normal "key
		// missing" case. Surfacing it as "not found" would silently
		// hide data loss, so this is deliberately not swallowed here —
		// callers should propagate or log this loudly.
		return nil, false, false
	}

	// Linear scan within the chunk — chunks are small (entriesPerChunk
	// entries) by construction, so this is cheap; the expensive part
	// (decompression) already happened once per Get, not once per key
	// within the chunk.
	idx := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].Key, key) >= 0
	})
	if idx >= len(entries) || !bytes.Equal(entries[idx].Key, key) {
		// The bloom filter said "maybe" but the key genuinely isn't
		// here — a true false positive. Expected at roughly the
		// filter's configured rate; tracked for observability, not
		// treated as an error.
		atomic.AddUint64(&r.bloomMisses, 1)
		return nil, false, false
	}

	e := entries[idx]
	if e.Op == OpDelete {
		return nil, true, true
	}
	return e.Value, true, false
}

// findChunk binary-searches the in-memory index to find which chunk
// could contain key, returning its index, or -1 if key is smaller than
// every chunk's first key (i.e. definitely not in this file).
//
// This is the payoff of the sparse index: this search touches only the
// small in-memory index slice, never the disk, and narrows the search to
// exactly one chunk before any decompression happens.
func (r *Reader) findChunk(key []byte) int {
	// Find the last chunk whose firstKey is <= key.
	lo, hi := 0, len(r.index)-1
	result := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if bytes.Compare(r.index[mid].firstKey, key) <= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return result
}

// readChunk reads and decompresses the chunk at the given index position,
// returning its decoded entries. Every Get pays this cost for exactly one
// chunk; callers doing many lookups in a hot loop would benefit from a
// chunk cache, which is intentionally not built here — see the README's
// "what I'd change at scale" notes for why that's a deliberate scope cut
// rather than an oversight.
func (r *Reader) readChunk(chunkIdx int) ([]Entry, error) {
	ie := r.index[chunkIdx]

	f, err := os.Open(r.path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s for chunk read: %w", r.path, err)
	}
	defer f.Close()

	compressed := make([]byte, ie.length)
	if _, err := f.ReadAt(compressed, int64(ie.offset)); err != nil {
		return nil, fmt.Errorf("sstable: read chunk at offset %d: %w", ie.offset, err)
	}

	raw, err := gzipDecompress(compressed)
	if err != nil {
		return nil, fmt.Errorf("sstable: decompress chunk at offset %d: %w", ie.offset, err)
	}

	return decodeAllEntries(raw)
}

// All reads and decompresses every chunk in order, returning the full
// sorted entry list (including tombstones). This is the SSTable side of
// what compaction needs: a complete, in-order stream of one file's
// contents to merge against others — compaction genuinely does need
// every entry, since it's rewriting the whole file.
func (r *Reader) All() ([]Entry, error) {
	var all []Entry
	for i := range r.index {
		entries, err := r.readChunk(i)
		if err != nil {
			return nil, fmt.Errorf("sstable: reading chunk %d: %w", i, err)
		}
		all = append(all, entries...)
	}
	return all, nil
}

// RangeScan returns every entry (including tombstones — same convention
// as All, callers filter those) whose key falls in [start, end], using
// the sparse index to decompress only the chunks that could possibly
// contain a matching key, rather than the whole file.
//
// This exists because db.DB.Scan originally called All() on every
// SSTable regardless of the requested range — correct, but it meant a
// scan over a 100-key window in a database with 20,000 keys still
// decompressed the ENTIRE dataset on every call. A benchmark comparing
// lsmdb's range scan against BoltDB's cursor surfaced this directly (a
// roughly 300x gap that was far too large to be normal LSM overhead),
// which is exactly the kind of problem a real benchmark suite is
// supposed to catch — see the benchmark/ package's README notes for the
// full before/after numbers.
//
// nil for start means "from the beginning of the file"; nil for end
// means "to the end of the file" — same convention db.DB.Scan uses.
func (r *Reader) RangeScan(start, end []byte) ([]Entry, error) {
	startChunk := 0
	if start != nil {
		if idx := r.findChunk(start); idx >= 0 {
			startChunk = idx
		}
		// If findChunk returns -1, start is before every chunk's first
		// key, meaning the range could still start at chunk 0 — leaving
		// startChunk at its zero value (0) is already correct for that
		// case, so no special handling needed here.
	}

	var out []Entry
	for i := startChunk; i < len(r.index); i++ {
		// Once a chunk's firstKey is already past the end of the range,
		// every later chunk (sorted by construction) is too — safe to
		// stop scanning the index entirely, not just skip this chunk.
		if end != nil && i > startChunk && bytes.Compare(r.index[i].firstKey, end) > 0 {
			break
		}

		entries, err := r.readChunk(i)
		if err != nil {
			return nil, fmt.Errorf("sstable: range scan reading chunk %d: %w", i, err)
		}
		for _, e := range entries {
			if start != nil && bytes.Compare(e.Key, start) < 0 {
				continue
			}
			if end != nil && bytes.Compare(e.Key, end) > 0 {
				// Entries within a chunk are sorted too, so once we're
				// past end there's nothing more to find in this chunk
				// either — but other chunks might still need checking
				// against the outer loop's chunk-level boundary above,
				// so this only breaks the inner per-entry loop.
				break
			}
			out = append(out, e)
		}
	}
	return out, nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
