package sstable

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// Reader provides read access to an existing, immutable SSTable file.
// Opening a Reader loads only the index block into memory — the
// (potentially much larger) data chunks are read and decompressed lazily,
// only the specific chunk(s) a Get or iteration actually needs.
type Reader struct {
	path       string
	index      []indexEntry
	numEntries int
}

// Open reads the footer and index block of the SSTable at path, without
// touching any data chunks. This is intentionally cheap: footer is a
// fixed size at a fixed position (seek to end, read backward), and the
// index is sized to be small relative to the data by construction
// (entriesPerChunk controls exactly this tradeoff).
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
	numEntries := binary.LittleEndian.Uint64(footer[16:24])
	magic := binary.LittleEndian.Uint64(footer[24:32])

	if magic != magicNumber {
		return nil, fmt.Errorf("sstable: %s has invalid magic number %x, not a valid SSTable file (or it's corrupted)", path, magic)
	}

	indexBytes := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBytes, int64(indexOffset)); err != nil {
		return nil, fmt.Errorf("sstable: read index block of %s: %w", path, err)
	}

	index, err := decodeIndex(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("sstable: decode index of %s: %w", path, err)
	}

	return &Reader{path: path, index: index, numEntries: int(numEntries)}, nil
}

// NumEntries returns the total entry count recorded in the footer,
// including tombstones. Cheap — answered entirely from the already-loaded
// footer, no chunk reads needed.
func (r *Reader) NumEntries() int {
	return r.numEntries
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
// memtable.Memtable.GetWithTombstone deliberately — the read path that
// will be built in Phase 4 needs to walk memtable -> newest SSTable ->
// older SSTables and stop as soon as ANY layer reports existsHere=true,
// using isDeleted to decide the final answer.
func (r *Reader) GetWithTombstone(key []byte) (value []byte, existsHere bool, isDeleted bool) {
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
		// Phase 4 callers should propagate or log this loudly.
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
// what Phase 4's compaction and range-scan logic will need: a complete,
// in-order stream of one file's contents to merge against others.
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

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
