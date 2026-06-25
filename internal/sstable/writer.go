package sstable

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/sujanuj/lsmdb/internal/bloom"
)

// entriesPerChunk controls index sparsity: one index entry is recorded
// per chunk, and each chunk holds this many data entries compressed
// together. 64 is a reasonable middle ground — large enough that the
// index stays small relative to the data (the whole point of a sparse
// index), small enough that decompressing one chunk to satisfy a single
// point lookup doesn't do excessive wasted work.
const entriesPerChunk = 64

// bloomFalsePositiveRate is the target false-positive rate for each
// SSTable's bloom filter. 1% is a standard default: at this rate the
// filter costs about 9.6 bits per key, which is small relative to a
// typical key+value pair, while still turning the large majority of
// true negatives into an O(1) bit-check instead of a chunk decompress.
const bloomFalsePositiveRate = 0.01

// magicNumber is written at the very end of the footer as a sanity check:
// any file that doesn't end with this exact value is either not an
// SSTable or has been corrupted, and should be rejected rather than
// silently misinterpreted. Bumped from the Phase 3 value (...0530, "0")
// to ...0531 ("1") because this phase changes the file layout (adding a
// bloom filter block) — a reader built for format 0 should not silently
// misparse a format-1 file, and vice versa; mismatched magic numbers
// make that impossible by construction.
const magicNumber uint64 = 0x53535441424C4531 // ASCII "SSTABLE1"

// footerSize: indexOffset, indexLen, bloomOffset, bloomLen, bloomNumBits,
// bloomNumHashes, numEntries, magic — 8 uint64 fields.
const footerSize = 8 * 8

// indexEntry records where one compressed chunk lives in the file, and
// the first key in that chunk (since chunks are written in sorted order,
// the first key of chunk i is an inclusive lower bound for every key in
// that chunk — a binary search over these is how a point lookup finds
// the right chunk without reading the whole index linearly... though
// for the modest index sizes here, linear scan over the loaded index is
// also entirely reasonable and is what Reader does for simplicity).
type indexEntry struct {
	firstKey []byte
	offset   uint64
	length   uint64 // length of the COMPRESSED chunk on disk
}

// Write flushes a sorted slice of entries to a brand-new SSTable file at
// path. entries MUST already be sorted by key — this is guaranteed by
// the caller using Memtable.All(), which iterates the skip list in
// sorted order. Write does not sort; doing so here would hide a caller
// bug instead of surfacing it.
//
// A bloom filter covering every key (including tombstoned ones — a
// lookup for a deleted key still needs to find its tombstone, so it must
// not be filtered out) is built during the same pass as chunking, then
// serialized into the file between the data chunks and the index block.
func Write(path string, entries []Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("sstable: create %s: %w", path, err)
	}
	defer f.Close()

	filter := bloom.New(len(entries), bloomFalsePositiveRate)

	var index []indexEntry
	var offset uint64

	for chunkStart := 0; chunkStart < len(entries); chunkStart += entriesPerChunk {
		chunkEnd := chunkStart + entriesPerChunk
		if chunkEnd > len(entries) {
			chunkEnd = len(entries)
		}
		chunk := entries[chunkStart:chunkEnd]

		var raw bytes.Buffer
		for _, e := range chunk {
			raw.Write(encodeEntry(e))
			filter.Add(e.Key)
		}

		compressed, err := gzipCompress(raw.Bytes())
		if err != nil {
			return fmt.Errorf("sstable: compress chunk at entry %d: %w", chunkStart, err)
		}

		n, err := f.Write(compressed)
		if err != nil {
			return fmt.Errorf("sstable: write chunk at entry %d: %w", chunkStart, err)
		}

		index = append(index, indexEntry{
			firstKey: chunk[0].Key,
			offset:   offset,
			length:   uint64(n),
		})
		offset += uint64(n)
	}

	bloomOffset := offset
	bloomBytes := filter.Bytes()
	if _, err := f.Write(bloomBytes); err != nil {
		return fmt.Errorf("sstable: write bloom filter: %w", err)
	}
	offset += uint64(len(bloomBytes))

	indexOffset := offset
	indexBytes := encodeIndex(index)
	if _, err := f.Write(indexBytes); err != nil {
		return fmt.Errorf("sstable: write index: %w", err)
	}

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(indexBytes)))
	binary.LittleEndian.PutUint64(footer[16:24], bloomOffset)
	binary.LittleEndian.PutUint64(footer[24:32], uint64(len(bloomBytes)))
	binary.LittleEndian.PutUint64(footer[32:40], filter.NumBits())
	binary.LittleEndian.PutUint64(footer[40:48], uint64(filter.NumHashes()))
	binary.LittleEndian.PutUint64(footer[48:56], uint64(len(entries)))
	binary.LittleEndian.PutUint64(footer[56:64], magicNumber)
	if _, err := f.Write(footer); err != nil {
		return fmt.Errorf("sstable: write footer: %w", err)
	}

	// Durability note: deliberately NOT calling f.Sync() here by
	// default. An SSTable flush is downstream of the WAL — if the
	// process crashes before this file is fsynced, the WAL still has
	// every write that went into it, and recovery replays the WAL into
	// a fresh memtable and simply redoes this flush. Syncing every
	// SSTable write would add durability that's already provided one
	// layer down, at real cost (gzip + fsync on every flush). A
	// production engine would still want to fsync the SSTable + update
	// a manifest atomically before truncating the corresponding WAL
	// segment — that ordering is exactly what the db package's flush
	// path notes as a real, deliberate gap for now.
	return nil
}

// encodeIndex serializes the index block: one entry per chunk, each as
// [keyLen uint32][key bytes][offset uint64][length uint64].
func encodeIndex(index []indexEntry) []byte {
	var buf bytes.Buffer
	for _, e := range index {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(e.firstKey)))
		buf.Write(hdr[:])
		buf.Write(e.firstKey)
		var rest [16]byte
		binary.LittleEndian.PutUint64(rest[0:8], e.offset)
		binary.LittleEndian.PutUint64(rest[8:16], e.length)
		buf.Write(rest[:])
	}
	return buf.Bytes()
}

// decodeIndex is the inverse of encodeIndex.
func decodeIndex(buf []byte) ([]indexEntry, error) {
	var index []indexEntry
	offset := 0
	for offset < len(buf) {
		if offset+4 > len(buf) {
			return nil, fmt.Errorf("sstable: truncated index entry header at offset %d", offset)
		}
		keyLen := int(binary.LittleEndian.Uint32(buf[offset : offset+4]))
		offset += 4
		if offset+keyLen+16 > len(buf) {
			return nil, fmt.Errorf("sstable: truncated index entry body at offset %d", offset)
		}
		key := make([]byte, keyLen)
		copy(key, buf[offset:offset+keyLen])
		offset += keyLen
		chunkOffset := binary.LittleEndian.Uint64(buf[offset : offset+8])
		chunkLen := binary.LittleEndian.Uint64(buf[offset+8 : offset+16])
		offset += 16
		index = append(index, indexEntry{firstKey: key, offset: chunkOffset, length: chunkLen})
	}
	return index, nil
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
