package sstable

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"os"
)

// entriesPerChunk controls index sparsity: one index entry is recorded
// per chunk, and each chunk holds this many data entries compressed
// together. 64 is a reasonable middle ground — large enough that the
// index stays small relative to the data (the whole point of a sparse
// index), small enough that decompressing one chunk to satisfy a single
// point lookup doesn't do excessive wasted work.
const entriesPerChunk = 64

// magicNumber is written at the very end of the footer as a sanity check:
// any file that doesn't end with this exact value is either not an
// SSTable or has been corrupted, and should be rejected rather than
// silently misinterpreted.
const magicNumber uint64 = 0x53535441424C4530 // ASCII "SSTABLE0"

// footerSize is fixed and always the same: 3 uint64 fields of metadata
// plus the magic number. Fixed size is what makes opening a file cheap —
// seek to (filesize - footerSize), read exactly that many bytes, done.
const footerSize = 8 * 4 // indexOffset, indexLen, numEntries, magic

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
func Write(path string, entries []Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("sstable: create %s: %w", path, err)
	}
	defer f.Close()

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

	indexOffset := offset
	indexBytes := encodeIndex(index)
	if _, err := f.Write(indexBytes); err != nil {
		return fmt.Errorf("sstable: write index: %w", err)
	}

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(indexBytes)))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(len(entries)))
	binary.LittleEndian.PutUint64(footer[24:32], magicNumber)
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
	// segment — that ordering is exactly what Phase 4 (the db package
	// tying WAL+memtable+sstable together) needs to get right.
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
