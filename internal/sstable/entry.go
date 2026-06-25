// Package sstable implements the on-disk, immutable sorted file format
// that a memtable gets flushed into once it's full. Unlike the WAL,
// which is append-only and mutable, an SSTable is written once, in full,
// and never modified again — compaction produces brand new SSTable files
// rather than editing existing ones.
package sstable

import (
	"encoding/binary"
	"fmt"
)

// OpType mirrors wal.OpType deliberately rather than importing it: an
// SSTable entry needs to record whether a key is a live value or a
// tombstone, for exactly the same reason the memtable does (a delete must
// be able to shadow an older value for the same key sitting in a
// different, older SSTable). Keeping a local copy avoids sstable
// depending on wal's package just for one type — these are two different
// layers that happen to need the same concept.
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Entry is a single key/value pair as it's encoded inside a data chunk.
type Entry struct {
	Key   []byte
	Value []byte
	Op    OpType
}

// entryHeaderSize is the fixed portion before the variable-length key and
// value: 4 bytes key length + 4 bytes value length + 1 byte op type.
const entryHeaderSize = 4 + 4 + 1

// encodeEntry serializes one entry to its on-disk byte representation.
// Multiple encoded entries are concatenated to form the uncompressed
// contents of a chunk, before that chunk is gzip-compressed as a whole.
func encodeEntry(e Entry) []byte {
	buf := make([]byte, entryHeaderSize+len(e.Key)+len(e.Value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(e.Value)))
	buf[8] = byte(e.Op)
	copy(buf[entryHeaderSize:entryHeaderSize+len(e.Key)], e.Key)
	copy(buf[entryHeaderSize+len(e.Key):], e.Value)
	return buf
}

// decodeEntry reads a single entry starting at the beginning of buf,
// returning the entry and how many bytes it consumed. Unlike the WAL's
// decoder, this does not need to handle truncation/corruption gracefully
// — a chunk's contents come from gzip decompression, which already fails
// loudly on corrupt input, and the chunk's own length is always exactly
// the concatenation of however many whole entries it contains. A
// malformed entry here means a bug in encodeEntry/decodeEntry agreement
// or genuine data corruption that gzip's checksum didn't catch (rare),
// either of which should surface as a hard error, not a silent skip.
func decodeEntry(buf []byte) (e Entry, consumed int, err error) {
	if len(buf) < entryHeaderSize {
		return Entry{}, 0, fmt.Errorf("sstable: entry header truncated: have %d bytes, need %d", len(buf), entryHeaderSize)
	}
	keyLen := binary.LittleEndian.Uint32(buf[0:4])
	valLen := binary.LittleEndian.Uint32(buf[4:8])
	op := OpType(buf[8])

	total := entryHeaderSize + int(keyLen) + int(valLen)
	if len(buf) < total {
		return Entry{}, 0, fmt.Errorf("sstable: entry body truncated: have %d bytes, need %d", len(buf), total)
	}

	key := make([]byte, keyLen)
	copy(key, buf[entryHeaderSize:entryHeaderSize+int(keyLen)])
	value := make([]byte, valLen)
	copy(value, buf[entryHeaderSize+int(keyLen):total])

	return Entry{Key: key, Value: value, Op: op}, total, nil
}

// decodeAllEntries decodes every entry in buf back to back. This is what
// runs after a chunk has been gzip-decompressed — buf at that point is
// the exact concatenation of encodeEntry outputs that Writer produced.
func decodeAllEntries(buf []byte) ([]Entry, error) {
	var entries []Entry
	offset := 0
	for offset < len(buf) {
		e, consumed, err := decodeEntry(buf[offset:])
		if err != nil {
			return nil, fmt.Errorf("sstable: decoding entry at offset %d: %w", offset, err)
		}
		entries = append(entries, e)
		offset += consumed
	}
	return entries, nil
}
