package wal

import (
	"encoding/binary"
	"hash/crc32"
)

// OpType distinguishes a regular write from a delete (tombstone).
// Deletes are NOT removed from storage immediately — they're written as a
// tombstone record so that during compaction, a delete can correctly
// shadow an older value for the same key even if that older value lives
// in a different (older) SSTable.
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Record is a single logical write: a key, an optional value, and what
// kind of operation it was. Value is empty for deletes.
type Record struct {
	Op    OpType
	Key   []byte
	Value []byte
}

// headerSize is the fixed-size portion of an encoded record, before the
// variable-length key and value bytes:
//
//	4 bytes  CRC32 checksum (covers everything after the checksum itself)
//	1 byte   op type
//	4 bytes  key length
//	4 bytes  value length
const headerSize = 4 + 1 + 4 + 4

// Encode serializes a Record into the on-disk byte format described above.
// The checksum is computed over [op][keyLen][valLen][key][value] — i.e.
// everything except the checksum field itself — so that any corruption
// anywhere in the record is caught on replay.
func Encode(r Record) []byte {
	buf := make([]byte, headerSize+len(r.Key)+len(r.Value))

	// Leave the first 4 bytes (checksum) for last — we need the rest of
	// the buffer filled in before we can compute it.
	buf[4] = byte(r.Op)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(r.Key)))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(r.Value)))
	copy(buf[headerSize:headerSize+len(r.Key)], r.Key)
	copy(buf[headerSize+len(r.Key):], r.Value)

	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return buf
}

// ErrCorrupt is returned by Decode when a record's checksum doesn't match
// its contents — meaning the record was only partially written (e.g. the
// process crashed mid-write) or the file was otherwise corrupted.
// Callers MUST treat this as "stop replaying here", not as a fatal error
// for the whole file: everything before the corrupt record is still valid.
var ErrCorrupt = decodeError("corrupt WAL record: checksum mismatch")

// ErrShortRead means there weren't even enough bytes left to read a full
// header. This is the normal/expected way a WAL file ends if the last
// write was interrupted before the header finished landing on disk.
var ErrShortRead = decodeError("short read: incomplete record header")

type decodeError string

func (e decodeError) Error() string { return string(e) }

// Decode reads a single record starting at the beginning of buf.
// It returns the decoded record and the number of bytes consumed, so the
// caller can advance to the next record. On ErrCorrupt or ErrShortRead,
// consumed is meaningless and replay should stop.
func Decode(buf []byte) (rec Record, consumed int, err error) {
	if len(buf) < headerSize {
		return Record{}, 0, ErrShortRead
	}

	storedChecksum := binary.LittleEndian.Uint32(buf[0:4])
	op := OpType(buf[4])
	keyLen := binary.LittleEndian.Uint32(buf[5:9])
	valLen := binary.LittleEndian.Uint32(buf[9:13])

	total := headerSize + int(keyLen) + int(valLen)
	if len(buf) < total {
		// The header claims a record larger than what's actually in the
		// file. This happens when a write was cut off partway through
		// writing the key/value bytes themselves.
		return Record{}, 0, ErrShortRead
	}

	actualChecksum := crc32.ChecksumIEEE(buf[4:total])
	if actualChecksum != storedChecksum {
		return Record{}, 0, ErrCorrupt
	}

	key := make([]byte, keyLen)
	copy(key, buf[headerSize:headerSize+int(keyLen)])
	value := make([]byte, valLen)
	copy(value, buf[headerSize+int(keyLen):total])

	return Record{Op: op, Key: key, Value: value}, total, nil
}
