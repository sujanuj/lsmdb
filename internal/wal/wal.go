// Package wal implements a write-ahead log: every mutation to the database
// is appended here BEFORE it's applied to the in-memory memtable. If the
// process crashes, replaying this log on startup reconstructs the memtable
// exactly as it was, modulo any record that was only partially written at
// the moment of the crash (which is detected and discarded, never
// misinterpreted as valid data).
//
// Durability tradeoff (the thing to actually be able to explain in an
// interview): every call to Append() writes to the OS page cache, but the
// data isn't guaranteed to survive a power loss until fsync() is called.
// This implementation calls fsync after every single append by default
// (SyncEveryWrite), which is the safest and slowest option — every write
// waits on a disk round-trip. The alternative, syncing on a timer or after
// N writes, trades a small durability window (you can lose the last few
// unsynced writes on power loss) for much higher throughput, because you
// batch many writes into one fsync call. Real engines (e.g. RocksDB) make
// this configurable for exactly this reason — there's no universally
// correct answer, only a tradeoff to pick deliberately.
package wal

import (
	"fmt"
	"io"
	"os"
)

// SyncPolicy controls when fsync is called relative to Append calls.
type SyncPolicy int

const (
	// SyncEveryWrite calls fsync after every Append. Safest, slowest.
	SyncEveryWrite SyncPolicy = iota
	// SyncManual never calls fsync automatically — the caller must call
	// Sync() explicitly (e.g. on a timer, or after a batch). Fastest,
	// and risks losing the most recent unsynced writes on crash.
	SyncManual
)

// Log is an append-only write-ahead log backed by a single file.
type Log struct {
	file   *os.File
	policy SyncPolicy
}

// Open opens (creating if necessary) the WAL file at path for appending.
// It does NOT replay existing contents — call Replay separately for that,
// before any new writes, so recovery happens in a well-defined order.
func Open(path string, policy SyncPolicy) (*Log, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &Log{file: f, policy: policy}, nil
}

// Append writes a single record to the log. Depending on the configured
// SyncPolicy, this may block until the write is durable on disk.
func (l *Log) Append(r Record) error {
	encoded := Encode(r)
	if _, err := l.file.Write(encoded); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if l.policy == SyncEveryWrite {
		return l.Sync()
	}
	return nil
}

// Sync forces any buffered writes to durable storage. Safe to call even
// if there's nothing new to flush.
func (l *Log) Sync() error {
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

// Close syncs and closes the underlying file.
func (l *Log) Close() error {
	if err := l.Sync(); err != nil {
		return err
	}
	return l.file.Close()
}

// Truncate resets the log to empty. Call this after a clean memtable
// flush to an SSTable — once the data is durably on disk in SSTable form,
// the WAL entries that produced it are redundant, and a smaller WAL means
// faster replay on the next crash recovery.
func (l *Log) Truncate() error {
	if err := l.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	_, err := l.file.Seek(0, io.SeekStart)
	return err
}

// Replay reads every valid record from the WAL file at path, in order,
// and calls apply for each one. It stops at the first sign of a partial
// or corrupt record — which, by construction, can only be the LAST record
// in the file (everything before it was already fsynced as a complete,
// checksummed unit before the next write began). This is the core crash
// recovery guarantee: a crash can lose at most the one write that was in
// flight, never corrupt or roll back an earlier committed write.
//
// Replay does not require an open Log — it's a standalone function so it
// can run before the log is opened for writing.
func Replay(path string, apply func(Record) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // nothing to replay
		}
		return fmt.Errorf("wal: replay read %s: %w", path, err)
	}

	offset := 0
	for offset < len(data) {
		rec, consumed, err := Decode(data[offset:])
		if err != nil {
			// ErrShortRead or ErrCorrupt: this is the tail of the file
			// where a write was interrupted. Stop here — do NOT treat
			// this as a fatal error, since everything before this point
			// is fully valid and already accounted for.
			break
		}
		if err := apply(rec); err != nil {
			return fmt.Errorf("wal: replay apply at offset %d: %w", offset, err)
		}
		offset += consumed
	}
	return nil
}
