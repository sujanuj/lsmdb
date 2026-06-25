package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestReplaySkipsTrailingPartialRecord simulates the exact failure mode a
// real crash produces: a WAL file on disk whose final record is cut off
// partway through, because the process died after write() returned but
// (for SyncManual) before the data was fully flushed, or because the
// write() syscall itself was interrupted partway. Replay must recover
// every complete record and silently drop only the partial tail — never
// corrupt earlier data, never panic, never silently return wrong data.
func TestReplaySkipsTrailingPartialRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	good := []Record{
		{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: OpPut, Key: []byte("k2"), Value: []byte("v2")},
		{Op: OpDelete, Key: []byte("k1")},
	}

	var buf bytes.Buffer
	for _, r := range good {
		buf.Write(Encode(r))
	}

	// Append one more record but only write half of its bytes — this is
	// what the file looks like if the process was killed mid-Append.
	partial := Encode(Record{Op: OpPut, Key: []byte("k3"), Value: []byte("v3-never-finished")})
	buf.Write(partial[:len(partial)/2])

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	var replayed []Record
	err := Replay(path, func(r Record) error {
		replayed = append(replayed, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay returned error, want clean stop at partial record: %v", err)
	}

	if len(replayed) != len(good) {
		t.Fatalf("replayed %d records, want %d (the partial trailing record must be dropped)", len(replayed), len(good))
	}
	for i, want := range good {
		got := replayed[i]
		if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.Op != want.Op {
			t.Errorf("record %d: got %+v, want %+v", i, got, want)
		}
	}
}

// TestAppendThenReplayRoundTrip is the "real" crash-recovery story end to
// end, minus an actual OS-level kill -9: write N records through the real
// Log.Append API with fsync after every write, close without any special
// cleanup, then replay from a fresh Open and confirm every record comes
// back exactly. This proves the writer and the replayer agree on the
// format and that fsync'd data is durable across a Close/Open cycle.
func TestAppendThenReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	log, err := Open(path, SyncEveryWrite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var written []Record
	for i := 0; i < 1000; i++ {
		r := Record{
			Op:    OpPut,
			Key:   []byte(fmt.Sprintf("key-%04d", i)),
			Value: []byte(fmt.Sprintf("value-%04d", i)),
		}
		if err := log.Append(r); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		written = append(written, r)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var replayed []Record
	err = Replay(path, func(r Record) error {
		replayed = append(replayed, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != len(written) {
		t.Fatalf("replayed %d records, want %d", len(replayed), len(written))
	}
	for i := range written {
		if !bytes.Equal(replayed[i].Key, written[i].Key) || !bytes.Equal(replayed[i].Value, written[i].Value) {
			t.Errorf("record %d mismatch: got %+v, want %+v", i, replayed[i], written[i])
		}
	}
}
