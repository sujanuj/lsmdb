package wal

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Record{
		{Op: OpPut, Key: []byte("hello"), Value: []byte("world")},
		{Op: OpDelete, Key: []byte("gone"), Value: nil},
		{Op: OpPut, Key: []byte(""), Value: []byte("empty key")},
		{Op: OpPut, Key: []byte("k"), Value: []byte("")},
	}

	for _, want := range cases {
		encoded := Encode(want)
		got, consumed, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode(%q) returned error: %v", want.Key, err)
		}
		if consumed != len(encoded) {
			t.Errorf("consumed = %d, want %d", consumed, len(encoded))
		}
		if got.Op != want.Op {
			t.Errorf("Op = %v, want %v", got.Op, want.Op)
		}
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("Key = %q, want %q", got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("Value = %q, want %q", got.Value, want.Value)
		}
	}
}

func TestDecodeDetectsCorruption(t *testing.T) {
	rec := Record{Op: OpPut, Key: []byte("key"), Value: []byte("value")}
	encoded := Encode(rec)

	// Flip a bit in the value bytes — checksum was computed over the
	// original, so this must be caught.
	corrupted := append([]byte{}, encoded...)
	corrupted[len(corrupted)-1] ^= 0xFF

	_, _, err := Decode(corrupted)
	if err != ErrCorrupt {
		t.Errorf("Decode(corrupted) error = %v, want ErrCorrupt", err)
	}
}

func TestDecodeDetectsTruncation(t *testing.T) {
	rec := Record{Op: OpPut, Key: []byte("key"), Value: []byte("a fairly long value here")}
	encoded := Encode(rec)

	// Simulate a process crash that cut the write off partway through —
	// truncate at every possible byte boundary and verify we never get a
	// false-positive successful decode.
	for cut := 0; cut < len(encoded); cut++ {
		truncated := encoded[:cut]
		_, _, err := Decode(truncated)
		if err == nil {
			t.Fatalf("Decode(truncated to %d/%d bytes) succeeded but should have failed", cut, len(encoded))
		}
		if err != ErrShortRead && err != ErrCorrupt {
			t.Fatalf("Decode(truncated to %d bytes) returned unexpected error: %v", cut, err)
		}
	}
}

func TestMultipleRecordsBackToBack(t *testing.T) {
	recs := []Record{
		{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("b"), Value: []byte("2")},
		{Op: OpDelete, Key: []byte("a"), Value: nil},
	}

	var buf bytes.Buffer
	for _, r := range recs {
		buf.Write(Encode(r))
	}

	data := buf.Bytes()
	offset := 0
	var decoded []Record
	for offset < len(data) {
		rec, consumed, err := Decode(data[offset:])
		if err != nil {
			t.Fatalf("Decode at offset %d: %v", offset, err)
		}
		decoded = append(decoded, rec)
		offset += consumed
	}

	if len(decoded) != len(recs) {
		t.Fatalf("decoded %d records, want %d", len(decoded), len(recs))
	}
	for i, want := range recs {
		got := decoded[i]
		if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.Op != want.Op {
			t.Errorf("record %d mismatch: got %+v, want %+v", i, got, want)
		}
	}
}
