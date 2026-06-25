package sstable

import (
	"bytes"
	"testing"
)

func TestEntryEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Entry{
		{Key: []byte("hello"), Value: []byte("world"), Op: OpPut},
		{Key: []byte("gone"), Value: nil, Op: OpDelete},
		{Key: []byte(""), Value: []byte("empty key"), Op: OpPut},
		{Key: []byte("k"), Value: []byte(""), Op: OpPut},
	}

	for _, want := range cases {
		encoded := encodeEntry(want)
		got, consumed, err := decodeEntry(encoded)
		if err != nil {
			t.Fatalf("decodeEntry(%q): %v", want.Key, err)
		}
		if consumed != len(encoded) {
			t.Errorf("consumed = %d, want %d", consumed, len(encoded))
		}
		if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.Op != want.Op {
			t.Errorf("got %+v, want %+v", got, want)
		}
	}
}

func TestDecodeAllEntries(t *testing.T) {
	want := []Entry{
		{Key: []byte("a"), Value: []byte("1"), Op: OpPut},
		{Key: []byte("b"), Value: []byte("2"), Op: OpPut},
		{Key: []byte("c"), Value: nil, Op: OpDelete},
	}

	var buf bytes.Buffer
	for _, e := range want {
		buf.Write(encodeEntry(e))
	}

	got, err := decodeAllEntries(buf.Bytes())
	if err != nil {
		t.Fatalf("decodeAllEntries: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) || got[i].Op != want[i].Op {
			t.Errorf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}
