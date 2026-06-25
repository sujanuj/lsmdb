package db

import (
	"fmt"
	"sort"
	"testing"
)

func collectScan(it *ScanIterator) map[string]string {
	out := make(map[string]string)
	for {
		k, v, ok := it.Next()
		if !ok {
			break
		}
		out[string(k)] = string(v)
	}
	return out
}

func collectScanOrdered(it *ScanIterator) []string {
	var keys []string
	for {
		k, _, ok := it.Next()
		if !ok {
			break
		}
		keys = append(keys, string(k))
	}
	return keys
}

func TestScanFullRangeReturnsEverything(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	want := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}
	for k, v := range want {
		mustPut(t, database, k, v)
	}

	got := collectScan(database.Scan(nil, nil))
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got %q, want %q", k, got[k], v)
		}
	}
}

func TestScanRangeIsInclusiveOnBothEnds(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, database, k, "v-"+k)
	}

	got := collectScanOrdered(database.Scan([]byte("b"), []byte("d")))
	want := []string{"b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestScanWithOnlyStartBound(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, database, k, "v")
	}

	got := collectScanOrdered(database.Scan([]byte("c"), nil))
	want := []string{"c", "d"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestScanWithOnlyEndBound(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	for _, k := range []string{"a", "b", "c", "d"} {
		mustPut(t, database, k, "v")
	}

	got := collectScanOrdered(database.Scan(nil, []byte("b")))
	want := []string{"a", "b"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestScanResultsAreSortedAndCorrectOrder(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	// Insert deliberately out of order.
	for _, k := range []string{"z", "m", "a", "y", "b"} {
		mustPut(t, database, k, "v")
	}

	got := collectScanOrdered(database.Scan(nil, nil))
	want := []string{"a", "b", "m", "y", "z"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestScanSkipsDeletedKeys(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	mustPut(t, database, "a", "1")
	mustPut(t, database, "b", "2")
	mustPut(t, database, "c", "3")
	if err := database.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := collectScan(database.Scan(nil, nil))
	if _, found := got["b"]; found {
		t.Error("scan should not include deleted key 'b'")
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
}

func TestScanSeesNewestAcrossMemtableAndSSTable(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	mustPut(t, database, "key", "old-value")
	if err := database.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Overwrite in the memtable, unflushed — this must win over the
	// flushed SSTable's value, exactly like Get's shadowing rule.
	mustPut(t, database, "key", "new-value")

	got := collectScan(database.Scan(nil, nil))
	if got["key"] != "new-value" {
		t.Errorf("got %q, want %q (memtable should shadow the SSTable)", got["key"], "new-value")
	}
}

func TestScanSpansMultipleSSTablesWithOverlap(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	mustPut(t, database, "shared", "v1")
	mustPut(t, database, "only-in-1", "a")
	if err := database.Flush(); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}
	mustPut(t, database, "shared", "v2") // overwrite, newer SSTable
	mustPut(t, database, "only-in-2", "b")
	if err := database.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

	got := collectScan(database.Scan(nil, nil))
	want := map[string]string{
		"shared":    "v2", // newer flush wins
		"only-in-1": "a",
		"only-in-2": "b",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries %v, want %d entries %v", len(got), got, len(want), want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got %q, want %q", k, got[k], v)
		}
	}
}

func TestScanEmptyDatabase(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	got := collectScan(database.Scan(nil, nil))
	if len(got) != 0 {
		t.Errorf("got %d entries from an empty database, want 0", len(got))
	}
}

func TestScanRangeWithNoMatches(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	mustPut(t, database, "a", "1")
	mustPut(t, database, "z", "26")

	got := collectScan(database.Scan([]byte("m"), []byte("n")))
	if len(got) != 0 {
		t.Errorf("got %d entries for an empty range, want 0", len(got))
	}
}

// TestScanLargeCrossCheck mixes puts, deletes, flushes, and compactions
// across many keys, then checks the FULL scan against a reference map
// tracked independently — the same style of heavyweight test used for
// the WAL, memtable, and compaction phases.
func TestScanLargeCrossCheck(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	reference := make(map[string]string)

	const numKeys = 80
	const numRounds = 10

	for round := 0; round < numRounds; round++ {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%03d", i)
			if round > 0 && i%9 == 0 {
				if err := database.Delete([]byte(key)); err != nil {
					t.Fatalf("Delete(%q): %v", key, err)
				}
				delete(reference, key)
				continue
			}
			value := fmt.Sprintf("round-%d", round)
			if err := database.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Put(%q): %v", key, err)
			}
			reference[key] = value
		}
		if err := database.Flush(); err != nil {
			t.Fatalf("Flush round %d: %v", round, err)
		}
	}

	got := collectScan(database.Scan(nil, nil))
	if len(got) != len(reference) {
		t.Fatalf("scan returned %d entries, reference has %d", len(got), len(reference))
	}
	for k, want := range reference {
		if got[k] != want {
			t.Errorf("key %q: got %q, want %q", k, got[k], want)
		}
	}

	// Also confirm strict sortedness across this larger, more realistic
	// dataset.
	ordered := collectScanOrdered(database.Scan(nil, nil))
	sorted := append([]string{}, ordered...)
	sort.Strings(sorted)
	for i := range ordered {
		if ordered[i] != sorted[i] {
			t.Fatalf("scan output not sorted at index %d: %q vs sorted %q", i, ordered[i], sorted[i])
		}
	}
}

// TestScanPartialRangeAcrossLargeDataset confirms range filtering picks
// out exactly the right subset, not an approximation, against the same
// reference-map style of cross-check.
func TestScanPartialRangeAcrossLargeDataset(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i)
		mustPut(t, database, key, fmt.Sprintf("val-%d", i))
		if i%100 == 0 {
			if err := database.Flush(); err != nil {
				t.Fatalf("Flush at i=%d: %v", i, err)
			}
		}
	}

	start := []byte("key-0100")
	end := []byte("key-0199")
	got := collectScan(database.Scan(start, end))

	if len(got) != 100 {
		t.Fatalf("got %d entries in range, want 100", len(got))
	}
	for i := 100; i <= 199; i++ {
		key := fmt.Sprintf("key-%04d", i)
		want := fmt.Sprintf("val-%d", i)
		if got[key] != want {
			t.Errorf("key %q: got %q, want %q", key, got[key], want)
		}
	}
}
