package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	s := NewSkipList(42)
	s.Put([]byte("b"), []byte("2"))
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("c"), []byte("3"))

	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		got, found := s.Get([]byte(k))
		if !found {
			t.Fatalf("Get(%q): not found", k)
		}
		if string(got) != want {
			t.Errorf("Get(%q) = %q, want %q", k, got, want)
		}
	}

	if _, found := s.Get([]byte("missing")); found {
		t.Error("Get(missing) should not be found")
	}
}

func TestOverwrite(t *testing.T) {
	s := NewSkipList(1)
	s.Put([]byte("k"), []byte("v1"))
	s.Put([]byte("k"), []byte("v2"))

	got, found := s.Get([]byte("k"))
	if !found || string(got) != "v2" {
		t.Errorf("Get(k) = %q, found=%v; want v2, true", got, found)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1 (overwrite shouldn't add a second entry)", s.Len())
	}
}

func TestDeleteThenGet(t *testing.T) {
	s := NewSkipList(7)
	s.Put([]byte("k"), []byte("v"))
	s.Delete([]byte("k"))

	if _, found := s.Get([]byte("k")); found {
		t.Error("Get after Delete should report not found")
	}

	_, existsHere, isDeleted := s.GetWithTombstone([]byte("k"))
	if !existsHere {
		t.Error("tombstone node should still exist physically")
	}
	if !isDeleted {
		t.Error("isDeleted should be true after Delete")
	}
}

func TestDeleteThenPutResurrects(t *testing.T) {
	s := NewSkipList(9)
	s.Put([]byte("k"), []byte("v1"))
	s.Delete([]byte("k"))
	s.Put([]byte("k"), []byte("v2"))

	got, found := s.Get([]byte("k"))
	if !found || string(got) != "v2" {
		t.Errorf("Get(k) after delete+put = %q, found=%v; want v2, true", got, found)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
}

func TestDeleteNonexistentKeyStillTombstones(t *testing.T) {
	// This matters for the multi-SSTable case: a delete for a key that
	// doesn't exist in THIS memtable might still need to shadow a value
	// sitting in an older, already-flushed SSTable.
	s := NewSkipList(3)
	s.Delete([]byte("never-existed"))

	_, existsHere, isDeleted := s.GetWithTombstone([]byte("never-existed"))
	if !existsHere || !isDeleted {
		t.Error("Delete on a nonexistent key should still create a tombstone")
	}
}

func TestSortedIteration(t *testing.T) {
	s := NewSkipList(123)
	keys := []string{"banana", "apple", "cherry", "date", "elderberry"}
	for _, k := range keys {
		s.Put([]byte(k), []byte("v-"+k))
	}

	entries := s.All()
	if len(entries) != len(keys) {
		t.Fatalf("All() returned %d entries, want %d", len(entries), len(keys))
	}
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Errorf("entries not strictly sorted at index %d: %q >= %q", i, entries[i-1].Key, entries[i].Key)
		}
	}
}

func TestAllIncludesTombstones(t *testing.T) {
	s := NewSkipList(55)
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("b"), []byte("2"))
	s.Delete([]byte("a"))

	entries := s.All()
	if len(entries) != 2 {
		t.Fatalf("All() returned %d entries, want 2 (live entry + tombstone)", len(entries))
	}
	found := map[string]bool{}
	for _, e := range entries {
		if string(e.Key) == "a" && !e.Deleted {
			t.Error("entry 'a' should be marked Deleted")
		}
		found[string(e.Key)] = true
	}
	if !found["a"] || !found["b"] {
		t.Error("All() should include both the tombstone and the live entry")
	}
}

// TestLargeRandomWorkload inserts a large number of random keys (with
// some overwrites and deletes mixed in), and cross-checks every read
// against a plain Go map used as a reference implementation. This is the
// single most valuable correctness test for the skip list: it isn't
// testing a hand-picked scenario, it's testing that the structure agrees
// with ground truth across thousands of operations and varied key
// orderings.
func TestLargeRandomWorkload(t *testing.T) {
	const n = 20000
	s := NewSkipList(999)
	reference := make(map[string]string)
	deleted := make(map[string]bool)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", rng.Intn(n/2)) // force collisions/overwrites
		op := rng.Intn(3)
		switch op {
		case 0, 1: // Put (weighted more likely than delete)
			value := fmt.Sprintf("val-%d", i)
			s.Put([]byte(key), []byte(value))
			reference[key] = value
			deleted[key] = false
		case 2: // Delete
			s.Delete([]byte(key))
			delete(reference, key)
			deleted[key] = true
		}
	}

	for key, wantValue := range reference {
		got, found := s.Get([]byte(key))
		if !found {
			t.Fatalf("Get(%q): not found, want %q", key, wantValue)
		}
		if string(got) != wantValue {
			t.Fatalf("Get(%q) = %q, want %q", key, got, wantValue)
		}
	}
	for key, isDel := range deleted {
		if !isDel {
			continue
		}
		if _, found := reference[key]; found {
			continue // was deleted then re-put, already checked above
		}
		if _, found := s.Get([]byte(key)); found {
			t.Fatalf("Get(%q): found, but key should be deleted", key)
		}
	}

	// Cross-check sortedness and that iteration matches the reference
	// map's live keys exactly.
	entries := s.All()
	var liveKeys []string
	for _, e := range entries {
		if !e.Deleted {
			liveKeys = append(liveKeys, string(e.Key))
		}
		if string(e.Key) < "" { // trivially false, keeps "i" usage realistic
		}
	}
	var refKeys []string
	for k := range reference {
		refKeys = append(refKeys, k)
	}
	sort.Strings(refKeys)
	sort.Strings(liveKeys)
	if len(liveKeys) != len(refKeys) {
		t.Fatalf("live key count = %d, want %d", len(liveKeys), len(refKeys))
	}
	for i := range refKeys {
		if liveKeys[i] != refKeys[i] {
			t.Fatalf("live keys differ from reference at index %d: %q vs %q", i, liveKeys[i], refKeys[i])
		}
	}
}

// TestConcurrentReadersDuringWrites exercises the RWMutex: many goroutines
// reading concurrently with a single writer goroutine, checked with -race
// to catch any data race in the locking discipline.
func TestConcurrentReadersDuringWrites(t *testing.T) {
	s := NewSkipList(2024)
	const writes = 5000
	const readers = 8

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s.Get([]byte(fmt.Sprintf("key-%04d", rand.Intn(writes))))
					s.All()
				}
			}
		}()
	}

	for i := 0; i < writes; i++ {
		s.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%d", i)))
	}
	close(stop)
	wg.Wait()

	if s.Len() != writes {
		t.Errorf("Len() = %d, want %d", s.Len(), writes)
	}
}

// TestRandomLevelDistribution sanity-checks the custom xorshift-based
// level generator: with p=0.25, the fraction of nodes promoted to level
// L+1 given they reached level L should be roughly 0.25. This doesn't
// need to be exact, but a generator with a serious bug (e.g. always
// returning 1, or a badly biased distribution) should fail this loudly.
func TestRandomLevelDistribution(t *testing.T) {
	s := NewSkipList(777)
	const trials = 100000
	counts := make(map[int]int)
	for i := 0; i < trials; i++ {
		counts[s.randomLevel()]++
	}

	if counts[1] == 0 {
		t.Fatal("level 1 should be the most common outcome and must occur")
	}

	// Expected: count(level L) / count(level L-1) ≈ p = 0.25, for the
	// first few levels where we have enough samples to be meaningful.
	for lvl := 2; lvl <= 4; lvl++ {
		if counts[lvl-1] < 50 {
			break // not enough samples at this depth to judge ratio
		}
		ratio := float64(counts[lvl]) / float64(counts[lvl-1])
		if ratio < 0.15 || ratio > 0.40 {
			t.Errorf("level %d / level %d ratio = %.3f, want roughly 0.25 (got level counts: %v)", lvl, lvl-1, ratio, counts)
		}
	}
}

func TestXorshiftNeverGetsStuckAtZero(t *testing.T) {
	x := newXorshift64(0) // the documented danger case
	for i := 0; i < 1000; i++ {
		if x.next() == 0 {
			// Zero outputs can legitimately occur sometimes; the actual
			// failure mode would be EVERY output being zero forever.
			continue
		}
	}
	// If we got here without the generator's internal state collapsing
	// to a fixed point, next() would have produced varied nonzero output
	// across 1000 calls — spot check a handful directly.
	seen := map[uint64]bool{}
	x2 := newXorshift64(0)
	for i := 0; i < 20; i++ {
		seen[x2.next()] = true
	}
	if len(seen) < 15 {
		t.Errorf("xorshift seeded with 0 produced only %d distinct values in 20 calls, looks stuck", len(seen))
	}
}
