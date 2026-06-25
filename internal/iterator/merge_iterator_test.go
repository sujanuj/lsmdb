package iterator

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/sujanuj/lsmdb/internal/sstable"
)

func e(key, value string, op sstable.OpType) sstable.Entry {
	var v []byte
	if op == sstable.OpPut {
		v = []byte(value)
	}
	return sstable.Entry{Key: []byte(key), Value: v, Op: op}
}

func drain(it *MergeIterator) []sstable.Entry {
	var out []sstable.Entry
	for {
		entry, ok := it.Next()
		if !ok {
			break
		}
		out = append(out, entry)
	}
	return out
}

func TestMergeIteratorNoOverlapInterleaves(t *testing.T) {
	gen0 := []sstable.Entry{e("a", "1", sstable.OpPut), e("c", "3", sstable.OpPut)}
	gen1 := []sstable.Entry{e("b", "2", sstable.OpPut), e("d", "4", sstable.OpPut)}

	out := drain(NewMergeIterator([][]sstable.Entry{gen0, gen1}))

	wantKeys := []string{"a", "b", "c", "d"}
	if len(out) != len(wantKeys) {
		t.Fatalf("got %d entries, want %d", len(out), len(wantKeys))
	}
	for i, want := range wantKeys {
		if string(out[i].Key) != want {
			t.Errorf("entry %d: key = %q, want %q", i, out[i].Key, want)
		}
	}
}

func TestMergeIteratorNewerGenerationWins(t *testing.T) {
	gen0 := []sstable.Entry{e("k", "old", sstable.OpPut)}
	gen1 := []sstable.Entry{e("k", "new", sstable.OpPut)}

	out := drain(NewMergeIterator([][]sstable.Entry{gen0, gen1}))

	if len(out) != 1 || string(out[0].Value) != "new" {
		t.Fatalf("got %+v, want exactly one entry with value \"new\"", out)
	}
}

func TestMergeIteratorTombstonesPassThroughUnlessCallerFilters(t *testing.T) {
	// The iterator itself never drops tombstones — that decision belongs
	// to callers (compaction.Merge decides based on safety; db.Scan
	// decides based on "a scan should never show deleted keys"). This
	// test locks in that the raw iterator is neutral on the question.
	gen0 := []sstable.Entry{e("k", "v", sstable.OpPut)}
	gen1 := []sstable.Entry{e("k", "", sstable.OpDelete)}

	out := drain(NewMergeIterator([][]sstable.Entry{gen0, gen1}))

	if len(out) != 1 || out[0].Op != sstable.OpDelete {
		t.Fatalf("got %+v, want exactly one entry that is the (newer) tombstone", out)
	}
}

func TestMergeIteratorStopsEarlyWithoutConsumingEverything(t *testing.T) {
	// This is the actual point of building an iterator instead of a
	// slice-returning function: calling Next() a few times should not
	// require having processed the entire dataset already. Verified
	// indirectly here by using a huge generation and only pulling a few
	// entries — if this were secretly eager, it would still work
	// correctness-wise, but a too-large dataset combined with a B.N-based
	// benchmark would reveal the difference; this test instead checks
	// that partial consumption returns the CORRECT first few entries,
	// which is the behavioral contract that matters for db.Scan.
	const n = 100000
	entries := make([]sstable.Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = e(fmt.Sprintf("key-%06d", i), fmt.Sprintf("val-%d", i), sstable.OpPut)
	}

	it := NewMergeIterator([][]sstable.Entry{entries})

	for i := 0; i < 5; i++ {
		got, ok := it.Next()
		if !ok {
			t.Fatalf("Next() %d: ok=false, want true", i)
		}
		wantKey := fmt.Sprintf("key-%06d", i)
		if string(got.Key) != wantKey {
			t.Fatalf("Next() %d: key = %q, want %q", i, got.Key, wantKey)
		}
	}
}

func TestMergeIteratorThreeGenerationsSameKey(t *testing.T) {
	gen0 := []sstable.Entry{e("k", "oldest", sstable.OpPut)}
	gen1 := []sstable.Entry{e("k", "middle", sstable.OpPut)}
	gen2 := []sstable.Entry{e("k", "newest", sstable.OpPut)}

	out := drain(NewMergeIterator([][]sstable.Entry{gen0, gen1, gen2}))

	if len(out) != 1 || string(out[0].Value) != "newest" {
		t.Fatalf("got %+v, want exactly one entry with value \"newest\"", out)
	}
}

func TestMergeIteratorOutputSorted(t *testing.T) {
	gen0 := []sstable.Entry{e("m", "1", sstable.OpPut), e("z", "2", sstable.OpPut)}
	gen1 := []sstable.Entry{e("a", "3", sstable.OpPut), e("n", "4", sstable.OpPut)}
	gen2 := []sstable.Entry{e("b", "5", sstable.OpPut), e("y", "6", sstable.OpPut)}

	out := drain(NewMergeIterator([][]sstable.Entry{gen0, gen1, gen2}))

	for i := 1; i < len(out); i++ {
		if bytes.Compare(out[i-1].Key, out[i].Key) >= 0 {
			t.Fatalf("output not sorted at index %d: %q >= %q", i, out[i-1].Key, out[i].Key)
		}
	}
}

func TestMergeIteratorEmptyInputs(t *testing.T) {
	out := drain(NewMergeIterator([][]sstable.Entry{}))
	if len(out) != 0 {
		t.Errorf("got %d entries, want 0", len(out))
	}
	out2 := drain(NewMergeIterator([][]sstable.Entry{{}, {}}))
	if len(out2) != 0 {
		t.Errorf("got %d entries, want 0", len(out2))
	}
}

// TestMergeIteratorLargeRandomCrossCheck is the same heavyweight
// correctness test style used for the original compaction.Merge: many
// generations, heavy key overlap, deletes mixed in, cross-checked
// against a dead-simple reference map.
func TestMergeIteratorLargeRandomCrossCheck(t *testing.T) {
	const numGenerations = 6
	const keyspaceSize = 2000
	const keysPerGeneration = 800

	rng := rand.New(rand.NewSource(7))

	var generations [][]sstable.Entry
	reference := make(map[string]sstable.Entry)

	for g := 0; g < numGenerations; g++ {
		seen := make(map[string]bool)
		var gen []sstable.Entry
		for len(gen) < keysPerGeneration {
			k := fmt.Sprintf("key-%04d", rng.Intn(keyspaceSize))
			if seen[k] {
				continue
			}
			seen[k] = true
			var entry sstable.Entry
			if rng.Intn(5) == 0 {
				entry = e(k, "", sstable.OpDelete)
			} else {
				entry = e(k, fmt.Sprintf("val-g%d", g), sstable.OpPut)
			}
			gen = append(gen, entry)
			reference[k] = entry
		}
		sort.Slice(gen, func(i, j int) bool { return bytes.Compare(gen[i].Key, gen[j].Key) < 0 })
		generations = append(generations, gen)
	}

	out := drain(NewMergeIterator(generations))

	if len(out) != len(reference) {
		t.Fatalf("got %d entries, reference has %d distinct keys", len(out), len(reference))
	}
	for i, got := range out {
		want, ok := reference[string(got.Key)]
		if !ok {
			t.Fatalf("unexpected key %q at index %d", got.Key, i)
		}
		if got.Op != want.Op || !bytes.Equal(got.Value, want.Value) {
			t.Errorf("key %q: got %+v, reference says %+v", got.Key, got, want)
		}
	}
	for i := 1; i < len(out); i++ {
		if bytes.Compare(out[i-1].Key, out[i].Key) >= 0 {
			t.Fatalf("output not sorted at index %d", i)
		}
	}
}
