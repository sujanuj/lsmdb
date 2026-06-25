package compaction

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

func TestMergeNoOverlapJustInterleaves(t *testing.T) {
	gen0 := []sstable.Entry{e("a", "1", sstable.OpPut), e("c", "3", sstable.OpPut)}
	gen1 := []sstable.Entry{e("b", "2", sstable.OpPut), e("d", "4", sstable.OpPut)}

	out := Merge([][]sstable.Entry{gen0, gen1}, false)

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

func TestMergeNewerGenerationWinsOnOverlap(t *testing.T) {
	// gen0 (older) has "k"=old; gen1 (newer) has "k"=new. Newer must win.
	gen0 := []sstable.Entry{e("k", "old", sstable.OpPut)}
	gen1 := []sstable.Entry{e("k", "new", sstable.OpPut)}

	out := Merge([][]sstable.Entry{gen0, gen1}, false)

	if len(out) != 1 {
		t.Fatalf("got %d entries, want 1 (the older value must be discarded, not kept alongside)", len(out))
	}
	if string(out[0].Value) != "new" {
		t.Errorf("surviving value = %q, want %q", out[0].Value, "new")
	}
}

func TestMergeNewerTombstoneShadowsOlderValueWithoutDropping(t *testing.T) {
	// gen0 has a real value; gen1 (newer) has a tombstone for the same
	// key. With dropObsoleteTombstones=false, the tombstone must still
	// be the thing that survives (NOT the older real value) — this is
	// the exact bug class compaction must avoid: resurrecting deleted
	// data because the merge picked the wrong generation.
	gen0 := []sstable.Entry{e("k", "still-there?", sstable.OpPut)}
	gen1 := []sstable.Entry{e("k", "", sstable.OpDelete)}

	out := Merge([][]sstable.Entry{gen0, gen1}, false)

	if len(out) != 1 {
		t.Fatalf("got %d entries, want 1", len(out))
	}
	if out[0].Op != sstable.OpDelete {
		t.Fatalf("surviving entry Op = %v, want OpDelete — the older live value must NOT resurrect", out[0].Op)
	}
}

func TestMergeDropObsoleteTombstonesReclaimsSpace(t *testing.T) {
	gen0 := []sstable.Entry{e("k", "v", sstable.OpPut)}
	gen1 := []sstable.Entry{e("k", "", sstable.OpDelete)}

	out := Merge([][]sstable.Entry{gen0, gen1}, true)

	if len(out) != 0 {
		t.Fatalf("got %d entries, want 0 — the tombstone won its key and dropObsoleteTombstones=true, so it should be fully reclaimed", len(out))
	}
}

func TestMergeDropObsoleteTombstonesOnlyDropsTombstonesNotLiveValues(t *testing.T) {
	// Sanity check that dropObsoleteTombstones doesn't accidentally drop
	// live entries too — only entries whose WINNING (surviving) version
	// is itself a tombstone get dropped.
	gen0 := []sstable.Entry{e("alive", "yes", sstable.OpPut)}
	gen1 := []sstable.Entry{e("dead", "", sstable.OpDelete)}

	out := Merge([][]sstable.Entry{gen0, gen1}, true)

	if len(out) != 1 {
		t.Fatalf("got %d entries, want 1 (only 'alive' should survive)", len(out))
	}
	if string(out[0].Key) != "alive" {
		t.Errorf("surviving entry key = %q, want %q", out[0].Key, "alive")
	}
}

func TestMergeThreeGenerationsSameKeyOnlyNewestSurvives(t *testing.T) {
	gen0 := []sstable.Entry{e("k", "oldest", sstable.OpPut)}
	gen1 := []sstable.Entry{e("k", "middle", sstable.OpPut)}
	gen2 := []sstable.Entry{e("k", "newest", sstable.OpPut)}

	out := Merge([][]sstable.Entry{gen0, gen1, gen2}, false)

	if len(out) != 1 {
		t.Fatalf("got %d entries for a key written in all 3 generations, want exactly 1", len(out))
	}
	if string(out[0].Value) != "newest" {
		t.Errorf("surviving value = %q, want %q", out[0].Value, "newest")
	}
}

func TestMergeOutputIsSorted(t *testing.T) {
	gen0 := []sstable.Entry{e("m", "1", sstable.OpPut), e("z", "2", sstable.OpPut)}
	gen1 := []sstable.Entry{e("a", "3", sstable.OpPut), e("n", "4", sstable.OpPut)}
	gen2 := []sstable.Entry{e("b", "5", sstable.OpPut), e("y", "6", sstable.OpPut)}

	out := Merge([][]sstable.Entry{gen0, gen1, gen2}, false)

	for i := 1; i < len(out); i++ {
		if bytes.Compare(out[i-1].Key, out[i].Key) >= 0 {
			t.Fatalf("output not strictly sorted at index %d: %q >= %q", i, out[i-1].Key, out[i].Key)
		}
	}
}

func TestMergeEmptyInputs(t *testing.T) {
	out := Merge([][]sstable.Entry{}, false)
	if len(out) != 0 {
		t.Errorf("Merge of no generations should produce 0 entries, got %d", len(out))
	}

	out2 := Merge([][]sstable.Entry{{}, {}}, false)
	if len(out2) != 0 {
		t.Errorf("Merge of empty generations should produce 0 entries, got %d", len(out2))
	}
}

// TestMergeLargeRandomCrossCheck is the heavyweight correctness test:
// many generations, many overlapping keys, deletes mixed in, all
// cross-checked against a reference map that simulates the same
// "process generations oldest to newest, last write wins" rule using
// plain Go code with no merge-heap cleverness at all. If the real
// k-way-merge implementation disagrees with this dead-simple reference
// on ANY key, that's a real bug.
func TestMergeLargeRandomCrossCheck(t *testing.T) {
	const numGenerations = 6
	const keyspaceSize = 2000
	const keysPerGeneration = 800 // fewer than keyspaceSize, but still forces real overlap across 6 generations

	rng := rand.New(rand.NewSource(7))

	var generations [][]sstable.Entry
	reference := make(map[string]sstable.Entry) // simulates "last write wins" oldest->newest

	for g := 0; g < numGenerations; g++ {
		seen := make(map[string]bool)
		var gen []sstable.Entry
		for len(gen) < keysPerGeneration {
			k := fmt.Sprintf("key-%04d", rng.Intn(keyspaceSize))
			if seen[k] {
				continue // one entry per key per generation — same constraint a real SSTable has
			}
			seen[k] = true

			var entry sstable.Entry
			if rng.Intn(5) == 0 { // 20% deletes
				entry = e(k, "", sstable.OpDelete)
			} else {
				entry = e(k, fmt.Sprintf("val-g%d", g), sstable.OpPut)
			}
			gen = append(gen, entry)
			reference[k] = entry // later generations overwrite earlier ones in the reference too
		}
		sort.Slice(gen, func(i, j int) bool { return bytes.Compare(gen[i].Key, gen[j].Key) < 0 })
		generations = append(generations, gen)
	}

	out := Merge(generations, false)

	if len(out) != len(reference) {
		t.Fatalf("merge produced %d entries, reference map has %d distinct keys", len(out), len(reference))
	}

	for i, got := range out {
		want, ok := reference[string(got.Key)]
		if !ok {
			t.Fatalf("merge produced unexpected key %q at index %d", got.Key, i)
		}
		if got.Op != want.Op || !bytes.Equal(got.Value, want.Value) {
			t.Errorf("key %q: merge produced %+v, reference says %+v", got.Key, got, want)
		}
	}

	// Also confirm sortedness held across this much larger, more
	// realistic dataset, not just the small hand-written cases above.
	for i := 1; i < len(out); i++ {
		if bytes.Compare(out[i-1].Key, out[i].Key) >= 0 {
			t.Fatalf("output not sorted at index %d: %q >= %q", i, out[i-1].Key, out[i].Key)
		}
	}
}

// TestMergeLargeRandomCrossCheckWithTombstoneDropping repeats the same
// cross-check but with dropObsoleteTombstones=true, confirming the
// output matches the reference MINUS any keys whose final state is a
// tombstone (which should be physically absent from the output, not
// present as a delete marker).
func TestMergeLargeRandomCrossCheckWithTombstoneDropping(t *testing.T) {
	const numGenerations = 5
	const keyspaceSize = 1500
	const keysPerGeneration = 600 // fewer than keyspaceSize, but still forces real overlap

	rng := rand.New(rand.NewSource(99))

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
			if rng.Intn(4) == 0 { // 25% deletes
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

	out := Merge(generations, true)

	wantLiveCount := 0
	for _, e := range reference {
		if e.Op == sstable.OpPut {
			wantLiveCount++
		}
	}
	if len(out) != wantLiveCount {
		t.Fatalf("merge with tombstone-dropping produced %d entries, want %d (only live final values)", len(out), wantLiveCount)
	}

	for _, got := range out {
		if got.Op == sstable.OpDelete {
			t.Fatalf("found a tombstone in output despite dropObsoleteTombstones=true: %q", got.Key)
		}
		want := reference[string(got.Key)]
		if want.Op != sstable.OpPut {
			t.Fatalf("merge kept key %q but reference says its final state is a tombstone", got.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("key %q: value = %q, want %q", got.Key, got.Value, want.Value)
		}
	}
}
