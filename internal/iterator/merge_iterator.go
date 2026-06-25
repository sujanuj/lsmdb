// Package iterator provides a generic, lazy k-way merge over multiple
// sorted streams of sstable.Entry, resolving same-key collisions by
// generation (higher generation == newer == wins). This is the one
// piece of merge logic shared by two different consumers:
//
//   - compaction.Merge, which drains a MergeIterator fully into a slice
//     and optionally drops tombstones, to physically rewrite SSTables
//   - db.DB.Scan, which wraps a MergeIterator with a key-range filter and
//     pulls entries one at a time, never materializing more of the
//     dataset than the caller has actually asked to see
//
// Both consumers need the exact same shadowing rule (newest generation
// wins on a key collision) — extracting it here means that rule is
// defined and tested in exactly one place, rather than risking the two
// call sites drifting out of sync with each other over time.
package iterator

import (
	"bytes"
	"container/heap"

	"github.com/sujanuj/lsmdb/internal/sstable"
)

// source is one input stream: a sorted slice of entries plus a
// generation number, where HIGHER means newer.
type source struct {
	entries    []sstable.Entry
	pos        int
	generation int
}

func (s *source) peek() (sstable.Entry, bool) {
	if s.pos >= len(s.entries) {
		return sstable.Entry{}, false
	}
	return s.entries[s.pos], true
}

func (s *source) advance() {
	s.pos++
}

type heapItem struct {
	sourceIdx int
}

type mergeHeap struct {
	items   []heapItem
	sources []*source
}

func (h *mergeHeap) Len() int { return len(h.items) }

func (h *mergeHeap) Less(i, j int) bool {
	ei, _ := h.sources[h.items[i].sourceIdx].peek()
	ej, _ := h.sources[h.items[j].sourceIdx].peek()
	cmp := bytes.Compare(ei.Key, ej.Key)
	if cmp != 0 {
		return cmp < 0
	}
	// Same key from two different sources: order the NEWER generation
	// first, so callers processing a run of equal keys naturally see the
	// newest one first and can discard the rest.
	return h.sources[h.items[i].sourceIdx].generation > h.sources[h.items[j].sourceIdx].generation
}

func (h *mergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *mergeHeap) Push(x any) { h.items = append(h.items, x.(heapItem)) }

func (h *mergeHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

// MergeIterator lazily merges N sorted entry streams (oldest generation
// first, matching the convention db.DB uses for its sstables slice),
// yielding exactly one entry per distinct key — the one from the highest
// generation — via repeated calls to Next. No more of the underlying
// streams is consumed than is needed to produce each entry, which is
// what makes this safe to use for a range scan that might stop early.
type MergeIterator struct {
	sources []*source
	h       *mergeHeap
}

// NewMergeIterator builds an iterator over the given streams. Each inner
// slice MUST already be sorted by key — same requirement sstable.Write
// and the memtable's All() already guarantee, so callers passing
// memtable.Entry-converted or sstable.Entry slices from those sources
// satisfy this automatically.
func NewMergeIterator(generationsOldestFirst [][]sstable.Entry) *MergeIterator {
	sources := make([]*source, len(generationsOldestFirst))
	for i, entries := range generationsOldestFirst {
		sources[i] = &source{entries: entries, generation: i}
	}

	h := &mergeHeap{sources: sources}
	for i, s := range sources {
		if _, ok := s.peek(); ok {
			heap.Push(h, heapItem{sourceIdx: i})
		}
	}

	return &MergeIterator{sources: sources, h: h}
}

// Next returns the next entry in globally sorted key order, resolving any
// same-key collision across sources by keeping only the highest-generation
// entry and silently consuming (discarding) every shadowed duplicate for
// that key from the other sources. Returns ok=false once every source is
// exhausted.
func (m *MergeIterator) Next() (entry sstable.Entry, ok bool) {
	if m.h.Len() == 0 {
		return sstable.Entry{}, false
	}

	top := m.h.items[0]
	winningEntry, _ := m.sources[top.sourceIdx].peek()
	winningKey := winningEntry.Key

	first := true
	for m.h.Len() > 0 {
		cur := m.h.items[0]
		curEntry, ok := m.sources[cur.sourceIdx].peek()
		if !ok || !bytes.Equal(curEntry.Key, winningKey) {
			break
		}
		if first {
			winningEntry = curEntry
			first = false
		}
		heap.Pop(m.h)
		m.sources[cur.sourceIdx].advance()
		if _, ok := m.sources[cur.sourceIdx].peek(); ok {
			heap.Push(m.h, heapItem{sourceIdx: cur.sourceIdx})
		}
	}

	return winningEntry, true
}
