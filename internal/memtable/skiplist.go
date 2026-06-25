// Package memtable implements the in-memory write buffer for the LSM
// engine: a sorted skip list that holds the most recently written keys
// before they're flushed to an immutable SSTable on disk.
//
// Why a skip list and not a balanced tree (red-black, AVL): a skip list
// insert only ever rewires a small, predictable set of pointers at each
// level near the insertion point. A tree insert can trigger rotations
// that cascade up toward the root and touch nodes far from the insertion
// point. That locality makes skip lists much easier to reason about under
// concurrent access — this is the actual reason LevelDB, RocksDB, and
// most production LSM engines use skip lists for their memtable, not a
// simplification made for this project.
package memtable

import (
	"bytes"
	"sync"
)

const (
	// maxLevel caps how many levels a node can be promoted to. With
	// p = 0.25, the expected number of nodes that reach level L is
	// n * 0.25^L, so 16 levels comfortably covers any realistic memtable
	// size (0.25^16 is astronomically small) without wasting memory on
	// levels that will basically never be used.
	maxLevel = 16
	// p is the probability a node promoted to level L is also promoted
	// to level L+1. 0.25 is RocksDB's actual default — it trades a
	// slightly higher constant factor on search (you check more nodes
	// per level) for fewer levels overall and less pointer overhead per
	// node, compared to the more "textbook" p = 0.5.
	p = 0.25
)

// node is a single entry in the skip list. next[i] points to the next
// node that also exists at level i; next[0] is the plain sorted linked
// list that every node participates in.
type node struct {
	key     []byte
	value   []byte
	deleted bool // tombstone marker — see Delete
	next    []*node
}

// SkipList is a sorted, in-memory key/value structure safe for concurrent
// use by multiple readers and a single writer at a time (enforced via an
// RWMutex). It is the building block for Memtable.
type SkipList struct {
	mu     sync.RWMutex
	head   *node // sentinel; head.key is never compared against
	rng    *xorshift64
	level  int // highest level currently in use across any real node
	length int // number of live (non-deleted) entries, for size estimation
}

// NewSkipList creates an empty skip list. seed controls the PRNG used for
// level promotion — pass a fixed seed in tests for reproducible level
// structure, or a value derived from time for production use.
func NewSkipList(seed uint64) *SkipList {
	return &SkipList{
		head:  &node{next: make([]*node, maxLevel)},
		rng:   newXorshift64(seed),
		level: 1,
	}
}

// randomLevel decides how many levels a new node should be promoted to,
// using repeated coin flips with success probability p, capped at
// maxLevel. This is the classic skip-list promotion rule: level 1 is
// guaranteed, and each additional level is progressively less likely,
// which is what gives the structure its logarithmic search performance
// in expectation.
func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rng.float64() < p {
		lvl++
	}
	return lvl
}

// findPredecessors walks down from the top level to level 0, and at each
// level returns the last node whose key is strictly less than key. These
// are exactly the nodes whose next[] pointers need to be rewired on an
// insert, or that need to be checked when searching/deleting.
func (s *SkipList) findPredecessors(key []byte) []*node {
	preds := make([]*node, maxLevel)
	cur := s.head
	for lvl := s.level - 1; lvl >= 0; lvl-- {
		for cur.next[lvl] != nil && bytes.Compare(cur.next[lvl].key, key) < 0 {
			cur = cur.next[lvl]
		}
		preds[lvl] = cur
	}
	return preds
}

// Put inserts or overwrites the value for key. If key already exists,
// its node is updated in place (no new node, no level change) — this
// matters for memory accounting in the memtable layer above, since an
// overwrite doesn't add a new node to the structure.
func (s *SkipList) Put(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	preds := s.findPredecessors(key)

	// If the very next node at level 0 has an equal key, this is an
	// overwrite, not a fresh insert.
	if existing := preds[0].next[0]; existing != nil && bytes.Equal(existing.key, key) {
		wasDeleted := existing.deleted
		existing.value = append([]byte{}, value...)
		existing.deleted = false
		if wasDeleted {
			s.length++
		}
		return
	}

	lvl := s.randomLevel()
	if lvl > s.level {
		// Newly promoted to a level higher than any existing node — the
		// predecessor at those new levels is just the head sentinel,
		// since nothing else reaches that high yet.
		for i := s.level; i < lvl; i++ {
			preds[i] = s.head
		}
		s.level = lvl
	}

	newNode := &node{
		key:   append([]byte{}, key...),
		value: append([]byte{}, value...),
		next:  make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		newNode.next[i] = preds[i].next[i]
		preds[i].next[i] = newNode
	}
	s.length++
}

// Get returns the value for key and whether it was found. A tombstoned
// (deleted) key is reported as not found, even though its node is still
// physically present in the structure.
func (s *SkipList) Get(key []byte) (value []byte, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	preds := s.findPredecessors(key)
	candidate := preds[0].next[0]
	if candidate == nil || !bytes.Equal(candidate.key, key) {
		return nil, false
	}
	if candidate.deleted {
		return nil, false
	}
	return append([]byte{}, candidate.value...), true
}

// GetWithTombstone returns whether key exists in this skip list at all,
// and if so, whether it's a live value or a tombstone. This distinction
// matters one layer up: a tombstone means "stop searching, this key is
// deleted," while "doesn't exist here" means "keep checking older
// SSTables," and those two cases must not be conflated.
func (s *SkipList) GetWithTombstone(key []byte) (value []byte, existsHere bool, isDeleted bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	preds := s.findPredecessors(key)
	cur := preds[0].next[0]
	if cur == nil || !bytes.Equal(cur.key, key) {
		return nil, false, false
	}
	if cur.deleted {
		return nil, true, true
	}
	return append([]byte{}, cur.value...), true, false
}

// Delete marks key as tombstoned. It does NOT physically remove the node.
//
// This is deliberate and important: in an LSM tree, a delete has to be
// able to "win" over an older Put for the same key even if that older Put
// already lives in a different, already-flushed SSTable. If Delete just
// removed the in-memory node, a Get that fell through to an older SSTable
// would resurrect the supposedly-deleted value. Writing a tombstone that
// itself gets flushed (and survives compaction until it has provably
// shadowed every older version of the key) is the standard LSM technique
// for making deletes correct across the whole multi-level structure —
// physical removal only happens later, during compaction.
//
// If key doesn't exist yet, Delete still inserts a tombstone node for it.
// This matters for a database where a Put for this key might already be
// sitting in an older SSTable on disk that this memtable doesn't know
// about — the tombstone needs to exist so the read path finds it and
// correctly reports the key as deleted, rather than falling through to
// stale data in that older SSTable.
func (s *SkipList) Delete(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	preds := s.findPredecessors(key)
	if existing := preds[0].next[0]; existing != nil && bytes.Equal(existing.key, key) {
		if !existing.deleted {
			existing.deleted = true
			existing.value = nil
			s.length--
		}
		return
	}

	// No existing node for this key in this memtable — insert a fresh
	// tombstone so a later Get still finds "deleted" rather than falling
	// through to an older SSTable that might have a real value.
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			preds[i] = s.head
		}
		s.level = lvl
	}
	newNode := &node{
		key:     append([]byte{}, key...),
		deleted: true,
		next:    make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		newNode.next[i] = preds[i].next[i]
		preds[i].next[i] = newNode
	}
}

// Len returns the number of live (non-tombstoned) entries.
func (s *SkipList) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length
}

// Entry is a single key/value pair yielded during iteration, including
// tombstones (callers that care about live-vs-deleted should check
// Deleted explicitly — this is needed by the flush path, which must write
// tombstones to the SSTable too, not just live values).
type Entry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

// All returns every entry in ascending key order, including tombstones.
// This is the iteration the flush-to-SSTable path will use later — an
// SSTable's whole format depends on receiving entries already sorted,
// which the skip list provides for free.
func (s *SkipList) All() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var entries []Entry
	for cur := s.head.next[0]; cur != nil; cur = cur.next[0] {
		entries = append(entries, Entry{
			Key:     append([]byte{}, cur.key...),
			Value:   append([]byte{}, cur.value...),
			Deleted: cur.deleted,
		})
	}
	return entries
}
