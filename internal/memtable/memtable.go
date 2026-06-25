package memtable

import "time"

// approxNodeOverheadBytes estimates the fixed per-entry overhead beyond
// the raw key/value bytes: pointer slice headers, the deleted flag, etc.
// This doesn't need to be exact — it only needs to be a reasonable,
// consistent estimate so the flush threshold triggers at roughly the
// right memory size rather than wildly over or under it.
const approxNodeOverheadBytes = 48

// Memtable is the in-memory write buffer sitting in front of the SSTable
// layer. It wraps a SkipList and adds the one thing the skip list itself
// doesn't know how to do: track its own approximate size in bytes, so the
// engine knows when to flush to disk and start a new, empty memtable.
type Memtable struct {
	list      *SkipList
	sizeBytes int
}

// New creates an empty memtable. The seed is plumbed through to the
// underlying skip list's level-generation PRNG.
func New(seed uint64) *Memtable {
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	return &Memtable{list: NewSkipList(seed)}
}

// Put inserts or overwrites a key's value.
func (m *Memtable) Put(key, value []byte) {
	// Note: this is a simple approximation that treats every Put as
	// adding new bytes, even when it's actually an overwrite of an
	// existing key (in which case the skip list reuses the node rather
	// than growing). A precise accounting would need the skip list to
	// report whether an insert was new-vs-overwrite and the size delta
	// of the value specifically. For a flush-threshold heuristic, slight
	// overestimation on overwrite-heavy workloads is an acceptable
	// trade for keeping this layer simple — worth flagging explicitly
	// as a known approximation rather than silently shipping it as if
	// it were exact.
	m.sizeBytes += len(key) + len(value) + approxNodeOverheadBytes
	m.list.Put(key, value)
}

// Delete tombstones a key. See SkipList.Delete for why this doesn't
// physically remove anything yet.
func (m *Memtable) Delete(key []byte) {
	m.sizeBytes += len(key) + approxNodeOverheadBytes
	m.list.Delete(key)
}

// Get returns the value for key. found is false both when the key was
// never written and when it was deleted (the two are indistinguishable
// from this layer's API — callers needing to tell them apart, e.g. to
// stop searching older SSTables on a confirmed tombstone, should use
// GetWithTombstone instead).
func (m *Memtable) Get(key []byte) (value []byte, found bool) {
	return m.list.Get(key)
}

// GetWithTombstone returns the full picture for a key: whether it exists
// at all in this memtable, and if so, whether that existence is a live
// value or a tombstone. The read path above the memtable needs this
// distinction — a tombstone here means "stop, this key is deleted,"
// while "not found at all" means "keep checking older SSTables."
func (m *Memtable) GetWithTombstone(key []byte) (value []byte, existsHere bool, isDeleted bool) {
	return m.list.GetWithTombstone(key)
}

// SizeBytes returns the approximate memory footprint of all writes so
// far. Compare this against a configured threshold (commonly 4MB-64MB in
// real engines) to decide when to flush.
func (m *Memtable) SizeBytes() int {
	return m.sizeBytes
}

// Len returns the number of live (non-tombstoned) entries.
func (m *Memtable) Len() int {
	return m.list.Len()
}

// All returns every entry in sorted key order, including tombstones. Used
// by the (future) flush path to write a new SSTable.
func (m *Memtable) All() []Entry {
	return m.list.All()
}
