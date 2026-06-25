// Package compaction implements size-tiered compaction: merging multiple
// SSTable files into one, resolving shadowed (overwritten or deleted)
// keys along the way so disk space is actually reclaimed and the number
// of files a Get has to check stays bounded.
package compaction

import (
	"github.com/sujanuj/lsmdb/internal/iterator"
	"github.com/sujanuj/lsmdb/internal/sstable"
)

// Merge performs a k-way merge of multiple sorted entry streams, listed
// oldest generation first (matching the same convention db.DB uses for
// its sstables slice). For each distinct key, only the entry from the
// highest generation survives — every shadowed older entry for that key
// is discarded, which is the actual disk-space reclamation compaction
// exists for.
//
// The actual merge logic lives in internal/iterator.MergeIterator, which
// is shared with db.DB.Scan (Phase 6) — both need the exact same
// "highest generation wins on a key collision" rule, so it's defined and
// tested in exactly one place rather than risking the two call sites
// drifting apart. Merge itself is a thin wrapper: drain the iterator
// fully into a slice, optionally dropping tombstones along the way.
//
// dropObsoleteTombstones controls whether a tombstone that wins its key
// (i.e. is the newest entry for that key among all inputs) is itself
// dropped from the output entirely, rather than written through. This
// is only safe when the input set includes every SSTable older than the
// ones being merged — see ShouldDropTombstones in policy.go for the
// actual safety check callers must perform before passing true here.
// Merge itself does not re-verify that precondition; it does exactly
// what it's told, because the caller (db.DB's compaction path) is where
// that safety reasoning actually belongs.
func Merge(generationsOldestFirst [][]sstable.Entry, dropObsoleteTombstones bool) []sstable.Entry {
	it := iterator.NewMergeIterator(generationsOldestFirst)

	var out []sstable.Entry
	for {
		entry, ok := it.Next()
		if !ok {
			break
		}
		if entry.Op == sstable.OpDelete && dropObsoleteTombstones {
			continue // physically reclaim: this tombstone is provably obsolete
		}
		out = append(out, entry)
	}
	return out
}
