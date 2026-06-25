package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// discoverSSTables scans dataDir for files matching the sstable-NNNNNN.sst
// naming pattern, returning their full paths in ascending sequence order
// (oldest first) and the highest sequence number found (0 if none exist).
//
// Sorting by the parsed integer rather than lexicographic filename order
// matters once sequence numbers exceed 6 digits in a long-running
// instance — %06d keeps this from being an issue for any realistic
// demo/test run, but parsing the number explicitly rather than relying on
// string sort is the version of this that doesn't quietly become wrong
// at scale.
func discoverSSTables(dataDir string) (paths []string, maxSeq int, err error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, 0, fmt.Errorf("db: read data dir %s: %w", dataDir, err)
	}

	type found struct {
		seq  int
		path string
	}
	var all []found

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var seq int
		n, scanErr := fmt.Sscanf(e.Name(), "sstable-%06d.sst", &seq)
		if scanErr != nil || n != 1 {
			continue // not an sstable file we recognize; ignore silently
		}
		all = append(all, found{seq: seq, path: filepath.Join(dataDir, e.Name())})
	}

	sort.Slice(all, func(i, j int) bool { return all[i].seq < all[j].seq })

	for _, f := range all {
		paths = append(paths, f.path)
		if f.seq > maxSeq {
			maxSeq = f.seq
		}
	}
	return paths, maxSeq, nil
}
