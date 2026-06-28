package benchmark

import (
	"testing"
)

// valueSize is used throughout: 100 bytes is a reasonably realistic
// small-record size (think a user profile field, a session token, a
// short log line) — large enough that compression/serialization
// overhead is visible, small enough that benchmarks run in a
// practical amount of time.
const valueSize = 100

// newEngine constructs one of the three comparison targets by name,
// inside its own fresh temp directory. Centralizing this avoids each
// benchmark function repeating the same three-way switch.
func newEngine(b *testing.B, name string) (engine, func()) {
	b.Helper()
	dir, cleanupDir := tempEngineDir(name)

	var e engine
	var err error
	switch name {
	case "lsmdb":
		e, err = newLSMDBEngine(dir)
	case "bolt":
		e, err = newBoltEngine(dbPath(dir, "bench.bolt"))
	case "sqlite":
		e, err = newSQLiteEngine(dbPath(dir, "bench.sqlite"))
	default:
		b.Fatalf("unknown engine %q", name)
	}
	if err != nil {
		cleanupDir()
		b.Fatalf("newEngine(%q): %v", name, err)
	}

	cleanup := func() {
		e.Close()
		cleanupDir()
	}
	return e, cleanup
}

var engineNames = []string{"lsmdb", "bolt", "sqlite"}

// BenchmarkSequentialWrite inserts b.N keys in strictly increasing
// order. This is the workload shape where a B-tree's page-splitting
// behavior is at its cheapest (always appending to the rightmost leaf)
// and an LSM engine's append-only WAL + memtable is also at its
// cheapest — both engines should perform relatively well here, which
// makes this a useful BASELINE before looking at the random-write case.
func BenchmarkSequentialWrite(b *testing.B) {
	for _, name := range engineNames {
		b.Run(name, func(b *testing.B) {
			e, cleanup := newEngine(b, name)
			defer cleanup()
			value := valueOfSize(valueSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := e.Put(sequentialKey(i), value); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

// BenchmarkRandomWrite inserts b.N keys in a scrambled (but
// deterministic) order. This is the workload shape where LSM engines
// are EXPECTED to win: a B-tree must do random-access page reads/writes
// to insert into the middle of its structure, while lsmdb's memtable
// (an in-memory skip list) and append-only WAL never care what order
// keys arrive in — sequential disk I/O regardless of key order is the
// entire premise of the LSM design.
func BenchmarkRandomWrite(b *testing.B) {
	for _, name := range engineNames {
		b.Run(name, func(b *testing.B) {
			e, cleanup := newEngine(b, name)
			defer cleanup()
			value := valueOfSize(valueSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := randomKey(i, b.N+1)
				if err := e.Put(key, value); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}

// BenchmarkPointReadHot reads keys that were just written and are still
// in the most recently touched part of each engine's structure (lsmdb's
// memtable / a B-tree's cached top levels). All three engines should be
// fast here — this is the workload shape that's LEAST likely to show a
// meaningful difference, which is itself worth stating plainly rather
// than only reporting numbers that favor one engine.
func BenchmarkPointReadHot(b *testing.B) {
	const numKeys = 10000
	for _, name := range engineNames {
		b.Run(name, func(b *testing.B) {
			e, cleanup := newEngine(b, name)
			defer cleanup()
			value := valueOfSize(valueSize)
			for i := 0; i < numKeys; i++ {
				if err := e.Put(sequentialKey(i), value); err != nil {
					b.Fatalf("setup Put: %v", err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := sequentialKey(i % numKeys)
				if _, found, err := e.Get(key); err != nil {
					b.Fatalf("Get: %v", err)
				} else if !found {
					b.Fatalf("Get(%q): not found", key)
				}
			}
		})
	}
}

// BenchmarkPointReadCold writes enough data to force lsmdb through
// several real flush+compaction cycles (so the keys being read are
// sitting in actual on-disk SSTables, not the memtable), then reads
// keys scattered across that older data. This is the workload shape
// where a B-tree's O(log n) GUARANTEED single-path disk read can
// plausibly beat an LSM engine, which in the worst case has to consult
// the memtable and then multiple SSTables (even with bloom filters
// helping rule most of them out quickly). Reporting this honestly —
// rather than only benchmarking the hot-read case where lsmdb looks
// best — is the difference between a real performance analysis and a
// cherry-picked one.
func BenchmarkPointReadCold(b *testing.B) {
	// Large enough to span several flushes (FlushThresholdBytes is
	// 1MiB; numKeys * valueSize = ~1MB) and trigger at least one real
	// compaction round, while staying small enough that per-write-fsync
	// engines (BoltDB, lsmdb under SyncEveryWrite) finish setup in a
	// practical amount of time.
	const numKeys = 10000
	for _, name := range engineNames {
		b.Run(name, func(b *testing.B) {
			e, cleanup := newEngine(b, name)
			defer cleanup()
			value := valueOfSize(valueSize)
			for i := 0; i < numKeys; i++ {
				key := randomKey(i, numKeys+1) // scattered insertion order, like a real cold dataset
				if err := e.Put(key, value); err != nil {
					b.Fatalf("setup Put: %v", err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := randomKey(i%numKeys, numKeys+1)
				if _, found, err := e.Get(key); err != nil {
					b.Fatalf("Get: %v", err)
				} else if !found {
					b.Fatalf("Get(%q): not found", key)
				}
			}
		})
	}
}

// BenchmarkRangeScan writes numKeys sequential keys, then repeatedly
// scans a fixed-size window. This exercises each engine's sorted
// iteration path directly (lsmdb's MergeIterator, BoltDB's cursor,
// SQLite's range WHERE clause with its primary-key index).
func BenchmarkRangeScan(b *testing.B) {
	const numKeys = 20000
	const scanWindow = 100
	for _, name := range engineNames {
		b.Run(name, func(b *testing.B) {
			e, cleanup := newEngine(b, name)
			defer cleanup()
			value := valueOfSize(valueSize)
			for i := 0; i < numKeys; i++ {
				if err := e.Put(sequentialKey(i), value); err != nil {
					b.Fatalf("setup Put: %v", err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				startIdx := i % (numKeys - scanWindow)
				start := sequentialKey(startIdx)
				end := sequentialKey(startIdx + scanWindow)
				results, err := e.Scan(start, end)
				if err != nil {
					b.Fatalf("Scan: %v", err)
				}
				if len(results) != scanWindow+1 {
					b.Fatalf("Scan returned %d results, want %d", len(results), scanWindow+1)
				}
			}
		})
	}
}

// BenchmarkMixedWorkload interleaves writes and reads at a realistic
// ratio (90% reads, 10% writes — a common shape for many real services)
// rather than benchmarking each operation in total isolation. This is
// the workload shape closest to "what would actually happen in
// production," as opposed to the other benchmarks, which deliberately
// isolate one operation to make its cost legible.
func BenchmarkMixedWorkload(b *testing.B) {
	const numKeys = 5000
	for _, name := range engineNames {
		b.Run(name, func(b *testing.B) {
			e, cleanup := newEngine(b, name)
			defer cleanup()
			value := valueOfSize(valueSize)
			for i := 0; i < numKeys; i++ {
				if err := e.Put(sequentialKey(i), value); err != nil {
					b.Fatalf("setup Put: %v", err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%10 == 0 {
					key := randomKey(i, numKeys+1)
					if err := e.Put(key, value); err != nil {
						b.Fatalf("Put: %v", err)
					}
				} else {
					key := sequentialKey(i % numKeys)
					if _, _, err := e.Get(key); err != nil {
						b.Fatalf("Get: %v", err)
					}
				}
			}
		})
	}
}

// BenchmarkOverwriteHeavy repeatedly overwrites the SAME small set of
// keys — the workload shape that most directly demonstrates why
// compaction exists. lsmdb accumulates stale versions until compaction
// reclaims them; a B-tree overwrites the existing leaf entry in place
// with no equivalent buildup. This is intentionally adversarial to
// lsmdb's design, included for the same reason BenchmarkPointReadCold
// is: an honest comparison shows where the LSM design pays a real cost,
// not only where it wins.
func BenchmarkOverwriteHeavy(b *testing.B) {
	const numHotKeys = 100
	for _, name := range engineNames {
		b.Run(name, func(b *testing.B) {
			e, cleanup := newEngine(b, name)
			defer cleanup()
			value := valueOfSize(valueSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := sequentialKey(i % numHotKeys)
				if err := e.Put(key, value); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
		})
	}
}
