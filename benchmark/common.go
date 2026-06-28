// Package benchmark compares lsmdb against SQLite (modernc.org/sqlite,
// pure Go, no CGo) and BoltDB (go.etcd.io/bbolt) across workload shapes
// chosen to tell an honest story, not just to make lsmdb look good:
// sequential writes, random writes, hot point reads, cold point reads
// (after compaction has pushed data into older SSTables), and range
// scans. Run with:
//
//	go test ./benchmark/... -bench=. -benchtime=1x -run=^$
//
// -run=^$ skips the package's regular tests (there are correctness
// tests living alongside, used to sanity-check each engine adapter
// before trusting its numbers) and runs only benchmarks.
package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
)

// engine is the minimal interface every comparison target implements,
// covering exactly the operations being benchmarked. Keeping this
// narrow (not trying to abstract over each engine's full API) is
// deliberate — the benchmarks should measure each engine doing the same
// logical work, not be bottlenecked by an abstraction layer none of the
// real engines actually has.
type engine interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	// Scan returns all live key/value pairs in [start, end], inclusive,
	// in sorted order.
	Scan(start, end []byte) ([][2][]byte, error)
	Close() error
}

// tempEngineDir creates a fresh temp directory for one engine instance
// and returns a cleanup func. Each engine gets its own directory even
// within the same benchmark run, so on-disk state from one engine can
// never leak into another's numbers.
func tempEngineDir(name string) (dir string, cleanup func()) {
	dir, err := os.MkdirTemp("", "lsmdb-bench-"+name+"-")
	if err != nil {
		panic(fmt.Sprintf("benchmark: failed to create temp dir: %v", err))
	}
	return dir, func() { os.RemoveAll(dir) }
}

func sequentialKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%010d", i))
}

// randomKey maps i through a simple deterministic permutation so
// "random write" benchmarks insert keys out of order without needing a
// real RNG (which would make benchmark runs non-reproducible run to
// run). A multiplicative hash over a power-of-two-ish range spreads
// sequential i values across the keyspace in an order that isn't itself
// sequential, while staying a pure, deterministic function of i.
func randomKey(i, total int) []byte {
	const prime = 2654435761 // Knuth's multiplicative hash constant
	scrambled := (i * prime) % total
	if scrambled < 0 {
		scrambled += total
	}
	return []byte(fmt.Sprintf("key-%010d", scrambled))
}

func valueOfSize(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte('a' + i%26)
	}
	return v
}

func dbPath(dir, filename string) string {
	return filepath.Join(dir, filename)
}
