package benchmark

import (
	"fmt"
	"testing"
)

// TestEngineAdaptersCorrectness runs the exact same Put/Get/Scan
// sequence against all three engines and checks they agree. This exists
// specifically to catch a broken adapter BEFORE trusting its benchmark
// numbers — a Get that silently always returns "not found", for
// instance, would make an engine look unrealistically fast in a
// read benchmark, and that bug needs to be caught here, not discovered
// after citing a number in a README.
func TestEngineAdaptersCorrectness(t *testing.T) {
	for _, name := range engineNames {
		t.Run(name, func(t *testing.T) {
			dir, cleanup := tempEngineDir(name)
			defer cleanup()

			var e engine
			var err error
			switch name {
			case "lsmdb":
				e, err = newLSMDBEngine(dir)
			case "bolt":
				e, err = newBoltEngine(dbPath(dir, "test.bolt"))
			case "sqlite":
				e, err = newSQLiteEngine(dbPath(dir, "test.sqlite"))
			}
			if err != nil {
				t.Fatalf("new%sEngine: %v", name, err)
			}
			defer e.Close()

			// Basic put/get.
			if err := e.Put([]byte("a"), []byte("1")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			v, found, err := e.Get([]byte("a"))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !found || string(v) != "1" {
				t.Fatalf("Get(a) = %q, found=%v; want \"1\", true", v, found)
			}

			// Not-found case — this is the one a broken adapter most
			// commonly gets wrong (e.g. returning found=true with an
			// empty value instead of found=false).
			_, found, err = e.Get([]byte("never-written"))
			if err != nil {
				t.Fatalf("Get(never-written): %v", err)
			}
			if found {
				t.Fatal("Get(never-written) should report found=false")
			}

			// Overwrite.
			if err := e.Put([]byte("a"), []byte("2")); err != nil {
				t.Fatalf("Put overwrite: %v", err)
			}
			v, found, err = e.Get([]byte("a"))
			if err != nil || !found || string(v) != "2" {
				t.Fatalf("Get(a) after overwrite = %q, found=%v, err=%v; want \"2\", true, nil", v, found, err)
			}

			// Scan correctness: insert a known sorted set, confirm Scan
			// returns exactly the expected subset in order.
			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("key-%03d", i)
				if err := e.Put([]byte(key), []byte(fmt.Sprintf("val-%d", i))); err != nil {
					t.Fatalf("Put(%q): %v", key, err)
				}
			}
			results, err := e.Scan([]byte("key-005"), []byte("key-009"))
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(results) != 5 {
				t.Fatalf("Scan returned %d results, want 5", len(results))
			}
			for i, want := range []string{"key-005", "key-006", "key-007", "key-008", "key-009"} {
				if string(results[i][0]) != want {
					t.Errorf("result %d key = %q, want %q", i, results[i][0], want)
				}
			}
		})
	}
}
