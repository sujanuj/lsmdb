// Command lsmdb-cli is a small standalone program used to demonstrate WAL
// crash recovery live. It supports two modes:
//
//	lsmdb-cli write <wal-path> <count>
//	    Appends <count> sequential records (key-0000, key-0001, ...) to
//	    the WAL as fast as possible, printing progress. Meant to be
//	    killed mid-run with `kill -9` from another terminal.
//
//	lsmdb-cli replay <wal-path>
//	    Replays the WAL and prints how many complete records were
//	    recovered, plus the last key seen — proving that everything up
//	    to the kill point survived and nothing beyond it leaked in.
//
// Demo script:
//
//	go run ./cmd/lsmdb-cli write /tmp/demo.wal 5000000 &
//	sleep 0.5 && kill -9 %1
//	go run ./cmd/lsmdb-cli replay /tmp/demo.wal
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/sujanuj/lsmdb/internal/wal"
)

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	switch os.Args[1] {
	case "write":
		if len(os.Args) != 4 {
			usage()
		}
		runWrite(os.Args[2], os.Args[3])
	case "replay":
		runReplay(os.Args[2])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  lsmdb-cli write <wal-path> <count>")
	fmt.Fprintln(os.Stderr, "  lsmdb-cli replay <wal-path>")
	os.Exit(1)
}

func runWrite(path, countStr string) {
	count, err := strconv.Atoi(countStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid count %q: %v\n", countStr, err)
		os.Exit(1)
	}

	log, err := wal.Open(path, wal.SyncEveryWrite)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()

	for i := 0; i < count; i++ {
		rec := wal.Record{
			Op:    wal.OpPut,
			Key:   []byte(fmt.Sprintf("key-%08d", i)),
			Value: []byte(fmt.Sprintf("value-%08d-payload", i)),
		}
		if err := log.Append(rec); err != nil {
			fmt.Fprintf(os.Stderr, "append %d: %v\n", i, err)
			os.Exit(1)
		}
		if i%10000 == 0 {
			fmt.Printf("wrote %d records (pid=%d)\n", i, os.Getpid())
		}
	}
	fmt.Printf("done: wrote %d records\n", count)
}

func runReplay(path string) {
	n := 0
	var lastKey string
	err := wal.Replay(path, func(r wal.Record) error {
		n++
		lastKey = string(r.Key)
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("recovered %d complete records\n", n)
	if n > 0 {
		fmt.Printf("last recovered key: %s\n", lastKey)
	}
}
