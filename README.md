# lsmdb

A log-structured merge-tree (LSM-tree) storage engine, built from scratch in Go.

This is a learning project, not a production database — the goal is to actually
implement the pieces that make engines like RocksDB, LevelDB, and Cassandra's storage
layer work, rather than depend on one.

## Status

**Phase 1: Write-ahead log + crash recovery — done**

- [x] WAL record format with per-record checksums (CRC32)
- [x] Append-only writer with a configurable fsync policy
- [x] Replay logic that recovers all complete records and discards a trailing
      partial record (the signature of a crash mid-write) without treating it as
      a fatal error
- [x] Unit tests for encode/decode round-trips and checksum corruption
- [x] Truncation tests — verify replay behaves correctly no matter which exact
      byte the file is cut off at
- [x] A live kill-9 demo (`cmd/lsmdb-cli`) for actually watching recovery happen

**Phase 2 (current): Memtable**

- [x] Skip list with a from-scratch xorshift64* PRNG for level promotion
- [x] Put / Get / Delete (tombstone, not physical removal) / sorted iteration
- [x] Concurrent-safe via RWMutex; verified race-free under concurrent
      readers + a writer with `go test -race`
- [x] Size-byte tracking on the Memtable wrapper, for a future flush threshold
- [x] Cross-checked against a reference `map[string]string` over 20,000 random
      put/delete operations
- [x] Integration test proving `wal.Replay` correctly rebuilds a memtable —
      including overwrites and tombstones surviving the replay exactly

**Planned:**

- [ ] SSTable file format + flush from memtable
- [ ] Bloom filters per SSTable
- [ ] Multi-level reads (memtable -> newest SSTable -> older SSTables)
- [ ] Compaction (size-tiered, to start)
- [ ] Range scans (k-way merge iterator)
- [ ] Benchmark suite vs. SQLite and BoltDB

## Why a skip list, not a balanced tree, for the memtable

A skip list insert only rewires a small, local set of pointers near the
insertion point at each level. A balanced tree insert can trigger rotations
that cascade upward toward the root and touch nodes far from where the new
key landed. That locality is what makes skip lists much easier to reason
about under concurrent access — it's the actual reason LevelDB, RocksDB, and
most production LSM engines use a skip list for the memtable, not a
simplification made for this project.

Level promotion uses a hand-rolled xorshift64* generator (`internal/memtable/rand.go`)
rather than `math/rand` — the level-promotion coin flip doesn't need
cryptographic randomness, just speed and reasonable uniformity, which is
exactly the use case xorshift generators are built for (it's the same
generator family Go's own runtime uses internally for map iteration order).
Each new node gets level 1 for free, then a 25% chance of promotion to each
subsequent level (RocksDB's actual default `p = 0.25`), capped at 16 levels.

## Why deletes are tombstones, not removals

A delete has to be able to outrank an older `Put` for the same key even when
that older `Put` already lives in a different, already-flushed SSTable. If
`Delete` just removed the node from the memtable, a `Get` that fell through to
an older SSTable would resurrect a value that was supposed to be gone.
Writing a tombstone — itself a real entry that gets flushed and eventually
removed during compaction once it's provably shadowed every older version of
the key — is the standard LSM technique for making deletes correct across the
whole multi-level structure.

## Why a WAL first

The WAL is the durability boundary: a write isn't safe until it's recorded here.
Everything upstream (the memtable, the query layer) can be wrong in lots of small
ways and just produce a bug. If the WAL is wrong, you lose data permanently and
silently, which is a different category of problem — so it's worth getting this
layer right, with real tests, before building anything on top of it.

### On-disk record format

```
[4 bytes checksum][1 byte op][4 bytes key_len][4 bytes value_len][key][value]
```

The checksum covers everything after itself. On replay, a checksum mismatch or a
record that claims to be longer than the bytes actually remaining in the file is
treated as "this is the tail of an interrupted write" — replay stops there and
returns everything decoded so far as valid. This is deliberate: a half-written
record at the end of the file is the *expected* shape of a crash, not corruption
to panic over.

### Durability vs. throughput

`Log.Append` can fsync after every write (`SyncEveryWrite`) or leave syncing to
the caller (`SyncManual`). Every write that returns successfully under
`SyncEveryWrite` is guaranteed durable across a power loss, at the cost of a
disk round-trip per write. `SyncManual` is faster but means a crash can lose
writes since the last explicit `Sync()` call. This is the same tradeoff every
real WAL implementation (Postgres, RocksDB, etc.) exposes as a tunable -- there's
no setting that's "correct" independent of the workload's tolerance for loss vs.
its need for throughput.

## Proving crash recovery actually works

Two kinds of test exist for this:

**Synthetic (`internal/wal/crash_test.go`)** -- construct a WAL file by hand with
a deliberately truncated final record (simulating every possible crash point,
byte by byte) and confirm replay recovers exactly the complete records and
nothing else.

**Live (`cmd/lsmdb-cli`)** -- an actual subprocess you can `kill -9` mid-write:

```bash
go build -o bin/lsmdb-cli ./cmd/lsmdb-cli

./bin/lsmdb-cli write /tmp/demo.wal 5000000 &
PID=$!
sleep 0.3
kill -9 $PID        # simulate a hard crash, no graceful shutdown

./bin/lsmdb-cli replay /tmp/demo.wal
# -> recovered N complete records
# -> last recovered key: key-0000XXXX
```

Run it a few times -- the exact record count recovered will vary by a few hundred
depending on timing, but it should always be a clean prefix of what was written,
never a record beyond what was actually durable, and never a panic on the
truncated tail.

## Running tests

```bash
go test ./...          # everything
go test ./... -race    # with the race detector — important for internal/memtable
```

## Project layout

```
lsmdb/
├── internal/
│   ├── wal/             <- write-ahead log (Phase 1)
│   ├── memtable/         <- skip list + memtable wrapper (Phase 2)
│   ├── sstable/             (next)
│   └── db/                  (later -- ties it all together)
└── cmd/
    └── lsmdb-cli/       <- demo/debug CLI
```
