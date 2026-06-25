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

**Phase 2: Memtable — done**

- [x] Skip list with a from-scratch xorshift64* PRNG for level promotion
- [x] Put / Get / Delete (tombstone, not physical removal) / sorted iteration
- [x] Concurrent-safe via RWMutex; verified race-free under concurrent
      readers + a writer with `go test -race`
- [x] Size-byte tracking on the Memtable wrapper, for a future flush threshold
- [x] Cross-checked against a reference `map[string]string` over 20,000 random
      put/delete operations
- [x] Integration test proving `wal.Replay` correctly rebuilds a memtable —
      including overwrites and tombstones surviving the replay exactly

**Phase 3: SSTable — done**

- [x] Chunked, gzip-compressed data blocks (64 entries per chunk)
- [x] Sparse index over chunks, loaded fully into memory on `Open`
- [x] Fixed-size footer with a magic number for fast, safe file validation
- [x] Point lookup (`Get`): binary search the index, decompress one chunk, scan it
- [x] Tombstones survive serialization and are distinguishable from
      "never existed" via `GetWithTombstone`
- [x] Full sorted iteration (`All`) for future compaction/range scans
- [x] End-to-end integration test: WAL write -> replay into a fresh memtable
      (simulating a crash restart) -> flush to SSTable -> fresh read of the
      file, confirming an overwrite and a delete both survive the entire chain
- [x] Measured real compression ratio on repetitive data (logged in tests:
      255KB of repetitive payload compresses to ~1.5% of original, including
      the bloom filter added in Phase 4)

**Phase 4 (current): Multi-level reads + bloom filters**

- [x] `internal/db`: a unified `DB` type wiring WAL + memtable + N SSTables
      into one `Get`/`Put`/`Delete` API
- [x] Multi-level `Get`: checks the memtable first, then every SSTable from
      newest to oldest, stopping at the first layer with ANY entry for the
      key (live or tombstone) — proven with a test where the same key has
      different values across 2 SSTables and an unflushed memtable write,
      and the newest always wins
- [x] Tombstone shadowing across SSTable generations: a delete recorded in
      a newer SSTable correctly hides a live value sitting in an older one
- [x] Restart correctness: closing and reopening a `DB` rediscovers existing
      SSTable files in the right age order and replays the WAL on top —
      proven with a real close/reopen test, not just a single long-lived
      instance
- [x] From-scratch bloom filter (`internal/bloom`) using double hashing
      (2 real hash computations simulating k≈7 hash functions via
      `h1 + i*h2`), sized from the standard m/n and k derivation for a
      target 1% false-positive rate (~9.6 bits/key)
- [x] Bloom filter measured directly: 50,000 keys added with zero false
      negatives; measured false-positive rate 0.47% against a 1% target
      across 100,000 disjoint probes
- [x] Bloom filter wired into the SSTable file format itself (written
      between the data chunks and the index, loaded on `Open`) and
      consulted before any chunk read — measured 98.95% of genuinely-absent
      key lookups resolved with zero disk reads
- [x] File format version bumped (magic number changed) since the on-disk
      layout changed — a v0 reader will correctly refuse to misparse a v1 file

**Planned:**

- [ ] Compaction (size-tiered, to start)
- [ ] Range scans (k-way merge iterator across memtable + multiple SSTables)
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

## SSTable file format

An SSTable is written once, in full, and never modified — compaction (Phase
4) produces brand-new files rather than editing existing ones. The format:

```
┌─────────────────────────────────────┐
│  Chunk 0 (gzip-compressed)           │  entries 0-63, compressed together
├─────────────────────────────────────┤
│  Chunk 1 (gzip-compressed)           │  entries 64-127
├─────────────────────────────────────┤
│  ...                                 │
├─────────────────────────────────────┤
│  Index block                         │  [firstKey][offset][length] per chunk
├─────────────────────────────────────┤
│  Footer (fixed 32 bytes)             │  indexOffset, indexLen, numEntries, magic
└─────────────────────────────────────┘
```

**Why chunk-level compression, not whole-file or per-entry:** compressing the
entire data block as one blob means a single point lookup has to decompress
the whole file before reading anything, which defeats the index. Compressing
each entry individually loses cross-entry redundancy (e.g. repeated key
prefixes) and adds large per-entry overhead. Grouping entries into
fixed-size chunks and compressing each one is the actual design RocksDB's
block-based table format uses — not a simplification invented here.

**Why a sparse index, one entry per chunk (64 entries):** a dense index
(one entry per key) would be roughly the same size as the data itself,
losing the entire point of keeping the index in memory. A sparse index over
chunks stays small enough to fully load on `Open`, and `Get` becomes: binary
search the in-memory index for the right chunk, decompress just that one
chunk, linear-scan within it.

**Why a fixed-size footer at the end:** this is what makes `Open` cheap —
seek to `filesize - footerSize`, read a small fixed struct, and it points
directly at the index without ever scanning the file. The magic number
inside the footer is a fast sanity check that the file is actually a valid
SSTable and not corrupted or unrelated data; `Open` rejects anything else.

**The compression/lookup tradeoff in practice:** every `Get` pays the cost of
decompressing one full chunk, even though it only needs one entry inside it.
Smaller chunks reduce wasted decompression per lookup but shrink the
compression window (less redundancy to exploit) and grow the index. 64
entries per chunk is a starting point, not a tuned constant — a real
benchmark (Phase 6) is what would justify moving it.

## Known simplifications in this phase

- `Reader.readChunk` re-opens the file on every call rather than holding it
  open across calls, and doesn't cache decompressed chunks. Both are real
  costs under load — see "what I'd change at scale" for why this is a
  deliberate scope cut for now rather than an oversight.

## Multi-level reads: how Get actually decides the answer

```
DB.Get(key):
  1. Check the memtable (newest data) via GetWithTombstone
     -> existsHere? return immediately (live value, or "not found" if tombstoned)
  2. Check SSTables from NEWEST to OLDEST, same GetWithTombstone check
     -> first one that existsHere wins, stop walking further
  3. Nothing anywhere had it -> truly not found
```

The reason every layer reports `(value, existsHere, isDeleted)` instead of
just `(value, found)` is exactly to make step 2 correct: a tombstone in a
newer SSTable must be able to shadow a real value sitting in an older one,
and the only way to know to *stop* at the tombstone (rather than keep
looking and find the stale value underneath) is for the tombstone to report
"yes, I exist here" even though it's logically a deletion.

SSTables are tracked oldest-first in a slice, because that's the order
flushes naturally produce — each new flush just appends. Reads walk the
slice backward (`for i := len-1; i >= 0; i--`) to check newest-first, which
needs zero extra bookkeeping beyond that ordering.

**A known durability gap, named explicitly:** flushing the memtable to a new
SSTable does NOT truncate the WAL afterward, even though the flushed data is
now redundant on disk. Truncating safely requires the flush and the
truncation to be atomic with respect to a crash in between — if the WAL
were truncated first and the process died before the SSTable write
finished, that data would be lost permanently. The current code accepts
ever-growing WAL files in exchange for correctness; a real fix needs a
manifest/checkpoint file recording exactly which WAL entries are safely
captured in which SSTable, written atomically alongside the flush. That's
exactly the kind of thing "what I'd change at scale" exists to call out.

## Bloom filters: the math and the measured result

For `n` expected keys and a target false-positive rate `p`, the optimal bit
array size is `m = -n·ln(p) / (ln 2)²` and the optimal number of hash
functions is `k = (m/n)·ln 2`. At `p = 1%`, this works out to roughly **9.6
bits per key** and **k ≈ 7** — both derived from the formulas in
`internal/bloom/bloom.go`, not hardcoded, so the filter sizes itself
correctly if the target rate changes.

Rather than implementing 7 separate hash functions, the filter uses
**double hashing** (Kirsch & Mitzenmacher, 2006): two real hash
computations (`h1`, `h2`, both FNV-1a) combine as `position_i = h1 + i·h2`
to simulate k independent-enough hash functions. This is the standard
technique production bloom filters use, not a shortcut taken for this
project.

The filter is built during the SSTable flush (every key, including
tombstones — a lookup for a deleted key must still find its tombstone, not
be incorrectly filtered out) and serialized into the file itself, between
the data chunks and the index block. `Open` loads it into memory alongside
the index, and every `Get` consults it before touching the data chunks at
all: if the filter says "definitely absent," the answer is returned
immediately, no chunk decompression, no disk read for that chunk.

**Measured, not just asserted:**
- 50,000 keys added to a filter sized for a 1% target rate: **zero false
  negatives** across all 50,000 (the one guarantee a bloom filter can never
  violate)
- 100,000 probes against guaranteed-absent keys: **0.47% measured
  false-positive rate** against the 1% target
- A real SSTable with 5,000 entries, probed with 2,000 genuinely-absent
  keys: **98.95% resolved by the bloom filter alone**, zero chunk reads

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
go test ./...          # everything (43 tests as of Phase 4)
go test ./... -race    # with the race detector
go test ./internal/bloom/... -v    # see the measured FP rate logged directly
go test ./internal/sstable/... -run TestBloom -v   # see bloom-skip rate on a real file
```

## Project layout

```
lsmdb/
├── internal/
│   ├── wal/             <- write-ahead log (Phase 1)
│   ├── memtable/         <- skip list + memtable wrapper (Phase 2)
│   ├── sstable/           <- chunked, compressed, indexed file format (Phase 3)
│   ├── bloom/             <- from-scratch bloom filter (Phase 4)
│   └── db/                <- multi-level Get/Put/Delete, ties everything together (Phase 4)
└── cmd/
    └── lsmdb-cli/       <- demo/debug CLI
```
