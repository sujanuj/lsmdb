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

**Phase 4: Multi-level reads + bloom filters — done**

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
      SSTable files in the right age order and replays the WAL on top
- [x] From-scratch bloom filter (`internal/bloom`) using double hashing
      (2 real hash computations simulating k≈7 hash functions via
      `h1 + i*h2`), sized from the standard m/n and k derivation for a
      target 1% false-positive rate (~9.6 bits/key)
- [x] Bloom filter measured directly: 50,000 keys added with zero false
      negatives; measured false-positive rate ~0.5% against a 1% target
      across 100,000 disjoint probes
- [x] Bloom filter wired into the SSTable file format itself and consulted
      before any chunk read — measured ~99% of genuinely-absent key
      lookups resolved with zero disk reads

**Phase 5: Compaction — done**

- [x] `internal/compaction`: a from-scratch k-way merge over N sorted
      entry streams using a min-heap, where ties on the same key are
      broken by generation (newer wins)
- [x] Size-tiered compaction policy (`PickCompactionCandidates`): groups
      SSTables into tiers by size (within a 2x ratio of each other) and
      triggers a compaction pass once a tier accumulates 4 files
- [x] A conservative, always-safe tombstone-dropping rule
      (`ShouldDropTombstones`): only physically discards a tombstone if
      the compaction reaches back to the oldest file in the database —
      makes the "delete undoes itself" bug class structurally impossible
- [x] Wired into `db.DB`: every flush checks the policy and performs a
      real compaction pass automatically, end to end on real files on disk
- [x] Cross-checked against a hand-written reference implementation across
      thousands of overlapping keys with deletes mixed in
- [x] A real stress test: 30 rounds x 50 keys with overwrites and periodic
      deletes, collapsing from 30 flushes down to single digits of files,
      every key verified against an independent reference map

**Phase 6: Range scans — done**

- [x] `internal/iterator`: the k-way merge heap logic extracted out of
      `compaction.Merge` into a standalone, genuinely lazy `MergeIterator`
- [x] `compaction.Merge` refactored to be a thin wrapper around
      `MergeIterator` — re-verified with every existing Phase 5 test still
      passing unchanged after the refactor
- [x] `db.DB.Scan(start, end)`: a `ScanIterator` over every live key in
      `[start, end]` (inclusive both ends), built from the memtable plus
      every SSTable as one merge

**Phase 7 (current): Benchmarks vs. SQLite and BoltDB**

- [x] `benchmark/`: head-to-head Go `testing.B` benchmarks against real,
      external engines — `github.com/mattn/go-sqlite3` (SQLite, configured
      with `journal_mode=WAL` to compare against a realistic production
      setup, not SQLite's slower legacy default) and `go.etcd.io/bbolt`
      (BoltDB — a B+tree KV store with no SQL layer in the way, which
      isolates "B-tree vs LSM-tree" more directly than the SQLite
      comparison does)
- [x] Each engine's adapter has its own correctness test
      (`TestEngineAdaptersCorrectness`), run and verified BEFORE trusting
      any benchmark number from it — catching a broken adapter early
      matters more than getting numbers fast
- [x] Seven workload shapes, chosen specifically to tell an honest story
      rather than only the ones that flatter lsmdb: sequential writes,
      random writes, hot point reads, cold point reads (after real
      flush/compaction), range scans, a realistic 90/10 mixed
      read/write workload, and an overwrite-heavy workload that's
      deliberately adversarial to the LSM design
- [x] **A real bug found and fixed by the benchmark itself:** the first
      range-scan run showed lsmdb roughly 300x slower than BoltDB — far
      too large a gap to be ordinary LSM overhead. Root cause: `DB.Scan`
      was calling `sstable.Reader.All()`, which decompresses every chunk
      in a file regardless of the requested range. Fixed by adding
      `Reader.RangeScan`, which uses the existing sparse index to skip
      chunks entirely outside `[start, end]`. Result: **a 55x improvement**
      (19.7ms -> 367µs per scan on the same benchmark), with 9 new
      correctness tests added for the fix before trusting the new numbers
- [x] Full results table and honest analysis below — including the cases
      where lsmdb loses, and why

**What's next:** This completes the planned phases. See "Ideas for
further work" at the end for what a Phase 8+ could look like.

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

## Compaction: why it exists and how it's safe

Without compaction, every overwrite and every delete just adds another
entry to a new SSTable, while the stale old entry sits untouched in an
older file forever. `DB.Get` already knows to skip stale data (newest
layer wins), but the disk space and the file count both grow without
bound. Compaction is the process that periodically rewrites a set of
SSTables into one, physically discarding anything that's been shadowed.

### The merge: a k-way merge with generation-based shadowing

`internal/compaction.Merge` takes N sorted entry streams (oldest first,
same convention `db.DB.sstables` uses) and produces one sorted stream,
using a min-heap (`container/heap`) to always emit the globally smallest
key next. When the same key appears in multiple input streams, the
heap's tie-break rule (higher generation — i.e. newer — sorts first)
guarantees the newest entry for that key is the one that survives; every
older duplicate for that key is read and then discarded. This is the
exact same "newest wins" rule `DB.Get` uses for shadowing, just applied
once during compaction instead of being re-evaluated on every single
read afterward.

### The dangerous bug this is built to avoid

A tombstone existing in a newer file must be able to shadow a real value
sitting in an older file. If a tombstone gets *physically dropped*
during a compaction that doesn't actually include every older copy of
that key, the dropped tombstone stops shadowing anything — and on the
next read, an even-older value for that key can resurface as if it had
never been deleted. This is the textbook LSM compaction bug, and it's
exactly why `dropObsoleteTombstones` in `Merge` is a parameter the merge
itself doesn't decide on its own — the safety reasoning lives one level
up, in the policy.

### The safety rule: conservative by design

`ShouldDropTombstones` only allows dropping tombstones when the
compaction's candidate set reaches all the way back to age index 0 — the
oldest file in the whole database. If there's any older file *outside*
this compaction, tombstones are kept, full stop, even if it's overwhelmingly
likely none of them actually still need to shadow anything in that older
file. This trades a small amount of unnecessary tombstone longevity for
making the dangerous failure mode (a delete silently undoing itself)
structurally impossible to trigger — checking real per-key safety against
every older file's actual contents would require reading those files on
every compaction decision, which defeats the entire point of compaction
being bounded work.

### Size-tiered policy

```
PickCompactionCandidates groups SSTables into tiers by size (within a 2x
ratio of each other) and returns the first tier that has reached
SizeTierThreshold (4) files. A compacted file naturally lands in a larger
tier, where it waits for peers of ITS size to accumulate before compacting
again — this is what makes file count grow logarithmically rather than
linearly with total writes, even though each individual compaction pass
is bounded work.
```

This mirrors Cassandra's Size-Tiered Compaction Strategy at a conceptual
level: lots of small files compact into fewer medium files, which
eventually compact into fewer large files, rather than re-merging the
entire dataset on every single flush (which would make every write pay
for the full size of the database, an obviously bad tradeoff at scale).

## Range scans: reusing compaction's merge, properly this time

A range scan ("give me every live key between X and Y, in order") is
structurally the same problem compaction's merge already solves — many
sorted sources, same-key collisions resolved by "newest generation wins"
— with three differences: it includes the **memtable** (compaction never
touches the memtable, only flushed SSTables), it filters to a key range,
and it must **never** drop tombstones (a scan is a read; physically
discarding data is compaction's job, under much more careful safety
rules).

Rather than duplicating the heap logic in a second place, the original
`compaction.Merge` heap implementation was extracted into
`internal/iterator.MergeIterator` — a genuinely lazy, pull-based
iterator. `compaction.Merge` is now a thin wrapper that drains the
iterator into a slice (with optional tombstone-dropping); `db.DB.Scan`
wraps the same iterator with a range filter and tombstone-skipping,
pulling one entry at a time instead of materializing the whole merge.

**This was a real refactor of working, already-tested code, treated
accordingly:** every one of Phase 5's compaction tests was re-run
immediately after the extraction and confirmed to still pass unchanged —
that's what actually justifies calling the refactor "behavior-preserving"
rather than just hoping it is.

```go
type ScanIterator struct { /* wraps a MergeIterator + range bounds */ }

func (s *ScanIterator) Next() (key, value []byte, ok bool) {
    for {
        entry, ok := s.inner.Next()
        if !ok { return nil, nil, false }
        if pastEnd(entry.Key)    { return nil, nil, false } // sorted output -> safe to stop entirely
        if beforeStart(entry.Key) { continue }               // keep pulling
        if entry.Op == OpDelete   { continue }                // skip, never surface, never drop on disk
        return entry.Key, entry.Value, true
    }
}
```

Because the underlying merge always yields keys in ascending order, the
moment a key is past the end of the range, *every subsequent key* will
be too — so `Next` can stop the whole scan right there instead of just
skipping one entry, which keeps a bounded scan over a huge dataset cheap
even when the dataset itself is far larger than the requested range.

**Range convention:** `[start, end]`, inclusive on both ends. `nil` for
either bound means unbounded in that direction — `Scan(nil, nil)` is a
full scan.

**A named limitation, not a hidden one:** `Scan` takes a snapshot of the
memtable and SSTable state at the moment it's called. A write that
happens while a caller is still iterating is not reflected in that scan.
A live-updating scan would need either copy-on-write memtable semantics
or holding the engine's lock for the scan's entire duration (which would
block every other write for as long as the scan takes) — both real
options, neither implemented here, and worth being explicit about rather
than letting a caller discover it by surprise.

### What's deliberately NOT built in compaction or scans

- **Background/async compaction.** `db.DB` runs exactly one compaction
  pass synchronously inside the `Flush`/`Put`/`Delete` call that triggers
  it, holding the same lock as every other operation. A production engine
  runs compaction on a background goroutine so writes aren't blocked
  waiting for a merge to finish — that's a real, deliberate scope cut
  named here rather than discovered by surprise.
- **Per-key tombstone safety analysis.** As described above — the
  conservative whole-tier rule is intentional, not a missing optimization.
- **Leveled compaction** (RocksDB/LevelDB's default strategy, which
  organizes files into non-overlapping key-range levels rather than
  size tiers). Size-tiered was chosen for this phase because it's
  conceptually simpler to reason about and test correctly; leveled
  compaction's main advantage is bounding the worst-case number of files
  a single key could be spread across, at the cost of more total bytes
  rewritten over time (higher write amplification, lower space amplification —
  a real tradeoff between the two strategies that's worth being able to name).
- **Live-updating scans.** As described above — `Scan` is a snapshot, not
  a subscription.

## Benchmarks: lsmdb vs. SQLite vs. BoltDB

`benchmark/` runs real Go `testing.B` benchmarks against two actual,
external embedded-database engines:

- **SQLite** via `github.com/mattn/go-sqlite3`, configured with
  `journal_mode=WAL` and `synchronous=NORMAL` — the realistic production
  configuration, not SQLite's slower legacy rollback-journal default.
  Comparing against a deliberately weakened baseline would make the
  comparison meaningless.
- **BoltDB** via `go.etcd.io/bbolt` — a B+tree key-value store with no SQL
  layer in the way, which isolates "B-tree vs. LSM-tree" more directly
  than the SQLite comparison does (SQLite's B-tree is wrapped in SQL
  parsing, query planning, and a row-based table format).

Every adapter has its own correctness test (`TestEngineAdaptersCorrectness`)
that runs and must pass before any benchmark number from that engine is
trusted — a broken adapter (e.g. a `Get` that silently always returns
"not found") would make an engine look unrealistically fast, and that
class of bug needs to be caught here, not discovered after a number gets
quoted somewhere.

### Results

100-byte values, 1000 iterations per benchmark, single-threaded, on the
machine the suite was actually run on (numbers will vary by hardware —
the *relative* shape and the *reasons* behind it are what's meant to
transfer, not the absolute nanosecond counts):

| Workload | lsmdb | BoltDB | SQLite | Winner |
|---|---|---|---|---|
| Sequential write | 577µs | 1227µs | 67µs | SQLite |
| Random write | 597µs | 1235µs | 73µs | SQLite |
| Point read (hot) | 80µs | 4.6µs | 37µs | BoltDB |
| Point read (cold, post-compaction) | 74µs | 4.6µs | 26µs | BoltDB |
| Range scan (100-key window) | 368µs | 26µs | 168µs | BoltDB |
| Mixed 90% read / 10% write | 61µs | 136µs | 32µs | SQLite |
| Overwrite-heavy (100 hot keys) | 642µs | 1265µs | 25µs | SQLite |

### Reading these results honestly

**BoltDB wins every pure-read shape, by a lot.** This is the single most
important and explainable result in the table, not something to talk
around: BoltDB memory-maps its entire file. A hot or cold read is often
just a pointer dereference into that mmap — no syscall, no parsing, no
lock contention beyond a read transaction handle. lsmdb's `Get` takes an
`RWMutex.RLock`, walks a skip list (several `bytes.Compare` calls per
level), and — even for genuinely in-memory data — pays real CPU cost a
direct memory-mapped pointer read simply doesn't have. This isn't a bug;
it's the actual, structural cost of choosing a design optimized for
write throughput over one optimized for read latency.

**SQLite wins every write-heavy shape, including the ones LSM-tree
theory says should favor lsmdb.** At this single-threaded, small-batch
scale, every engine's *durability* setting dominates the result far more
than the underlying data structure does: lsmdb's `SyncEveryWrite` policy
(see the WAL section above) and BoltDB's per-transaction fsync both pay
a real disk round-trip on every single write, while SQLite's
`synchronous=NORMAL` is deliberately less strict. This is a genuinely
useful finding about *this specific configuration*, not a refutation of
LSM-tree write theory — the random-vs-sequential write gap that LSM
design is supposed to produce only shows up at a scale and concurrency
level where avoiding random disk seeks actually matters more than the
per-write fsync cost. Re-running this with `SyncManual` and batched
syncing (the production configuration real LSM engines use under load)
would be the natural next experiment — noted in "what I'd change at
scale," not pretended away here.

**The range-scan story is really two results, and the gap between them
is the interesting part.** lsmdb's range scan was originally ~300x
slower than BoltDB's — far too large a gap to be normal LSM overhead.
Investigating it surfaced a real bug: `DB.Scan` was decompressing every
chunk of every SSTable regardless of the requested range, because it
called `Reader.All()` instead of using the existing sparse index to skip
irrelevant chunks. Adding `Reader.RangeScan` (which uses
`findChunk`/binary search on the index, the exact mechanism `Get` already
used) fixed this — **a 55x improvement**, from 19.7ms down to 367µs per
scan on the identical benchmark. The corrected gap against BoltDB (368µs
vs. 26µs) is now explainable by the same mmap-vs-skip-list reasoning as
the point-read results, not by an algorithmic bug. This sequence — write
a benchmark, get a suspicious number, investigate instead of reporting
it, find and fix a real inefficiency, re-measure — is the actual value a
benchmark suite provides beyond bragging rights.

**Where lsmdb's design should matter more than it does here:** every one
of these benchmarks runs single-threaded against a dataset small enough
to fit comfortably in memory and disk cache. LSM-tree's real advantages
— turning random writes into sequential disk I/O, supporting much higher
write throughput under concurrent load, and a compaction story that
matters at datasets far larger than RAM — are exactly the conditions
this benchmark suite, run on a single machine against small datasets,
doesn't stress. A fairer head-to-head would run with concurrent writers,
fsync disabled or batched (matching how these engines are actually
deployed at scale), and a dataset large enough to force real disk I/O
rather than page-cache hits for every engine.

### Running the benchmarks

```bash
# Correctness first — must pass before trusting any number below
go test ./benchmark/... -run TestEngineAdaptersCorrectness -v

# Full benchmark suite
go test ./benchmark/... -bench=. -benchtime=1000x -run=^$ -v

# One workload at a time
go test ./benchmark/... -bench=BenchmarkRangeScan -benchtime=1000x -run=^$ -v
```

`-run=^$` skips the package's regular tests so only benchmarks execute.
`-benchtime=1000x` runs exactly 1000 iterations rather than Go's default
adaptive time-based count, which keeps results comparable run to run
since BoltDB and SQLite setup phases (writing many keys with per-write
fsync) can otherwise dominate wall-clock time at very high iteration
counts.

## Ideas for further work

This project deliberately stopped at 7 phases — long enough to cover the
core mechanisms of a real LSM engine end to end, short enough to keep
every phase genuinely well-tested rather than rushing breadth over
depth. If continuing:

- **Async/background compaction**, so writes never block on a merge
- **Leveled compaction** as an alternative strategy to size-tiered, with
  a benchmark comparing write/space amplification between the two
- **Configurable WAL sync batching** (`SyncManual` + a timer), and
  re-running the write benchmarks above to see the random-vs-sequential
  write gap LSM theory predicts actually appear
- **Concurrent benchmark variants** (multiple goroutines writing/reading
  simultaneously), since single-threaded benchmarks understate where
  lsmdb's lock-per-operation design either helps or hurts relative to
  BoltDB's single-writer/many-readers model
- **A chunk cache** in `sstable.Reader`, to avoid re-decompressing the
  same hot chunk on every repeated read — directly informed by how slow
  the uncached cold-read benchmark above turned out to be

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
go test ./internal/...          # everything (94 tests as of Phase 7)
go test ./internal/... -race    # with the race detector
go test ./benchmark/... -run TestEngineAdaptersCorrectness -v   # sanity-check engine adapters
go test ./benchmark/... -bench=. -benchtime=1000x -run=^$ -v    # the actual benchmark suite
```

## Project layout

```
lsmdb/
├── internal/
│   ├── wal/             <- write-ahead log (Phase 1)
│   ├── memtable/         <- skip list + memtable wrapper (Phase 2)
│   ├── sstable/           <- chunked, compressed, indexed file format,
│   │                          incl. index-aware RangeScan (Phase 3, 6)
│   ├── bloom/             <- from-scratch bloom filter (Phase 4)
│   ├── iterator/          <- shared lazy k-way merge iterator (Phase 6)
│   ├── compaction/        <- size-tiered policy + Merge (Phase 5)
│   └── db/                <- Get/Put/Delete/Scan + compaction trigger
├── benchmark/             <- head-to-head vs. SQLite and BoltDB (Phase 7)
└── cmd/
    └── lsmdb-cli/       <- demo/debug CLI
```
