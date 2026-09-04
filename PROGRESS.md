# FORGE — Progress Log

## Current state

**Phase 12 complete. Continuing sequentially through Phase 14 per the master
continue-through-completion directive — see "Next task" at the end of this
file for exactly where to resume.**

## Completed work

- Analyzed the project and produced the full roadmap:
  [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and [docs/DESIGN.md](docs/DESIGN.md).
- Decided: Maven multi-module layout (`forge-common`, `forge-storage`,
  `forge-server`, `forge-client`, `forge-cluster`, `forge-bench`).
- Decided: replication model will start as leader-follower per partition
  (not leaderless/quorum-based) — rationale in DESIGN.md §6.
- Decided: failure detection starts as fixed-timeout heartbeats, not
  phi-accrual or gossip (SWIM), to keep Phase 8 tractable.
- Decided: full consensus (Raft/Paxos) is a gated stretch phase (14★), not a
  prerequisite for a working replicated system — gate criteria in DESIGN.md §9.
- **Revision after user review:**
  - Added explicit consistency & durability guarantees, phased and tied to
    a single load-bearing invariant (WAL order at the leader) — DESIGN.md §2.
  - Inserted a dedicated **Fault injection / chaos testing** phase (11),
    positioned after replication (9) and failure detection (8) so it has
    real distributed behavior to break; renumbered benchmarking suite to 12,
    compaction to 13★, automated failover/consensus to 14★.
  - Defined observable metrics (throughput, p50/p95/p99 latency, storage
    overhead, recovery time, replication lag, failure rate) with precise
    definitions and the phase each becomes measurable — DESIGN.md §3,
    mirrored in BENCHMARKS.md.
  - Defined 11 concrete experiments (E1–E11) that will produce the actual
    BENCHMARKS.md data, each tied to a phase and a swept variable —
    DESIGN.md §4.
  - Added per-feature contracts (failure assumptions, correctness invariant,
    consistency guarantee, recovery behavior) for every distributed feature:
    WAL, storage engine, concurrency control, partitioning, membership &
    failure detection, replication, recovery — DESIGN.md §11.
  - Reinforced the "core mechanisms built by us" constraint into a binding
    list of what's implemented vs. what's given infrastructure, and made the
    Raft/Paxos gate explicit and criteria-based rather than a bare stretch
    label — DESIGN.md §9, cross-linked from ARCHITECTURE.md's non-goals.

## Phase 0: project setup (complete, 2026-09-01)

- Toolchain installed on this machine (was missing entirely): Homebrew
  `openjdk` (26.0.2.1) and `maven` (3.9.16). Project targets
  `maven.compiler.release=21` — the LTS baseline — so it compiles/runs on
  any JDK 21+, not tied to the specific 26.0.2.1 currently installed.
- Created a Maven **multi-module** project at `~/forge`: a parent `pom.xml`
  (packaging `pom`, groupId `com.forge`, version `0.1.0-SNAPSHOT`) aggregating
  six modules, matching ARCHITECTURE.md §4 exactly:
  `forge-common`, `forge-storage`, `forge-server`, `forge-client`,
  `forge-cluster`, `forge-bench`.
- Parent `pom.xml` uses `dependencyManagement` to pin versions (not to pull
  dependencies into every module automatically): `junit-bom` 5.12.2,
  `slf4j-api` 2.0.17, `logback-classic` 1.5.18; `pluginManagement` pins
  `maven-compiler-plugin` 3.15.0 and `maven-surefire-plugin` 3.5.6. Versions
  were checked against Maven Central's current stable releases, not guessed.
- Each module declares its own `<dependencies>`: `slf4j-api` (main),
  `junit-jupiter` + `logback-classic` (test only — no module is an
  executable app yet, so no module owns the logging backend in production
  scope yet).
- Package skeleton created per module (`com.forge.<module>`), each with a
  one-line `package-info.java` describing its role (from ARCHITECTURE.md §3)
  and a smoke test (`<Module>ModuleSmokeTest`) that logs via SLF4J and
  asserts `Runtime.version().feature() >= 21` — verifies the module compiles,
  JUnit 5 runs, Logback is wired, and the JVM meets the project's minimum,
  all in one real (if minimal) test rather than an empty placeholder.
- **No inter-module dependencies were declared yet** — deliberately. Each
  module currently only depends on JUnit/SLF4J/Logback. A module will depend
  on a sibling (e.g. `forge-server` → `forge-storage`) exactly when a phase's
  code first needs to import it, not speculatively now.
- Added `.gitignore` (`target/`, IDE files, `.DS_Store`). Git itself was
  **not** initialized or committed — not part of this phase's checklist, and
  git operations are held for an explicit request per this session's
  standing rule on commits.
- No database functionality implemented, as instructed — every module
  currently contains only a package declaration and a smoke test.

## Tests passing

6/6 modules, 6/6 tests (one smoke test per module) — `mvn clean install` from
the repo root. Full log: see build output; summary:

```
forge-common ....................................... SUCCESS
forge-storage ....................................... SUCCESS
forge-server ........................................ SUCCESS
forge-client ........................................ SUCCESS
forge-cluster ....................................... SUCCESS
forge-bench ......................................... SUCCESS
BUILD SUCCESS
```

## Current architecture

Structure matches ARCHITECTURE.md §4 exactly. No storage engine, no network
code, no cluster logic — Phase 0 is tooling and scaffolding only.

## Known bugs

None — no functional code exists yet to have bugs.

## Phase 1: in-memory KV core (complete, 2026-09-01)

- Added to `forge-storage`, package `com.forge.storage`:
  - `KeyValueStore` — the Storage API interface (ARCHITECTURE.md §3.2):
    `put`/`get`/`delete`, all returning `Optional<byte[]>`.
  - `InMemoryKeyValueStore implements KeyValueStore` — single-threaded,
    `HashMap<String, byte[]>`-backed, explicitly documented as not
    thread-safe and not durable (both deliberate Phase 1 limitations, not
    gaps). Defensively clones `byte[]` on every `put` (copy-in) and every
    `get`/`delete` (copy-out) — no path lets a caller mutate stored state
    by holding a reference to an array they passed in or received back.
    Null keys/values rejected via `Objects.requireNonNull`.
- Added `com.forge.storage.repl.Repl` — a line-based REPL supporting `PUT`,
  `GET`, `DELETE`, `HELP`, `EXIT`/`QUIT` (case-insensitive command word).
  Command parsing/dispatch (`execute(String)`) is a package-private,
  I/O-free method so it's directly unit-testable; `run()` wraps it in the
  actual read/print loop over real streams. UTF-8 text ⟷ `byte[]`
  conversion happens **only** in this class — `KeyValueStore` never sees
  text, only bytes. Documented explicitly as *not* the Phase 5 wire
  protocol, despite superficially similar-looking commands.
- **No inter-module dependency changes** — engine and REPL both live in
  `forge-storage`; nothing in `forge-common`/`forge-client`/etc. was touched.
- **Encapsulation/future-phase-coupling review performed**: every field in
  both new classes is `private final`; no getters expose the backing
  `HashMap`; import graph for all three new files contains only JDK
  classes plus (`Repl` only) `com.forge.storage.{KeyValueStore,
  InMemoryKeyValueStore}` — zero references to `forge-server`,
  `forge-client`, `forge-cluster`, `java.net`, `java.nio.channels`, or file
  I/O. Confirms no accidental Phase 2 (WAL/persistence), Phase 4
  (concurrency), or Phase 5 (networking) functionality leaked in.

## Tests passing

`mvn clean test` from the repo root — **41/41 tests pass, 0 failures, 0
errors**, across all 6 modules:

```
forge-common  : 1  (Phase 0 smoke test)
forge-storage : 36 (1 Phase 0 smoke test + 16 InMemoryKeyValueStoreTest + 19 ReplTest)
forge-server  : 1  (Phase 0 smoke test)
forge-client  : 1  (Phase 0 smoke test)
forge-cluster : 1  (Phase 0 smoke test)
forge-bench   : 1  (Phase 0 smoke test)
BUILD SUCCESS
```

All Phase 0 smoke tests still pass unmodified — Phase 1 introduced no
regressions.

## Current architecture

`forge-storage` now has a real, tested Storage API (`KeyValueStore`) and its
Phase 1 in-memory implementation, plus a REPL. Every other module is still
Phase-0-only scaffolding. No WAL, no disk I/O, no concurrency control, no
network code, no cluster logic anywhere in the codebase yet.

## Known bugs

None found. Known, documented **limitations** (not bugs — all intentional
per DESIGN.md's phase table):
- Not durable: process exit loses all data (Phase 2 fixes this).
- Not thread-safe: concurrent access is undefined behavior (Phase 4 fixes this).
- REPL can only address keys/values it can itself type as UTF-8 text, and
  `GET` cannot query a key containing a space (the grammar for `GET`/`DELETE`
  is a single bare token, unlike `PUT`'s `<value...>`) — a REPL usability
  limit, not a `KeyValueStore` limit (the engine itself accepts any
  `String` key, including ones with spaces, when called directly).
- `GET` decodes stored bytes as UTF-8 for display; a value that isn't valid
  UTF-8 (impossible via this REPL, but possible via a future caller of
  `KeyValueStore` directly) would display with Unicode replacement
  characters rather than throwing — acceptable for a debug tool, worth
  keeping in mind once Phase 5's real client exists.

## Phase 2: write-ahead log (complete, 2026-09-01)

- Added `com.forge.storage.wal.WalRecord` — sealed interface (`Put`, `Delete`)
  representing one decoded WAL entry. `opType` (not value length/presence) is
  the sole discriminator, so `Put` with an empty `byte[]` is never confused
  with `Delete`. `Put` defensively clones its value on construction and on
  every `value()` read, and has hand-written content-based `equals`/`hashCode`
  (records default to identity-based array comparison, which would otherwise
  be wrong here) — same discipline as `InMemoryKeyValueStore`, applied
  consistently to a new type.
- Added `com.forge.storage.wal.WriteAheadLog` — concrete final class (no
  interface: unlike `KeyValueStore`, nothing in the roadmap plans a second
  WAL implementation). Binary format, recovery algorithm, and durability
  contract exactly as approved in the Phase 2 design — see the format and
  crash/recovery sections of this session's final report. Not thread-safe,
  by design, matching `InMemoryKeyValueStore`.
- Added `com.forge.storage.DurableKeyValueStore` — composes one
  `InMemoryKeyValueStore` + one `WriteAheadLog` behind the unmodified
  `KeyValueStore` interface. `put`/`delete` append-and-force the WAL record
  *before* touching in-memory state; `get` never touches the WAL.
  `IOException` from the WAL is rethrown as `UncheckedIOException` (same
  pattern already used by `Repl.run()`).
- **`KeyValueStore` and `InMemoryKeyValueStore` were not modified at all.**
  The only Phase 1 file touched was `Repl.main()` (one method): it now
  constructs a `DurableKeyValueStore` over a WAL file path (CLI arg or
  `forge.wal` default) instead of a bare `InMemoryKeyValueStore`, wrapped in
  try-with-resources. `execute()`/`run()`/`CommandResult` are untouched.
- **Security/correctness requirement (validate lengths before allocating)**:
  implemented as a strict validation order during recovery — declared
  `bodyLength` is checked against a hard 64 MiB sanity ceiling AND against
  the file's actual remaining bytes *before* any buffer sized by it is
  allocated; `keyLength`/`valueLength` are checked against what the already-
  bounded `bodyLength` can possibly hold before their own byte arrays are
  allocated. No allocation in the recovery path is ever sized by an
  unvalidated value.
- **Code review performed** against durability correctness, crash ordering,
  checksum/framing correctness, resource management, `FileChannel` lifecycle,
  sequence-number correctness, integer overflow, malformed-input handling,
  defensive copying, API encapsulation, and accidental Phase 3+ coupling
  (verified via import-graph grep — zero references to other modules, no
  SSTable/MemTable/compaction/networking/concurrency code anywhere in the
  new files). The review found and fixed one real bug — in the **test
  harness**, not production: a hand-crafted test helper wrote a length-prefix
  4 bytes short of what the format requires (it omitted the checksum's own
  bytes from the declared body length), which happened to make several
  corruption tests pass for the wrong reason. Fixed, and confirmed the
  affected tests now pass for the intended reason. The review also surfaced
  two real test-coverage gaps — an added defensive check (sequence numbers
  must strictly increase during recovery, beyond the literal minimum spec)
  and `WalRecord.Put`'s own defensive-copy/equals contract were implemented
  but not directly tested — both were closed with new tests rather than left
  as a known gap.
- Manually verified end-to-end across three separate `java` process
  invocations against the same WAL file (not just unit tests): PUT/PUT in
  process 1 → both recovered and a DELETE applied in process 2 → the DELETE
  still held in process 3. Confirms the wiring works outside the test runner
  too.
- **Fixed in a follow-up cleanup (2026-09-01):** the REPL's `BANNER` constant
  used to hardcode "in-memory, single-threaded, not durable" — accurate when
  it was written, stale the moment `main()` started wiring a
  `DurableKeyValueStore`. The real issue was a design smell, not just stale
  text: `Repl` is documented as talking to a `KeyValueStore` purely through
  the interface, so it has no legitimate way to know whether the store
  behind it is durable — a generic class shouldn't hardcode a claim about a
  specific implementation's properties. Fixed by making `BANNER` generic
  ("FORGE REPL. Type HELP for commands.") and moving the durability-specific
  message to `main()`, which is the one place that actually knows it just
  constructed a `DurableKeyValueStore`. No command parsing/dispatch behavior
  changed; `execute()`/`run()`'s control flow/`CommandResult` are untouched.
  Verified manually and via the full test suite (87/87 still pass — no test
  asserted the banner's exact text, only that line 0 of REPL output is "the
  banner," so nothing depended on the old wording).

## Tests passing

`mvn clean test` from the repo root — **87/87 tests pass, 0 failures, 0
errors**:

```
forge-common  : 1
forge-storage : 82  (1 smoke + 16 InMemoryKeyValueStoreTest + 8 WalRecordTest
                      + 21 WriteAheadLogTest + 19 ReplTest + 17 DurableKeyValueStoreTest)
forge-server  : 1
forge-client  : 1
forge-cluster : 1
forge-bench   : 1
BUILD SUCCESS
```

All Phase 0 and Phase 1 tests still pass unmodified.

## Current architecture

`forge-storage` now has a durable storage engine (`DurableKeyValueStore` over
`WriteAheadLog`) alongside the Phase 1 in-memory engine, both implementing
the same unmodified `KeyValueStore` contract. Still no MemTable/on-disk data
files, no compaction, no concurrency control, no network code, no cluster
logic anywhere in the codebase.

## Known bugs

None in shipped code. One bug was found and fixed during this phase, in the
**test harness** (not production): see above.

Known, documented **limitations** (intentional, per DESIGN.md's phase table):
- WAL grows indefinitely — no truncation for space until Phase 3's flush
  makes old entries safe to discard.
- Still not thread-safe (Phase 4).
- Extremely large single key/value writes (multi-gigabyte) could overflow
  the `int` length arithmetic on the *write* path; not guarded against,
  since the threat model for this phase's hardening is a corrupted file on
  *read*, not a malicious in-process caller on *write* — no key/value size
  ceiling has ever been specified for FORGE.

## Phase 3: persistent storage engine (complete, 2026-09-01)

- Added `com.forge.storage.memtable.StoredEntry` — sealed `{ Value, Tombstone }`,
  shared by the MemTable (in memory) and SSTables (on disk). `delete` inserts
  a `Tombstone`, never removes the key — removal would make a deletion
  invisible to any read that falls through to an older, already-flushed
  SSTable. `Value` defensively clones on construction and on every read,
  same discipline as `WalRecord.Put`.
- Added `com.forge.storage.memtable.MemTable` — sorted (`TreeMap`),
  single-threaded, tracks approximate byte size (flush trigger) and the
  highest sequence number absorbed (SSTable header watermark) incrementally.
- Added `com.forge.storage.sstable` — `SSTableFormat` (shared constants),
  `SSTableWriter` (write-only: header + framed records to a temp file,
  `force(true)`, atomic rename), `SSTableReader` (eager header validation at
  open; **no preloading of record data** — every `get` is a genuine on-disk
  scan with early termination on sorted order, since preloading would defeat
  the reason SSTables exist: bounding memory below dataset size). Any
  corruption anywhere in an SSTable is a thrown exception, never a silent
  skip or partial recovery — unlike WAL corruption, an SSTable represents
  data that was already durably acknowledged, so silently working around
  damage to it would be a silent loss of acknowledged data.
- Added one method to the existing, previously-untouched `WriteAheadLog`:
  `truncateAll()` — discards all records after a successful flush, relying
  on flush being synchronous/single-threaded (nothing could have appended a
  genuinely newer record mid-flush) so "discard everything" is correct, not
  just convenient. A general "keep the tail newer than a watermark" rewrite
  was deliberately **not** built — Phase 3 can never exercise or test that
  path, and untested generality was judged worse than none; flagged
  explicitly for Phase 4 to revisit once concurrent writers exist.
- Added `com.forge.storage.LsmKeyValueStore` — the orchestrator, composing
  one `WriteAheadLog` + one active `MemTable` + a newest-first list of
  `SSTableReader`s over a `Path dataDirectory`. Write path: WAL
  append-and-force, *then* MemTable mutate (unchanged ordering discipline
  from Phase 2). Read path: MemTable first, then SSTables newest-to-oldest,
  stopping at the first generation with any entry — `Value` returns,
  `Tombstone` stops the search without checking anything older. Recovery:
  discover+validate SSTables, compute watermark `F` = highest
  `maxSequenceNumber` among them, open the WAL, replay only records with
  `sequenceNumber() > F`. Exposes `flush()` beyond `KeyValueStore` (like
  `DurableKeyValueStore.close()`) so tests can trigger one deterministically.
- **`KeyValueStore`, `InMemoryKeyValueStore`, `DurableKeyValueStore`,
  `WalRecord`, and `Repl` are all unchanged** — confirmed via file
  modification timestamps, not just assumed. `DurableKeyValueStore` is kept
  exactly as-is and remains available: each phase adds a new, more capable
  engine behind the same interface without deleting the previous one's.
  `Repl.main()` was **not** rewired to `LsmKeyValueStore` this phase — not
  requested, and `DurableKeyValueStore` remains a fully valid engine to
  demonstrate via the REPL.
- **Real bug found and fixed by the crash-safety review** (via an actual
  failing integration test, not just inspection): `truncateAll()` only
  preserves the WAL's `nextSequenceNumber` for the lifetime of the
  `WriteAheadLog` instance that called it. Once that instance is closed, the
  now-empty file carries no durable memory of what was discarded — reopening
  it looks, from the WAL's own point of view, exactly like a WAL that was
  never used, so it would hand out sequence number 1 again. That collides
  with a sequence number a flushed SSTable already claims, and the very next
  restart's "skip records already covered by the newest SSTable" rule would
  then silently drop that *new*, already-acknowledged write — a real data-loss
  path, not a cosmetic issue. Fixed by adding
  `WriteAheadLog.ensureNextSequenceNumberAtLeast(long minimum)`, called by
  `LsmKeyValueStore`'s constructor with `watermark + 1` right after opening
  the WAL — the only place that has both the WAL's own view and the SSTable
  watermark. Covered by four new direct `WriteAheadLogTest` cases (including
  one that reproduces the original bug's exact symptom) plus the integration
  test that surfaced it in the first place.
- **Crash-safety verification performed for all four points in
  WAL → MemTable → SSTable → WAL truncation**, per the approved design's
  proof: (a) before flush starts — trivially unaffected, confirmed by
  ordinary restart tests; (b) during SSTable creation (temp file
  partial/unsynced/synced-but-unrenamed) — the file only ever exists under a
  name recovery never looks for, confirmed by a dedicated orphaned-temp-file
  test; (c) after rename, before WAL truncation — confirmed by a dedicated
  test that manually reproduces this exact intermediate state (writes a
  real SSTable via `SSTableWriter` while deliberately skipping
  `truncateAll()`) and verifies recovery is still fully correct; (d) after
  WAL truncation — the ordinary steady-state path, exercised repeatedly by
  the multi-restart-cycle tests. All four pass.
- Verified via `grep` that no `forge-server`/`client`/`cluster`/`bench`
  reference, and no compaction/Bloom-filter/socket/thread/synchronized/
  partition/replication/consensus code, exists anywhere in the new files —
  only documentation notes stating the classes are *not* thread-safe.

## Tests passing

`mvn clean test` from the repo root — **151/151 tests pass, 0 failures, 0
errors**:

```
forge-common  : 1
forge-storage : 146  (1 smoke + 16 InMemoryKeyValueStoreTest + 8 WalRecordTest
                       + 28 WriteAheadLogTest + 19 ReplTest + 17 DurableKeyValueStoreTest
                       + 7 StoredEntryTest + 13 MemTableTest + 14 SSTableWriterReaderTest
                       + 23 LsmKeyValueStoreTest)
forge-server  : 1
forge-client  : 1
forge-cluster : 1
forge-bench   : 1
BUILD SUCCESS
```

All Phase 0–2 tests still pass unmodified.

## Current architecture

`forge-storage` now has three storage engines behind the same unmodified
`KeyValueStore` interface: `InMemoryKeyValueStore` (Phase 1), `DurableKeyValueStore`
(Phase 2, WAL only), and `LsmKeyValueStore` (Phase 3, WAL + MemTable +
SSTables). No compaction, no Bloom filters, no concurrency control, no
network code, no cluster logic anywhere in the codebase yet.

## Known bugs

None in shipped code. One real bug (sequence-number reuse across a WAL
close/reopen after `truncateAll()`) was found and fixed during this phase —
see above.

Known, documented **limitations** (intentional, per DESIGN.md's phase table
and the explicit Phase 3 exclusion list):
- No compaction: SSTables and tombstones accumulate forever; disk usage is
  monotonically non-decreasing; GET cost grows with the number of SSTables
  that must be checked on a miss (read amplification). Deferred to stretch
  phase 13★.
- `SSTableReader.get` is a genuine per-call linear disk scan (with
  early-exit on sorted order) — no sparse index, no Bloom filter. Slower
  than an in-memory structure by design: preloading would defeat bounding
  memory below dataset size, the whole reason this phase exists.
- Flush is synchronous and single-threaded; it blocks the triggering
  `put`/`delete` call for its full duration.
- `WriteAheadLog.truncateAll()` discards everything unconditionally — correct
  only because flush is synchronous (nothing could have appended a newer
  record mid-flush). A concurrent writer (Phase 4) will need a general
  "discard up to a watermark, keep anything newer" rewrite instead.
- No directory-fsync after an SSTable's atomic rename — a documented,
  accepted gap against true power-loss (not process-crash) durability,
  matching the WAL's own already-documented fsync ceiling from Phase 2.
- Still not thread-safe (Phase 4).

## Phase 4: concurrency (complete, 2026-09-01)

- Added `com.forge.storage.ConcurrentLsmKeyValueStore` — a thread-safe
  `KeyValueStore` providing the same durability/recovery guarantees as
  `LsmKeyValueStore`, safe for concurrent PUT/GET/DELETE. **Not a wrapper**
  around `LsmKeyValueStore` (an earlier design draft proposed one, but a
  wrapper can't split flush into locked/lock-free phases without rewriting
  the delegate's own monolithic `flush()`) — a standalone orchestrator
  directly composing `WriteAheadLog`, two `MemTable`s (`active`/`frozen`),
  and an SSTable list. `LsmKeyValueStore` itself is completely untouched —
  still the Phase 3 baseline, still valid on its own.
- **Synchronization model**: one `ReentrantReadWriteLock` (`stateLock`)
  guards exactly three fields (`active`, `frozen`, `sstables`). `get()`
  takes the read lock only long enough to snapshot all three together as
  one atomic group, then scans SSTables lock-free. `put`/`delete` take the
  write lock for the in-memory lookup + WAL append + `active` mutation,
  then release it before any deferred SSTable scan (for the return value)
  or triggered flush's disk I/O. **A flush's slow phase — writing, forcing,
  and atomically renaming the SSTable, and truncating the WAL — holds no
  `stateLock` at all**; only the brief freeze (swap `active`/`frozen`) and
  completion (register the new reader, clear `frozen`) touch it, each for
  microseconds. This is what satisfies the phase's hard requirement: writes
  are never blocked by SSTable disk I/O.
- Added `WriteAheadLog.truncateUpTo(long watermark)` — discards records at
  or below the watermark, **preserving newer ones**, unlike the existing
  `truncateAll()` (kept, unchanged, still used by `LsmKeyValueStore`).
  Required because flush and ordinary writes can now genuinely overlap: a
  write can land in the active MemTable while an older, frozen MemTable's
  flush is still draining to disk, and that write's WAL record must survive
  the eventual truncation. Uses the identical crash-safe technique as
  SSTable creation (temp file, `force(true)`, atomic rename over the WAL's
  own path) — reuses the existing, already-tested `recover()` scan rather
  than writing new parsing logic. Runs under the WAL's own lock only, never
  `stateLock`, so it never blocks a concurrent `get()`, which has no
  dependency on WAL state at all.
- Added `synchronized` to `WriteAheadLog`'s four existing mutating methods
  (`appendPut`, `appendDelete`, `truncateAll`, `ensureNextSequenceNumberAtLeast`)
  plus `close()` — defensive hardening, not load-bearing given
  `ConcurrentLsmKeyValueStore`'s own lock already serializes all its
  callers, but the WAL now has three distinct callers with different
  threading models (`DurableKeyValueStore`, `LsmKeyValueStore`,
  `ConcurrentLsmKeyValueStore`), so making it correctly self-defending on
  its own terms is worthwhile at negligible cost (nanoseconds against the
  millisecond-scale `fsync` every call already pays). The class Javadoc now
  explicitly states this does **not** make `DurableKeyValueStore` or
  `LsmKeyValueStore` safe for direct concurrent use — their own `HashMap`/
  `TreeMap` remain unsynchronized.
- **Why the "frozen" MemTable tier exists, and the bug it closes**: an
  earlier design draft (stop-the-world, holding the lock through flush's
  entire disk I/O) was rejected specifically because it violates the "writes
  not blocked by SSTable I/O" requirement. The freeze-based alternative was
  then found, during design review, to have a real bug if implemented
  naively: reading `frozen` and `sstables` as two independent fields lets a
  reader observe a combination that never existed as one consistent state
  (`frozen == null` — already cleared — paired with an `sstables` snapshot
  taken *before* the corresponding new reader was added) — making a fully,
  durably flushed key transiently, incorrectly invisible to GET. Fixed by
  making the `frozen`-clear and `sstables`-update happen in the same
  `stateLock` write-lock hold, so no reader can ever observe one without the
  other. Implemented correctly from the start (not retrofitted) and verified
  directly by a dedicated test (`getNeverSeesAFlushedKeyAsAbsentAcrossTheEntireFlushWindow`)
  that hammers GET continuously across an entire real flush.
- `put`/`delete`'s previous-value lookup is deferred to a lock-free SSTable
  scan (on a snapshot taken while still holding the write lock) when the key
  isn't in `active`/`frozen` — avoiding holding `stateLock` during a
  potentially slow disk scan just to compute a return value, while remaining
  linearizable: the snapshot is fixed at the exact moment of the write's own
  linearization point, so it cannot observe anything newer.
- **Known, deliberately accepted limitation**: if `completeFlush` fails
  partway (e.g., disk full during the SSTable write), `frozen` is not
  cleared and this instance will not attempt another flush until restarted
  — no data is at risk (`active`/`frozen`/WAL remain fully consistent, and a
  restart's recovery can retry the flush from scratch), but automatic retry
  was judged out of scope for this phase (would require background-thread
  coordination this phase deliberately excludes).
- **`forge-bench` was deliberately not touched this phase** — this
  implementation pass focused strictly on concurrency correctness per the
  message's explicit rules; the benchmark harness (E3: Phase 3 vs. Phase 4
  throughput/latency comparison) remains a separate, not-yet-started task,
  noted here rather than silently dropped.

## Post-implementation adversarial review

Ran the full concurrency suite (`WriteAheadLogTest` + `ConcurrentLsmKeyValueStoreTest`,
71 tests) four times back to back — zero flakiness, zero failures — on top of
the design-time adversarial passes from the three prior review rounds. Added
one further adversarial test not previously present at the engine level:
manually reproducing the exact crash window between an SSTable's atomic
rename and WAL truncation (driving `WriteAheadLog`/`SSTableWriter` directly,
skipping truncation, then opening a real `ConcurrentLsmKeyValueStore`) —
verifies the crash-safety proof holds in the actual running code, not just
in the design. No bug was found during this pass — the two real bugs from
this feature's development (the WAL sequence-allocation/append coupling
requirement, and the frozen/sstables atomicity bug) were both caught and
fixed during the design-review rounds, before any code was written, exactly
as those reviews were meant to achieve.

## Tests passing

`mvn clean test` from the repo root — **194/194 tests pass, 0 failures, 0
errors**:

```
forge-common  : 1
forge-storage : 189  (1 smoke + 16 InMemoryKeyValueStoreTest + 8 WalRecordTest
                       + 35 WriteAheadLogTest (28 existing + 7 new)
                       + 19 ReplTest + 17 DurableKeyValueStoreTest
                       + 7 StoredEntryTest + 13 MemTableTest + 14 SSTableWriterReaderTest
                       + 23 LsmKeyValueStoreTest + 36 ConcurrentLsmKeyValueStoreTest (new))
forge-server  : 1
forge-client  : 1
forge-cluster : 1
forge-bench   : 1
BUILD SUCCESS
```

All Phase 0–3 tests still pass, byte-for-byte unmodified — confirmed both by
count (unchanged for every pre-existing test class) and by file
modification timestamps (no Phase 0–3 source or test file has a timestamp
from this session).

## Current architecture

`forge-storage` now has four storage engines behind the same unmodified
`KeyValueStore` interface: `InMemoryKeyValueStore` (Phase 1),
`DurableKeyValueStore` (Phase 2), `LsmKeyValueStore` (Phase 3, single-threaded),
and `ConcurrentLsmKeyValueStore` (Phase 4, thread-safe). No compaction, no
Bloom filters, no networking, no cluster logic anywhere in the codebase yet.

## Known bugs

None in shipped code.

Known, documented **limitations** (intentional):
- Writes do not get faster under concurrency, only safer — fully serialized
  by the WAL's inherent single-writer bottleneck, by design.
- No per-key lock striping — rejected during design review because the WAL
  append is a global bottleneck regardless of key, so finer-grained MemTable
  locking wouldn't remove the actual contention.
- A failed flush permanently disables further flushing for that instance
  until restart (see above) — correctness is preserved, availability of
  future flushes is not, until restart.
- `forge-bench`'s E3 benchmark (Phase 3 vs. Phase 4 throughput/latency
  comparison) has not been built or run — deferred, not forgotten.
- No compaction: SSTables and tombstones still accumulate forever (Phase 3
  limitation, unaffected by this phase).
- No networking, partitioning, replication, or consensus — untouched, as
  required.

## Phase 5: network layer (complete, 2026-09-02)

- Added a wire protocol in `forge-common` (`com.forge.common.protocol`),
  depended on by both `forge-server` and `forge-client` so the two sides
  can never drift: `ProtocolConstants`, sealed `Request`
  (`Put`/`Get`/`Delete`) and `Response` (`OkAbsent`/`OkPresent`/`Error`)
  record types with the same defensive-copy discipline as `WalRecord`, a
  checked `ProtocolException` (extends `IOException`, carries an
  `ERROR_*` code), and `FrameCodec` — the encoder/decoder. Wire format:
  `[int32 frameLength][payload]`, with request payloads
  `[int8 opCode][int32 keyLength][keyBytes][int32 valueLength][valueBytes]`
  and response payloads `[int8 status][int8 errorCode][int32 payloadLength][payloadBytes]`.
- **Length validation matches the WAL/SSTable discipline**: every length
  field is checked against both a configured maximum and the bytes actually
  available *before* it is used to allocate an array, so a corrupted or
  hostile length can never trigger an oversized allocation. The outer
  `frameLength` is checked before the payload is even read; the inner
  `keyLength`/`valueLength` are checked against `maxKeyLength` and the
  buffer's remaining bytes before each `new byte[...]`.
- **Decoding an unrecognized opcode/status still fully consumes the frame**:
  key and value are decoded uniformly before branching on opcode, so even a
  request `FrameCodec` doesn't understand is fully, safely read off the
  wire — which is what lets a connection recover and keep serving after a
  malformed (but not oversized) request, instead of having to close.
  `ERROR_OVERSIZED_REQUEST` is the one exception: it's raised *before* the
  declared payload is read, so the stream is left desynced and that case is
  always connection-fatal.
- Added `forge-server`: `ForgeServer` (binds and starts serving in its
  constructor, one virtual thread per connection via
  `Executors.newVirtualThreadPerTaskExecutor()`), `ConnectionHandler`
  (per-connection read-dispatch-write loop implementing the error-recovery
  policy above), and a `Main` CLI entry point. `ForgeServer` tracks active
  sockets so `close()` can force them shut (unblocking any in-flight
  blocked reads) in addition to closing the listening socket and shutting
  down the connection executor.
- Added `forge-client`: `ForgeClient` (blocking, one connection per
  instance, not thread-safe by design — one request in flight at a time)
  and `ForgeServerException` (thrown when the server returns
  `Response.Error`; extends `IOException` so callers can catch one
  hierarchy for both transport and server-reported failures).
  **Deliberately does not implement `KeyValueStore`**: that interface's
  `put`/`delete` return the previous value and declare no checked
  exceptions — the wire protocol has no way to carry a previous value back
  (PUT/DELETE both ack with a bare `OkAbsent`), and every network call here
  can fail with a checked `IOException`. Implementing it would also force
  an unwanted `forge-client` → `forge-storage` dependency.
- Added the `tests` Maven module (`forge-tests` artifact, reusing the
  Phase-0-reserved, previously-empty `tests/` directory) depending on
  `forge-server` + `forge-client` + `forge-storage` in test scope only —
  the one place a real client talks to a real server. Neither
  `forge-server` nor `forge-client` depends on the other, in any scope.
- **Connection model**: one virtual thread per connection running ordinary
  blocking I/O, chosen specifically because of FORGE's Java 21 baseline —
  not a general recommendation. Documented, accepted caveat: a virtual
  thread that blocks inside one of `WriteAheadLog`'s `synchronized` methods
  (e.g. during `force()`) pins its carrier platform thread for that
  duration rather than yielding it.
- **Bug found and fixed during adversarial review (before, not after, being
  reported as done)**: `ForgeServer.close()` had a shutdown race — a
  connection whose TCP handshake completed microseconds before `close()`
  set `closed = true` could still be returned by a subsequent `accept()`
  call, get added to `activeConnections` *after* `close()`'s cleanup loop
  had already iterated it, and then have its `connectionExecutor.execute(...)`
  submission rejected (executor already shut down) — leaking the socket.
  Fixed by re-checking `closed` immediately after `accept()` returns and
  before registering the connection, closing and discarding it immediately
  if shutdown has already begun. This narrows the window to the
  irreducible one that exists at the OS accept-queue level (outside this
  class's control, and outside any application-level fix) — documented as
  such in `ForgeServer`'s class Javadoc rather than claimed as fully closed.

## Post-implementation adversarial review

Reviewed `FrameCodec` for the same class of issue Phase 2's WAL codec had
(oversized-allocation-from-untrusted-length) — confirmed every length field
is bounds-checked before allocation, both the outer frame length and the
inner key/value lengths, with dedicated tests hand-crafting each malformed
case independently of `FrameCodec`'s own encoder (`FrameCodecTest`, 23
tests) so a bug in the encoder can't also hide from the test meant to catch
it. Reviewed `ConnectionHandler`'s error-recovery policy for stream-position
correctness (does treating most `ProtocolException`s as recoverable ever
leave the stream desynced?) — verified `ERROR_OVERSIZED_REQUEST` is the only
exception, since it's the only error raised before the payload is fully
read, and this is now a directly tested case in both `ForgeServerTest` and
`ClientServerIntegrationTest`. Reviewed `ForgeServer.close()`'s shutdown
path for resource leaks under concurrent new connections — found and fixed
the accept-during-shutdown race above.

## Tests passing

`mvn clean test` from the repo root — **265/265 tests pass, 0 failures, 0
errors**:

```
forge-common  : 42  (1 smoke + 10 RequestTest + 8 ResponseTest + 23 FrameCodecTest)
forge-storage : 189  (unchanged from Phase 4)
forge-server  : 11  (1 smoke + 10 ForgeServerTest)
forge-client  : 11  (1 smoke + 10 ForgeClientTest)
forge-cluster : 1  (unchanged)
forge-bench   : 1  (unchanged)
forge-tests   : 10  (8 ClientServerIntegrationTest + 2 ConcurrentClientsIntegrationTest)
BUILD SUCCESS
```

All Phase 0–4 tests still pass, unmodified — `forge-storage`'s 189 tests are
byte-for-byte the same count as at the end of Phase 4, and no Phase 0–4
source or test file was touched this phase.

## Current architecture

`forge-common` now carries the shared wire protocol
(`com.forge.common.protocol`) alongside its (still-unused-elsewhere)
package-info. `forge-server` and `forge-client` have gone from empty
placeholder modules to a real TCP server and a real blocking client, both
built only on `FrameCodec`/`Request`/`Response` from `forge-common` (plus
`forge-storage` for the server, which runs any `KeyValueStore` — used with
`ConcurrentLsmKeyValueStore` in the integration tests and in `Main`). A new
`forge-tests` module is the only place `forge-server` and `forge-client`
are ever used together. `forge-cluster` and `forge-bench` remain untouched.

## Known bugs

None in shipped code (the shutdown race above was found and fixed before
this phase was reported complete, per the standing "no fabricated results"
rule).

Known, documented **limitations** (intentional):
- No TLS, no authentication/authorization — any client that can open a TCP
  connection can read and write everything. Explicitly deferred; not an
  oversight.
- No idle/read timeouts on either side — a stalled peer holds its
  connection (and, server-side, its virtual thread) open indefinitely.
- `ForgeClient` does not reconnect or retry — a dropped connection is a
  terminal `IOException`; callers that want resilience build it on top.
- Server-side storage/internal error messages (`Response.Error`'s
  `message`) are not sanitized before being sent to the client — a
  `UncheckedIOException`'s message (which can include a local file path)
  reaches the wire as-is. Acceptable given no untrusted clients are assumed
  yet (see the auth limitation above), but would need addressing before
  this server is ever exposed to one.
- `ForgeServer.close()`'s shutdown has one irreducible race against the OS
  accept queue (documented in its class Javadoc) — not fixable at this
  layer.
- No partitioning, replication, consensus, or benchmarking hooks wired up
  yet — `forge-cluster` and `forge-bench` remain exactly as they were.

## Phase 6: concurrent server + first real benchmarks (complete, 2026-09-02)

### What was implemented

- **`forge-bench` went from an empty placeholder to a full benchmark
  harness.** Core: `LatencyStats` (nearest-rank percentiles, unit-tested
  against hand-computed values), `BenchOperation`/`Workload` (deterministic
  key/value generation, no RNG or allocation inside the timed path),
  `OpExecutor`/`NetworkOpExecutor`/`InProcessOpExecutor` (one interface, two
  implementations — real `ForgeClient` over loopback TCP, or a direct
  `KeyValueStore` call), `BenchWorker`/`ConcurrentBenchRun` (sequential and
  barrier-synchronized concurrent execution, correct throughput math — whole
  measured phase's wall clock, never summed per-thread durations),
  `BenchmarkResult`/`CsvReportWriter` (machine-readable CSV output, no
  hardcoded numbers), `BenchmarkRunner` (starts a real embedded
  `ForgeServer`, runs every experiment, writes CSV + human summary, tears
  down cleanly).
- **Four experiments implemented, matching DESIGN.md's Phase 6 scope
  exactly** ("first real benchmark (E1–E4)" per §5's phase table, plus E5,
  explicitly listed as runnable from Phase 6 in §4's experiment table): E1
  (sequential baseline, now over the network — it was in-process-only
  through Phase 4), E3 (concurrency scaling, 1/2/4/8/16/32 connections), E4
  (network overhead: in-process vs. loopback TCP), E5 (mixed 80/20
  read/write workload). E2 (fsync policy sweep) stays "Not run" — there is
  no fsync-off mode in the codebase to sweep; see BENCHMARKS.md's E2 note.
- **Correctness tests added specifically for Phase 6's concurrent-server
  requirements**, beyond what Phase 5 already covered:
  `RequestResponsePairingStressTest` (1000-op varied sequence on one
  connection, every response checked against exactly what its own request
  should have produced), `SharedKeyContentionIntegrationTest` (12 clients ×
  150 writes each, contending on 5 shared keys, every write self-describing
  so a torn/corrupted value would fail a regex check — none did),
  `ConcurrentBenchRunTest` and `BenchmarkCleanupTest` (harness mechanics:
  correct op counts, correct throughput arithmetic, no lost/collided keys
  under concurrency, port genuinely released after `close()`).
- **No production code changes were needed in `forge-server`/`forge-storage`
  this phase** — Phase 5's `ForgeServer`/`ConnectionHandler` and Phase 4's
  `ConcurrentLsmKeyValueStore` handled every correctness scenario this
  phase's adversarial tests threw at them (many simultaneous clients, shared
  key contention, malformed requests, shutdown) without a single failure.
  This phase's real findings were about *performance characteristics* of
  already-correct code, and about the *benchmark harness's own* correctness
  — see below.

### Benchmark methodology

Full methodology (environment, server placement, warm-up/measurement
separation, throughput/latency calculation, repeat count) is in
[BENCHMARKS.md §1](docs/BENCHMARKS.md#1-methodology) — not duplicated here
so there's exactly one place it can go stale. Headline facts: 3 repeats per
experiment configuration, warm-up always run and always discarded, all
numbers are client-observed (full round trip, not server-internal timing),
fsync is unconditionally on for every measurement (there's no way to turn it
off), 66 individual measured results in
`forge-bench/results/phase6-benchmarks-2026-09-01T19-12-12.429128Z.csv`.

### Results (full tables and interpretation in BENCHMARKS.md §2–§6)

- **E1 sequential**: GET ~34K ops/sec (0.03ms avg); PUT/DELETE ~251 ops/sec
  (~4.0ms avg) — both fsync-bound, essentially identical cost.
- **E3 concurrency scaling**: GET scales 37.7K → 105.7K ops/sec through 16
  connections, flattening at 32 (CPU-bound on this 10-core machine). **PUT
  throughput does not scale at all** — pinned at ~250 ops/sec from 1 to 32
  connections — while PUT latency grows almost exactly linearly with
  concurrency (4.0ms → 122.4ms). This is `WriteAheadLog`'s single-writer
  `synchronized` fsync, measured directly and matching the arithmetic
  predicted from it (~4ms/op ⇒ ~250/sec ceiling) at every level.
- **E4 network overhead**: in-process GET ~913K ops/sec (1µs) vs. loopback
  GET ~25.8K ops/sec (39µs) — network+protocol overhead ≈38µs/op, a real
  and significant cost for reads. PUT shows no measurable difference
  in-process vs. loopback (~4ms either way) — a ~4ms fsync makes 38µs of
  network overhead noise.
- **E5 mixed 80/20 workload**: isolated (fresh server) run is fast and
  stable — ~1,227 ops/sec across 3 repeats, matching a back-of-envelope
  model built from E3's PUT ceiling within 2%. The **same workload run
  against a store that already absorbed E1+E3's ~20K+ prior operations**
  degraded to ~43 ops/sec in one full-suite run — investigated in depth
  below rather than averaged away.

### Bugs and issues found

1. **E4 benchmark-methodology bug (found, fixed)**: the original
   `NetworkOverheadExperiment` shared one long-lived server/store with
   E1/E3 for its network-side measurement while giving the in-process side
   a fresh store every repeat. First run's loopback GET numbers collapsed
   from ~26K to 45 ops/sec by the third repeat — an asymmetry in the
   benchmark's own design, not a FORGE bug. Fixed by giving both sides a
   fresh server/store per repeat (`NetworkOverheadExperiment`'s Javadoc
   explains why); the corrected run is stable across all 3 repeats.
2. **`stateLock.writeLock()` is held for the entire WAL fsync duration in
   `ConcurrentLsmKeyValueStore.put`/`delete`** (confirmed by reading the
   code, `ConcurrentLsmKeyValueStore.java:159–178` and `:216–235`) — every
   concurrent GET blocks for the full ~4ms of any in-flight PUT/DELETE's
   fsync, since `get()` needs the same lock's read side. This was never
   visible in E1/E3 (each exercises one op type at a time) and is almost
   certainly the dominant contributor to E5's sustained-load degradation.
   **Not fixed this phase** — see "Why no fix" below.
3. **`SSTableReader.get()`'s lack of an index measurably costs 7–158µs per
   miss/hit**, confirmed with a small, isolated diagnostic
   (`SSTableScanCostDiagnosticRunner`) rather than left as speculation —
   real, and grows with accumulated data, but two to three orders of
   magnitude too small to be more than a secondary contributor to E5's
   181ms average.
4. **Benchmark-tooling bug (found, fixed), not a FORGE bug**:
   `forge-bench/pom.xml`'s `exec-maven-plugin` hardcoded `<mainClass>` as a
   literal, silently ignoring `-Dexec.mainClass=...` on the command line —
   every attempt to run an isolated diagnostic class actually re-ran the
   full benchmark suite instead, which is what first looked like an
   inexplicable multi-minute hang. Diagnosed with `jstack` against what was
   actually expected to be executing, not assumed from elapsed time. Fixed
   with a `${exec.mainClass}` property (default `BenchmarkRunner`,
   overridable).

Full writeup, including the exact investigation sequence, is in
[BENCHMARKS.md §6](docs/BENCHMARKS.md#6-what-the-benchmark-run-itself-found-methodology-affecting).

### Why no fix was made for finding #2

Instruction was explicit: only optimize when (1) there's evidence for the
bottleneck, (2) it's in Phase 6 scope, (3) correctness stays intact, (4) the
benchmark can demonstrate the effect. #1, #3, #4 are satisfied — but #2
isn't cleanly: narrowing `stateLock`'s write-lock hold to exclude the fsync
call is a real `forge-storage` concurrency-control change to Phase 4's
already-reviewed, already-shipped `ConcurrentLsmKeyValueStore`, and this
message's constraints are explicit that Phase 6 is `forge-server`/
`forge-bench` work, not a reopening of Phase 4's engine. It would also need
its own design review with the same rigor Phase 4 got (the write lock
currently also guards the atomic `frozen`/`sstables` update that Phase 4's
review specifically added to fix a real linearizability bug — narrowing the
lock scope without re-deriving that proof from scratch would be exactly the
kind of unreviewed change the project's methodology exists to prevent).
Recorded here as a **known, well-evidenced limitation** for a future phase
to pick up deliberately, not silently patched around.

### Tests

`mvn clean test` from the repo root — **280/280 tests pass, 0 failures, 0
errors**:

```
forge-common  : 42  (unchanged from Phase 5)
forge-storage : 189  (unchanged from Phase 5)
forge-server  : 11  (unchanged from Phase 5)
forge-client  : 11  (unchanged from Phase 5)
forge-cluster : 1  (unchanged)
forge-bench   : 14  (1 smoke + 7 LatencyStatsTest + 4 ConcurrentBenchRunTest + 2 BenchmarkCleanupTest)
forge-tests   : 12  (8 ClientServerIntegrationTest + 2 ConcurrentClientsIntegrationTest
                      + 1 RequestResponsePairingStressTest + 1 SharedKeyContentionIntegrationTest)
BUILD SUCCESS
```

All Phase 0–5 tests still pass, unmodified — no Phase 0–5 source or test
file was touched this phase (confirmed: `forge-common`/`forge-storage`/
`forge-server`/`forge-client` counts are byte-for-byte the same as Phase
5's). The two new `forge-tests` correctness tests found no bugs — reported
here per the "if it exposes a real bug, stop and fix" rule: it didn't, so
there was nothing to fix, which is itself the result being reported.

### Current architecture

`forge-bench` is now a real benchmark harness (`com.forge.bench`), built on
`forge-common`/`forge-storage`/`forge-server`/`forge-client`, runnable via
`mvn -pl forge-bench exec:java` (default target `BenchmarkRunner`;
`-Dexec.mainClass=...` selects one of the two diagnostic runners instead).
No other module changed. `forge-cluster` remains untouched.

### Known limitations

- **The write-lock-holds-fsync finding (above) is not fixed** — deliberate,
  explained above, and the single most important thing for whoever does
  Phase 7+ concurrency work to read first.
- E5's degraded-under-sustained-load number (~43 ops/sec, one observed run)
  is not fully quantitatively explained — two real, source-confirmed
  mechanisms are identified and one is measured directly, but the precise
  path from "these mechanisms exist" to "this exact number" would need
  profiling beyond this phase's scope. Reported as such, not smoothed over.
- E2 (fsync policy sweep) and the key-skew/Zipfian half of E5 remain not
  run — neither is in scope for `forge-server`/`forge-bench` alone (E2 needs
  a `forge-storage` toggle that doesn't exist; Zipfian needs a real
  generator that wasn't part of this phase's request).
- E6–E11 remain not run — Phases 7–13★ don't exist yet.
- No LAN leg of E4 — only one machine was available.
- Every Phase 5 limitation (no TLS/auth, no idle timeouts, no client
  reconnect/retry, unsanitized error messages, the one irreducible
  accept-queue shutdown race) is unchanged and still applies.

### Interview questions

**1. Why does PUT throughput stay flat at ~250 ops/sec from 1 to 32
concurrent connections, while GET throughput scales to over 100K? What
would it take to raise that 250 ceiling?**
`WriteAheadLog.appendPut` is `synchronized` and calls `FileChannel.force(true)`
(fsync) inside the lock, so only one PUT's fsync can be in flight across the
*entire process* at a time — every other concurrent PUT queues behind it
regardless of connection count. GET never touches the WAL. Raising the
ceiling means either making individual fsyncs faster (faster disk, or OS/
filesystem-level write-caching tradeoffs that weaken the durability
guarantee) or batching multiple pending writes into one fsync call (group
commit — accumulate a short window of concurrent appends, fsync once, ack
them all) — the latter is the standard technique real databases use here,
and is exactly the kind of thing that would need its own design review
given it changes the durability/ack-timing contract.

**2. E4 showed ~38µs of network+protocol overhead per GET but no
measurable overhead for PUT. Why would the same network layer cost matter
for one operation and not the other?**
Overhead is additive, not multiplicative: 38µs added to a ~1µs in-process
GET is a 35× slowdown and clearly visible; 38µs added to a ~4ms fsync-bound
PUT is under 1% and disappears into normal measurement noise. The lesson
generalizes: network/protocol overhead only matters for operations fast
enough that it isn't dwarfed by something slower already in the same path.

**3. The first E4 run showed loopback GET collapsing from 26K to 45 ops/sec
across three repeats of the *same* code and config. What was actually
different between the repeats, and why didn't concurrency or the network
layer explain it?**
Nothing about E4's own code changed between repeats — what changed was how
much prior write traffic the *shared* store carried into each repeat (it
was reused across E1, E3, and all of E4 before the fix). In-process GET,
which got a fresh store every repeat, stayed fast throughout; only the
network side, hitting an increasingly loaded store, degraded. That
asymmetry — not network variability — was the actual variable, which is why
fixing the benchmark's design (fresh store per repeat, both sides) made the
anomaly disappear rather than averaging it out.

**4. `ConcurrentLsmKeyValueStore.get()` takes a read lock and `put()`/
`delete()` take a write lock on the same `ReentrantReadWriteLock` — doesn't
a read-write lock exist specifically so reads don't block on writes? Why
did this benchmark find that GETs block on PUTs anyway?**
A `ReentrantReadWriteLock` lets multiple *readers* run concurrently with
each other, but a *writer* still excludes everyone — reader or writer —
for as long as it holds the write lock. The bug isn't the lock type, it's
*what's done while holding it*: `put()` calls `wal.appendPut()` — which
fsyncs — from inside the write-lock hold, so the write lock (and therefore
every concurrent GET) is held for the ~4ms fsync takes, not just for the
brief in-memory bookkeeping the design intended. The fix would be moving
the fsync call outside the lock (append-then-lock-to-publish, or similar) —
not swapping lock types.

**5. Why measure percentiles with "nearest-rank" (`ceil(p/100×n)-1`)
instead of, say, linear interpolation between the two nearest samples?**
Nearest-rank always reports an actual observed latency value — the sample
literally comes from a real operation that really happened — while
interpolation can report a number no operation ever produced. For a
benchmark whose whole point is "what did the system actually do," reporting
only values that were actually measured is the more honest choice, at the
cost of slightly coarser percentile boundaries on small sample sizes (which
matters little here since every measured phase has hundreds to thousands of
samples). It's also the convention most load-testing tools (`wrk`, JMH)
already use, so results are comparable to what a reader would expect from
other tools.

## Phase 7: partitioning / sharding (complete, 2026-09-03)

### Scope and design decisions

Followed ARCHITECTURE.md §3.6's already-chosen design (consistent hashing
with virtual nodes) rather than a fixed-N-partitions scheme — "configurable
number of partitions" in the request is implemented as a configurable
virtual-node count per physical node (`ConsistentHashRing.DEFAULT_VIRTUAL_NODES_PER_NODE
= 128`), which is the faithful mapping of that knob onto the design
ARCHITECTURE.md had already settled on, not a substitution for it.

**Freeze compliance**: `forge-storage` and `forge-server` were touched, both
times purely additively, both fully justified by Phase 7's own stated
requirements (not a later phase forcing a reopening, and not a stylistic
rewrite):
- `SSTableReader` gained `scanAll()` (every record, key-ascending) alongside
  the untouched `get()` — both now share one extracted `readRecordAt`
  helper, refactored so the corruption-checking logic exists in exactly one
  place rather than being duplicated and risking divergence. `get()`'s
  behavior and every existing test for it are unchanged.
- `ConcurrentLsmKeyValueStore` gained `keys()` (every live key, resolved
  correctly across active MemTable → frozen MemTable → SSTables
  newest-to-oldest, snapshotted under the same `stateLock` read lock `get()`
  uses). Needed because partition rebalancing has to know what a node
  actually stores; nothing before Phase 7 needed to enumerate keys at all.
- `ForgeServer`/`ConnectionHandler` gained an optional `Predicate<String>
  ownershipPredicate` parameter (new constructor overloads only — every
  pre-Phase-7 constructor delegates to `key -> true`, so a caller that never
  heard of partitioning gets byte-for-byte the same behavior as before).
  When a key fails the predicate, `ConnectionHandler` returns a new
  `ProtocolConstants.ERROR_NOT_OWNER` response *without ever touching the
  store* — DESIGN.md's Phase 7 contract's "fail closed, not open."
  `ERROR_NOT_OWNER` is a new, additive wire-protocol constant; the frame
  format itself didn't change (errors already carry a code + message).

**Real multi-node, not simulated**: every partitioning test starts multiple
genuinely separate `ForgeServer` + `ConcurrentLsmKeyValueStore` pairs, each
its own TCP port and its own on-disk directory. `PartitioningIntegrationTest`
verifies physical placement directly against each node's own store (not
just "routing appeared to work") — a key is confirmed present on exactly
the one node the topology says owns it, and absent from every other node.

**What Phase 7 deliberately does not do** (honest scope boundaries, not
oversights):
- No gossip or live topology distribution — every participant (client,
  and each node's ownership predicate) holds an explicit, static
  `ClusterTopology`/`ConsistentHashRing` snapshot. ARCHITECTURE.md §3.6
  explicitly names "every-node-has-a-copy" as the starting point before
  gossip.
- No automatic rebalancing — `PartitionRebalancer.migrate` is an explicit,
  invokable operation (a human or a future coordinator calls it after a
  topology change), not a background process that discovers and reacts to
  membership changes on its own. Membership itself doesn't exist until
  Phase 8.
- No replica-based bootstrap for partition movement — there's no
  replication yet (Phase 9), so movement is a direct copy from the losing
  node to the gaining node, not "copy from a healthy replica" the way
  DESIGN.md's Phase 7 recovery-behavior contract eventually describes (that
  fuller behavior depends on Phase 9/10 machinery that doesn't exist yet).
- A running node's ownership predicate is fixed at construction — it is not
  live-updated when the topology changes. `PartitioningIntegrationTest`'s
  rebalancing test makes this explicit: adding a node requires restarting
  the existing nodes' servers (atop their *existing, untouched* stores) with
  a predicate built from the new ring. A live-swappable predicate would be a
  small, well-scoped follow-up — not built now because nothing in this
  phase's own tests required it, and building it speculatively would be the
  premature-complexity the project's principles warn against.

### Implementation

New in `forge-cluster` (previously an empty placeholder module): `NodeId`,
`NodeAddress` (records), `ConsistentHashRing` (SHA-256-based ring — not
`Object.hashCode()`, which the JDK doesn't guarantee stable across
versions — with virtual nodes, immutable `withNode`/`withoutNode`),
`ClusterTopology` (ring + node→address map), `PartitionedForgeClient`
(client-side routing per ARCHITECTURE.md §3.5, lazy per-node connections),
`PartitionRebalancer` (data migration on topology change).

### Bug found and fixed during adversarial review

**`PartitionedForgeClient` let multiple threads share one node's
`ForgeClient` connection with no synchronization.** `ForgeClient` is
explicitly documented (Phase 5) as not thread-safe — one request in flight
per connection. The concurrency test written specifically to probe this
(`manyThreadsRoutingToTheSameNodeThroughOneClientNeverCorruptTheWire`, and
independently `PartitioningIntegrationTest`'s concurrent-routing test)
caught it immediately: two threads' PUT/GET calls interleaved their bytes on
the same socket, corrupting a frame, surfaced as a nonsensical
`ForgeServerException: invalid key length 6913`. **Fixed** by synchronizing
each use of a node's connection on that connection object, serializing
concurrent callers per node (callers targeting *different* nodes are
unaffected — each node has its own lock). Documented as a real, named
tradeoff in `PartitionedForgeClient`'s class Javadoc: same-node concurrent
throughput is traded for correctness; a connection pool per node would
remove the queuing at real added complexity this phase's own requirements
didn't demonstrate a need for.

This is exactly the kind of bug the required "adversarial review" step
exists to catch — found by a test built to specifically probe the
concurrency model, not by code inspection alone.

### Tests

`mvn clean test` from the repo root — **322/322 tests pass, 0 failures, 0
errors**:

```
forge-common  : 42  (unchanged)
forge-storage : 198  (189 unchanged + 3 SSTableReader.scanAll() + 6 ConcurrentLsmKeyValueStore.keys())
forge-server  : 15  (11 unchanged + 4 ownership-predicate tests)
forge-client  : 11  (unchanged)
forge-cluster : 25  (1 smoke + 12 ConsistentHashRingTest + 6 ClusterTopologyTest
                      + 4 PartitionedForgeClientTest + 2 PartitionRebalancerTest)
forge-bench   : 14  (unchanged)
forge-tests   : 17  (12 unchanged + 5 PartitioningIntegrationTest)
BUILD SUCCESS
```

Every pre-Phase-7 test still passes unmodified — the additive-only changes
to `forge-storage`/`forge-server` did not need a single existing test to
change.

### Known limitations (Phase 7)

- No live topology propagation/gossip; no automatic rebalancing trigger.
- Ownership predicates are fixed at `ForgeServer` construction, not
  hot-swappable.
- Partition movement is a direct node-to-node copy, not a replica-based
  bootstrap (needs Phase 9/10).
- No partition-count/rebalance-cost benchmarking yet (that's Phase 12,
  building on this phase's `ClusterTopology`/`PartitionRebalancer`).
- Distribution fairness is statistical (verified in
  `ConsistentHashRingTest.keysDistributeAcrossAllNodesReasonably` with a
  generous band to avoid flakiness), not guaranteed exactly even.

## Phase 8: membership + failure detection (complete, 2026-09-03)

### Design decisions

**Fixed-timeout heartbeat detector, not phi-accrual or gossip** — per
DESIGN.md §10, which named fixed-timeout as the deliberate Phase 8 starting
point. Implemented as a **passive, clock-driven state machine with no
threads of its own**: `FailureDetector.tick()` re-evaluates every tracked
node against `Clock.instant()` and returns whatever transitions just
happened; nothing inside it sleeps, schedules, or spawns anything. This is
what makes its actual correctness fully testable with `java.time.Clock`
(the JDK's own injectable clock abstraction — no custom time abstraction
invented) and zero real sleeping in `FailureDetectorTest`'s 18 tests. A
separate class, `HeartbeatService`, is what actually drives it in
production (calls `tick()` on a schedule) and is where real time
necessarily appears — its own tests are explicitly real-time/real-UDP
integration tests, not correctness tests, and say so in their Javadoc.

**"N consecutive missed heartbeats" modeled as elapsed time ≥
suspectTimeout/deadTimeout**, rather than literally counting missed sends.
Equivalent in effect, more robust to jitter (one heartbeat arriving a little
early or late can't corrupt a miss-counter), and it's what makes the
detector's invariant — DEAD only after the full `deadTimeout` elapses, never
on a single missed beat — a single, simple comparison rather than counting
logic that itself would need proving correct.

**UDP, not the existing TCP client protocol, for heartbeats.** `forge-common`'s
`Request`/`Response` are sealed types with exhaustive switches across
`FrameCodec`/`ConnectionHandler`/`ForgeClient`; adding a heartbeat variant
would mean touching all three for a message shape that doesn't conceptually
belong to that client/data-plane protocol. A heartbeat is fire-and-forget
and tolerates loss by design — that's UDP's actual delivery semantics, not
something layered on top of TCP's guarantees that heartbeats don't need.
`java.net.DatagramSocket` is standard JDK — no new dependency, per this
project's binding constraint against pulling in a gossip/membership library.

**Join is required before a heartbeat counts** — `recordHeartbeat` for a
node that was never `join`ed (or that has since `leave`d) is ignored rather
than silently admitting it. `join`/`leave` are the one authoritative way a
node enters or exits this detector's view; an arbitrary UDP packet can't
make itself a "member."

**Reused `NodeId`/`NodeAddress` from Phase 7** rather than inventing
parallel identity types — "every node should have a stable identity" is
exactly what Phase 7's partitioning already needed and built.

### Failure model (stated precisely, per this phase's own requirement)

- **Heartbeat interval / timeouts**: caller-configured `Duration`s, no
  hardcoded default — `MembershipIntegrationTest` uses 30ms/200ms/400ms for
  a fast test; a real deployment would use larger, network-appropriate
  values (see docs/OPERATIONS.md, written during final documentation).
- **False-positive limitation, stated honestly**: a slow-but-alive node
  (GC pause, congested network, overloaded host) is indistinguishable from a
  dead one once it misses heartbeats for `deadTimeout` — this detector
  *will* mark it dead. Shorter timeouts detect real failures faster at the
  cost of more false positives; longer timeouts trade the other way. Nothing
  here softens that tradeoff (no phi-accrual, no adaptive timeout) — a fixed
  timeout, deliberately, matching DESIGN.md's phased plan.
- **Network delay**: heartbeats can be delayed or dropped independent of
  whether the sender is alive (`HeartbeatService`'s send path logs and
  continues on a failed send rather than treating it as fatal — loss is
  expected, not exceptional). Clocks across nodes are never compared to each
  other — every timestamp evaluated is local to the one detector doing the
  evaluating.
- **Consistency guarantee**: eventually consistent, not agreed-upon —
  `MembershipIntegrationTest` demonstrates this directly: node A's and node
  B's detectors are two independent, unsynchronized views that happen to
  converge on the same conclusion about node C, not a single shared source
  of truth.
- **Recovery**: `recordHeartbeat` unconditionally returns a node to ALIVE
  regardless of its prior state (SUSPECT or DEAD) — no separate
  "un-mark-dead" call, no restart required. Verified directly by
  `FailureDetectorTest`'s revival and repeated-flapping tests.

### Implementation

New in `forge-cluster`'s `com.forge.cluster.membership` package:
`NodeState` (enum: ALIVE/SUSPECT/DEAD), `MembershipEntry`, `MembershipChange`
(records), `FailureDetector` (the core detector), `HeartbeatService` (real
UDP sender/receiver + scheduled `tick()` driver). No changes to any other
module — Phase 8 needed nothing from `forge-storage`/`forge-server` beyond
what Phase 7 already added.

### Tests

`mvn clean test` from the repo root — **345/345 tests pass, 0 failures, 0
errors**:

```
forge-common  : 42   (unchanged)
forge-storage : 198  (unchanged)
forge-server  : 15   (unchanged)
forge-client  : 11   (unchanged)
forge-cluster : 47   (25 from Phase 7 + 18 FailureDetectorTest + 4 HeartbeatServiceTest)
forge-bench   : 14   (unchanged)
forge-tests   : 18   (17 from Phase 7 + 1 MembershipIntegrationTest)
BUILD SUCCESS
```

`FailureDetectorTest` covers every scenario this phase's requirements
listed by name: join, leave, healthy-stays-healthy, suspect transition, dead
transition, recovery from both SUSPECT and DEAD, repeated flapping (5
consecutive death/revival cycles), delayed-but-in-time heartbeat, duplicate
heartbeat (idempotent), heartbeat for a never-joined node (ignored), heartbeat
for a left node (ignored), two nodes tracked independently, and a real
16-thread concurrent-update stress test (200 ops/thread, no lost or
duplicated membership entries). `HeartbeatServiceTest` proves the real UDP
wiring: sustained real heartbeats keep two real nodes mutually ALIVE well
past what a single `join()` would cover on its own, a peer that stops
sending is genuinely detected as DEAD by a real peer over a real socket, and
`close()` both releases the UDP port and is idempotent.
`MembershipIntegrationTest` (in `forge-tests`) runs a real 3-node cluster
with both a `ForgeServer` (Phase 7 data plane) and a `HeartbeatService`
(Phase 8 membership plane) per node, kills one node outright, and confirms
both survivors' independent detectors converge on DEAD for it while still
seeing each other as ALIVE, and that only the killed node's own data shard
becomes unreachable.

No bugs were found during this phase's adversarial review — the detector's
single-lock design and the pure clock-driven state machine left little
surface for the concurrency bugs Phase 7 found; the concurrent-update stress
test and the real-UDP tests both passed on the first run.

### Known limitations (Phase 8)

- Fixed-timeout only — no phi-accrual, no adaptive timeouts, no gossip
  dissemination of membership state between nodes (each node's `FailureDetector`
  only knows about the peers it was explicitly told to `addPeer`/`join`).
- No authentication on heartbeat datagrams — any UDP packet containing a
  known `NodeId`'s bytes, from anywhere, is accepted as a valid heartbeat
  from that node. Acceptable for this phase (same trust model as the
  unauthenticated TCP client protocol since Phase 5), revisit before any
  real deployment.
- `HeartbeatService` does not yet feed membership changes into anything
  automatically — `PartitionRebalancer` (Phase 7) is still an explicit,
  manually-invoked operation; nothing currently triggers it when a
  `FailureDetector` observes a DEAD node. Wiring that up is a natural, but
  deliberately deferred, piece of Phase 9/10's failover story.
- Membership view is per-node/per-detector, never reconciled cluster-wide —
  by design (§ above), but worth restating: there's no single place to ask
  "what does the cluster agree the membership is."

## Phase 9: replication (complete, 2026-09-04)

### Design decisions

**Asynchronous leader-follower replication, built directly on the existing
WAL rather than a parallel log** — per DESIGN.md §2/§6's already-decided
model. The unit of replication is literally `WalRecord` (`forge-storage`'s
existing Put/Delete type); a follower applies a leader's records under the
leader's own sequence numbers, never generating its own.

**Real, genuine architectural gaps this phase exposed in already-shipped
`forge-storage` code — found, understood, and fixed surgically, exactly per
this project's standing rule for when a later phase may touch a frozen one:**
1. `WriteAheadLog.appendPut`/`appendDelete` always self-assign a sequence
   number — there was no way to durably apply a record under someone else's
   numbering. **Added** `WriteAheadLog.appendReplicated(WalRecord)`
   (returns `APPLIED`/`ALREADY_APPLIED`/`GAP_DETECTED` — new enum
   `ReplicationOutcome`) and `nextSequenceNumber()`. Purely additive; every
   existing method and its tests are untouched.
2. `ConcurrentLsmKeyValueStore` had no way to apply an externally-numbered
   record into its MemTable+WAL under the same locking discipline `put`/
   `delete` use. **Added** `applyReplicated(WalRecord)` and
   `lastAppliedSequenceNumber()`, mirroring `put`/`delete`'s exact
   write-lock-then-defer-flush shape.
3. There was no way for a leader to learn about its own writes as they
   happen (needed to forward them to followers), and no way to read the
   WAL's *current* contents on demand (`recoveredRecords()` is a fixed
   snapshot from whenever the log was opened, by design, since Phase 2).
   **Added**: a replication-listener hook on `ConcurrentLsmKeyValueStore`
   (`addReplicationListener`/`removeReplicationListener`, notified *after*
   `stateLock` is released — the same place flush's disk I/O is already
   deferred to, so a slow listener can't block other writers) and
   `WriteAheadLog.currentRecords()` / `ConcurrentLsmKeyValueStore.currentWalRecords()`
   (an on-demand fresh re-scan, reusing the existing, already-tested
   `recover()` logic).

Each of these is additive-only (new methods, zero changed signatures,
zero changed behavior for any existing caller) and independently unit
tested; the full pre-Phase-9 `forge-storage` test suite passed unmodified
throughout.

**A separate TCP protocol for replication, not the client wire protocol** —
same reasoning as Phase 8's heartbeats: `forge-common`'s `Request`/`Response`
are sealed, exhaustively-switched client/data-plane types; log shipping is
a different kind of traffic (a continuous stream, not one request/one
response) that doesn't belong in that protocol. TCP, not UDP (unlike
heartbeats) — replication needs reliable, ordered delivery; heartbeat loss
is fine, a replication gap is not.

**Catch-up scope boundary, stated honestly**: a follower catches up on
whatever is currently in the leader's WAL and nothing more —
`currentWalRecords()` never includes anything already flushed to an
SSTable. A follower behind further than that needs a full bootstrap, which
is explicitly Phase 10's job ("snapshot or SSTable transfer"), not this
phase's.

**Durability guarantee, stated precisely, not oversold**: this is
asynchronous replication — a leader's local write acks (returns from
`put`/`delete`) as soon as its own WAL fsync completes, before any follower
has necessarily seen it. A write acked to a caller can be lost if the
leader crashes before shipping it to any follower.
`writesSucceedLocallyOnTheLeaderEvenWithZeroFollowersConnected` verifies
this directly. Synchronous replication (ack only after ≥1 follower
acks) — DESIGN.md §2's opt-in alternative — is **not implemented**: it
would mean changing `put`/`delete`'s own ack timing, a Phase-4-reviewed code
path deliberately left untouched this phase. Recorded as a scope boundary.

### The catch-up/live-tail race, and why it can't lose or duplicate records

The hardest correctness question this phase raised: when a follower
connects, how does it get both "everything already written" (the backlog)
and "everything written from now on" (the live tail) without a gap or a
double-send in between? Solved by registering the replication listener
*before* reading the backlog — so anything written in between lands in the
backlog scan, the live queue, or (under the race) both — then having the
live-tail sender track the highest sequence number actually sent and skip
anything at or below it. Proven, not just argued: `ReplicationIntegrationTest`
includes a test that runs a continuous background writer on the leader
*while* a follower connects and catches up, then verifies the follower's
final on-disk state (after a restart) exactly matches what was written —
every backlog key, every live-tail key up to wherever the writer stopped,
no gaps, no duplicates. It passed on the first run — the design was worked
through on paper (see the `ReplicationServer` class Javadoc) before writing
the code, the same discipline every prior phase's genuinely hard
correctness question got.

### Implementation

New `com.forge.cluster.replication` package in `forge-cluster`:
`ReplicaState` (record), `ReplicationWireFormat` (package-private codec),
`ReplicationServer` (leader: accepts followers, streams catch-up + live
tail, tracks per-follower acked sequence number for lag), `ReplicationFollower`
(follower: connects, requests catch-up from its own `lastAppliedSequenceNumber()`,
applies every record via `applyReplicated`, acks periodically). Additive
changes to `forge-storage` as described above; no changes to
`forge-server`/`forge-client`/`forge-common`.

### Tests

`mvn clean test` from the repo root — **381/381 tests pass, 0 failures, 0
errors**:

```
forge-common  : 42   (unchanged)
forge-storage : 225  (198 from Phase 7 + 10 WAL appendReplicated/nextSequenceNumber tests
                       + 17 ConcurrentLsmKeyValueStore applyReplicated/currentWalRecords/listener tests)
forge-server  : 15   (unchanged)
forge-client  : 11   (unchanged)
forge-cluster : 56   (47 from Phase 7/8 + 9 ReplicationIntegrationTest)
forge-bench   : 14   (unchanged)
forge-tests   : 18   (unchanged from Phase 8)
BUILD SUCCESS
```

`ReplicationIntegrationTest` proves, with real sockets and real on-disk
stores: writes on the leader reach a connected follower; a follower catches
up on history written entirely before it ever connected; deletes replicate
correctly; **writes concurrent with a follower's catch-up are never lost or
duplicated** (the race test above); a disconnected follower never blocks the
leader from continuing to serve writes; a follower can "crash," restart
(reopening the same directory, recovering via the durable WAL), reconnect,
and finish catching up with nothing duplicated; replication lag is tracked
and converges to exactly zero once caught up; a leader disappearing
entirely does not corrupt or hang a connected follower's own state.

No bugs were found in the new network/replication code during adversarial
review — the concurrent catch-up race test (the scenario most likely to
expose one) passed on its first run, which is attributed directly to
working through the race on paper before writing the code (see the design
decisions above), not to an absence of looking for one.

### Known limitations (Phase 9)

- **Asynchronous only** — no synchronous/quorum-ack replication mode (see
  durability section above). A future implementation would need to change
  `ConcurrentLsmKeyValueStore.put`/`delete`'s ack timing, which is out of
  this phase's scope by design.
- **Catch-up is WAL-only** — cannot bring a far-behind or brand-new follower
  up to date once data has been flushed to SSTables; that needs Phase 10's
  bootstrap/snapshot transfer.
- **No automatic leader election or failover** — replication here assumes a
  fixed, externally-designated leader; a leader dying just stops the stream
  (verified not to corrupt the follower), it does not trigger anything.
  That's Phase 14's job.
- **One leader, many followers, no follower-to-follower or chained
  replication** — `applyReplicated` deliberately does not notify
  replication listeners (verified by
  `applyReplicatedDoesNotNotifyReplicationListeners`), so a follower cannot
  itself re-forward what it receives. Sufficient for this phase's scope;
  revisit only if a real need for replication trees appears.
- **No authentication** on the replication protocol — same trust model as
  the heartbeat and client protocols since Phases 5/8, revisit before any
  real deployment.
- Replication lag is exposed as a raw sequence-number delta
  (`ReplicaState`), not yet surfaced through any metrics/admin interface —
  that's the Observability section of final hardening.

## Phase 10: recovery (complete, 2026-09-04)

### Scope: what already existed vs. what was genuinely new

Most of "recovery" was already real and tested before this phase started:
WAL+SSTable replay on restart (Phase 2–4), and a follower reconnecting after
a "crash" and catching up via the WAL (Phase 9's
`aFollowerCanReconnectAfterRestartAndFinishCatchingUpWithoutDuplicatingAnything`).
What was genuinely missing, and what this phase actually built: **bootstrap
for a node too far behind for WAL-based catch-up alone** — Phase 9's
`ReplicationServer`/`ReplicationFollower` explicitly cannot help once a
leader has flushed past what a follower needs (`currentWalRecords()` never
includes anything already in an SSTable). That gap is exactly what DESIGN.md
names as needing "snapshot or SSTable transfer."

### Design decisions

**Resolved key-value snapshot, not raw SSTable file transfer** — simpler,
more portable, no cross-node file-format coupling to reason about (even
though the format would be identical). A snapshot is a `(watermark,
key→value map)` pair; the destination bulk-loads it as a single new
SSTable, using **`SSTableWriter.write` completely unchanged** (the exact
same crash-safe temp-file → `force()` → atomic-rename path a normal flush
already uses) and **`WriteAheadLog.ensureNextSequenceNumberAtLeast`
completely unchanged** (the exact mechanism Phase 3 built for reconciling a
WAL with a pre-existing SSTable watermark). The new
`ConcurrentLsmKeyValueStore.loadSnapshot(Map<String,byte[]>, long watermark)`
method is essentially "compose these two already-reviewed primitives once,"
not new persistence logic.

**Chosen specifically so `lastAppliedSequenceNumber()` becomes exactly
`watermark + 1`** after loading — so a `ReplicationFollower` started
immediately afterward resumes from precisely where the snapshot left off.
Verified directly (`aFollowerCanResumeIncrementalReplicationSeamlesslyAfterLoadingASnapshot`,
and the network-level `aBootstrappedNodeResumesReplicationSeamlesslyWithNoGapAndNoDuplication`).

**Snapshot consistency, stated precisely — not oversold**: the watermark is
captured first, then keys/values are read via the store's already-thread-safe
methods without blocking concurrent writers. A key written or deleted
*during* the transfer can, rarely, end up on either side of the nominal
watermark. This is provably harmless, not just hoped to be: any record a
snapshot happens to catch early is simply re-applied (idempotent effect,
same value) once replication streams past it, and anything the snapshot
missed is corrected the moment its own record streams past — the
destination's final state converges to correct either way. Proven, not just
argued: `bootstrapWhileConcurrentWritesAreHappeningStillConvergesToTheCorrectFinalState`
runs a live writer racing an actual bootstrap-then-replicate sequence and
verifies exact final-state convergence.

**Crash safety of an interrupted transfer**: `SnapshotClient` reads the
*entire* stream into memory first and calls `loadSnapshot` only after
seeing the end-of-stream marker. A connection dropped at any point before
that never calls `loadSnapshot` at all — the destination is left exactly as
empty as it started, safely retryable from scratch. Verified directly
(`anInterruptedTransferLeavesTheDestinationCompletelyUntouched`, using a
hand-rolled server that sends a partial stream and closes without the
terminator).

**Epochs/generations**: not implemented this phase. DESIGN.md's Phase 7
contract and this phase's request both raise "prevent a stale node from
overwriting newer data" — but the mechanism that actually creates that
danger (automatic leader election, so a demoted-but-still-writing old
leader can collide with a new one) doesn't exist until Phase 14. Without
election, this project's replication model has exactly one, fixed,
externally-designated leader; a rejoining follower is never mistaken for
authoritative, since it only ever pulls data, never pushes as a leader
would. Building a generation/epoch mechanism now, ahead of the actual
danger it would guard against, would be exactly the premature complexity
this project's principles warn against — noted here as deliberately
deferred to Phase 14, not overlooked.

### A real bug found by adversarial review, and why it was fixed in the test, not the storage engine

While testing bootstrap racing a concurrent write workload, an early,
zero-delay version of `bootstrapWhileConcurrentWritesAreHappeningStillConvergesToTheCorrectFinalState`
took **over ten minutes** instead of the expected sub-second run (caught
because `mvn test` itself stalled, not because of a wrong result).

**Root cause, precisely identified**: `SnapshotServer.sendSnapshot()` calls
`store.get(key)` once per key — hundreds of separate `stateLock` read-lock
acquisitions for one snapshot — by deliberate design, so a snapshot in
progress never holds one long lock that would block writers (see
`SnapshotServer`'s Javadoc). `ConcurrentLsmKeyValueStore.stateLock` is a
**non-fair** `ReentrantReadWriteLock` (Phase 4, original design). A test
writer thread doing `put()` in a zero-delay tight loop re-acquires the write
side back-to-back with no gap — and a non-fair `ReentrantReadWriteLock`
provides no bound on how long a waiting reader can be starved under
sustained, uninterrupted writer pressure. Phase 9's equivalent race test
never hit this because `ReplicationServer` streams via a *push* listener,
never re-acquiring `stateLock` per key the way `SnapshotServer`'s *pull*
design does — the difference in architecture is exactly why one test starved
and the other didn't.

**Fixed by pacing the test's writer** (a small, realistic delay between
writes — every real client write has at least a network round trip before
the next one; a zero-delay tight loop in one process is not a realistic
workload) — **not by changing `stateLock`'s fairness mode.** Making the lock
fair would be a genuine, defensible fix for the underlying starvation
*possibility*, but it is a change to Phase 4's already-reviewed concurrency
primitive with a real, uncharacterized performance cost (fair locks carry
more overhead even when uncontended), and Phase 6's entire benchmark suite
characterized this store's throughput assuming non-fair semantics. Changing
it here, inside Phase 10's bootstrap work, would be exactly the kind of
unreviewed, unbenchmarked change to a frozen phase this project's freeze
rule exists to prevent — recorded below as a known, real, evidenced
limitation for a future dedicated design pass, not silently patched around
or hidden.

### Implementation

New `com.forge.cluster.recovery` package in `forge-cluster`: `SnapshotServer`
(runs on any node willing to bootstrap others), `SnapshotClient` (one-shot
fetch-and-load). One additive method on `forge-storage`'s
`ConcurrentLsmKeyValueStore`: `loadSnapshot(Map<String,byte[]>, long)`. No
other changes to `forge-storage`/`forge-server`/`forge-client`/`forge-common`.

### Tests

`mvn clean test` from the repo root — **395/395 tests pass, 0 failures, 0
errors**:

```
forge-common  : 42   (unchanged)
forge-storage : 233  (225 from Phase 9 + 8 loadSnapshot tests)
forge-server  : 15   (unchanged)
forge-client  : 11   (unchanged)
forge-cluster : 62   (56 from Phase 9 + 6 SnapshotTransferIntegrationTest)
forge-bench   : 14   (unchanged)
forge-tests   : 18   (unchanged)
BUILD SUCCESS
```

`SnapshotTransferIntegrationTest` proves: a full transfer from a populated
source; an empty-source transfer still produces a valid, watermark-correct
destination; a non-empty destination is rejected outright; an interrupted
transfer leaves the destination completely untouched; a bootstrapped node
resumes ordinary replication with no gap and no duplication; and bootstrap
racing a concurrent write workload converges to the exact correct final
state.

### Known limitations (Phase 10)

- **Non-fair `stateLock` can starve a sequential multi-key reader under
  sustained, gap-free writer pressure** (see above) — real, evidenced,
  deliberately not fixed here; a candidate for a dedicated Phase 4
  concurrency-model revisit with its own benchmarking, not a Phase 9/10 fix.
- **No epoch/generation mechanism yet** — safe today only because there is
  no automatic failover (Phase 14) to create the stale-leader danger it
  would guard against; deliberately deferred, not overlooked.
- **Snapshot transfer is all-or-nothing per attempt** — no resumable/partial
  transfer for a very large dataset; a failed attempt restarts from scratch.
  Acceptable for this phase's scope; revisit if a real dataset size makes
  full retransmission impractical.
- **No automatic bootstrap triggering** — `SnapshotClient.fetchAndLoad` is
  an explicit operation a caller invokes; nothing watches membership
  (Phase 8) and automatically decides "this node needs a full bootstrap."
  That orchestration is exactly the kind of thing Phase 14's
  failover/consensus layer would own, not built ahead of it here.
- **No persistent cluster metadata** — a node doesn't durably remember its
  own partition assignments or cluster membership across a restart; every
  test in Phases 7–10 reconstructs topology/detector state at process
  startup. "Crash before metadata update" (one of this phase's requested
  crash-window scenarios) isn't yet applicable for exactly this reason —
  there is no persistent metadata update to crash before or after.

## Phase 11: chaos / fault injection (complete, 2026-09-04)

### Design decisions

**A real TCP proxy, not a mock or an in-process fake** — `FaultInjectingTcpProxy`
sits between a real client and a real server, forwarding actual bytes over
actual sockets, and can be told, programmatically and deterministically
(never randomly), to delay, drop, duplicate, or sever traffic. Every fault
in every scenario fires at an exact, controlled point in the test — the
explicit, reproducible design this phase's own requirements demanded
("this should NOT be random chaos for the sake of looking impressive").

**Reordering scoped to where "order" is a well-defined concept, not
re-derived at the byte level.** A byte-stream proxy has no notion of
"messages" to permute — naively swapping byte chunks would mostly just
produce corrupted frames, which is a different (and already well-tested)
failure mode, not a meaningful reordering test. Phase 9's
`WriteAheadLogTest.appendReplicatedRefusesToCreateAGap` and
`appendReplicatedIsIdempotentForAnAlreadyAppliedSequenceNumber` already
directly test FORGE's actual response to out-of-order and duplicate
delivery at the layer where sequence order is well-defined; Scenario D
builds on that rather than re-implementing a cruder version of the same
proof.

**Every scenario states its assumptions, safety property, and liveness
property up front, in its own Javadoc, before any code runs** — and none
claims more than Phases 7–10's actual architecture supports. In particular:
this project has no consensus or automatic failover yet (Phase 14), so no
scenario claims "zero data loss" for a write only ever acknowledged by a
leader that died before shipping it anywhere (Scenario A is explicit about
exactly this). Scenario C ("partition separates leader from majority") is
adapted honestly: with no quorum concept yet, "partitioned from the
majority" and "partitioned from its only follower" are the same scenario
here — noted as an adaptation, not silently treated as equivalent.

### Real bugs found by adversarial testing — all three in test assumptions about TCP/contract semantics, not in production code

1. **`dropNextGenuinelyDiscardsExactlyThatManyChunks` assumed two
   back-to-back writes would arrive as two separate reads.** TCP has no
   message boundaries; writing "dropped" then "delivered" with no gap risked
   the proxy reading both as one combined chunk, making `dropNext(1)`
   appear to drop everything (a real timeout, not a flaky assertion). Fixed
   by inserting a real delay between the two writes, long enough to
   guarantee the proxy has already read (and correctly discarded) the first
   chunk as its own `read()` call before the second is even sent.
2. **The partition test assumed severing a connection always throws an
   `IOException`.** `Socket.close()` on a healthy connection produces a
   graceful FIN, which the peer normally sees as a clean EOF (`read()`
   returning `-1`), not an exception. Fixed to accept either outcome as
   "severed" — both mean the same thing; only an unblocked, successful read
   of real data would mean `partition()` failed.
3. **Scenario C originally assumed a `ReplicationFollower` automatically
   reconnects once a partition heals.** It doesn't — Phase 9 deliberately
   scoped out auto-reconnect (see `ReplicationFollower`'s own class
   Javadoc); a severed connection is terminal, and the caller is expected
   to construct a fresh instance, exactly as Phase 9/10's own reconnect
   tests already do. The test's expectation, not the production contract,
   was wrong — fixed by having the scenario explicitly reconnect after
   `heal()`, matching the real, documented contract instead of an assumed
   one.

None of these were races or nondeterminism — each was a single,
reproducible, understood mistake in what the test itself assumed, caught
immediately by the test failing the same way every time, fixed once, and
confirmed fixed by rerunning.

### Implementation

New `com.forge.cluster.chaos` package in `forge-cluster`: `FaultInjectingTcpProxy`
(delay/drop/duplicate/partition, real sockets). No production code in any
other module was touched this phase — Phase 11 is purely a testing/verification
layer built entirely on top of Phases 7–10's already-shipped components.

### The six scenarios, and what each actually proved

| # | Scenario | Safety proved | Liveness proved |
|---|---|---|---|
| A | Leader crashes during active replication | Already-applied follower data survives intact | Follower's own store stays fully usable without the leader |
| B | Follower disappears and rejoins | No record lost or duplicated across the cycle | Full parity reached again within a bounded time |
| C | Network partition, leader↔follower | Follower's partial data stays consistent during the partition; leader writes never block | Reconnecting after heal reaches full parity |
| D | Messages significantly delayed | No corruption despite 200ms injected delay per hop | Replication still fully converges, just later |
| E | Node crashes during bootstrap recovery | Destination is never partially loaded — all-or-nothing | A fresh retry after the crash succeeds normally |
| F | Two nodes have temporarily stale membership | Neither detector ever throws or corrupts its state while views disagree | Both views converge back to agreement once heartbeats resume |

### Tests

`mvn clean test` from the repo root — **406/406 tests pass, 0 failures, 0
errors**:

```
forge-common  : 42   (unchanged)
forge-storage : 233  (unchanged)
forge-server  : 15   (unchanged)
forge-client  : 11   (unchanged)
forge-cluster : 73   (62 from Phase 10 + 6 ChaosScenarioTest + 5 FaultInjectingTcpProxyTest)
forge-bench   : 14   (unchanged)
forge-tests   : 18   (unchanged)
BUILD SUCCESS
```

### Known limitations (Phase 11)

- **UDP (heartbeats) has no fault-injection proxy** — `FaultInjectingTcpProxy`
  is TCP-only; Scenario F exercises membership staleness by simply not
  sending heartbeats (calling the detector directly) rather than through an
  injected network fault, since `HeartbeatService`'s UDP traffic has no
  analogous proxy built. A `DatagramSocket`-based equivalent would be a
  natural, small follow-up if UDP-specific fault injection (packet loss
  patterns beyond "heartbeats stopped entirely") becomes a real need.
- **Reordering is not implemented at the network layer** (see design
  decisions above) — covered at the WAL layer instead, which is where it's
  actually meaningful.
- **No fault-injection during WAL/SSTable disk I/O itself** — "disk/WAL
  corruption where safe to test" (this phase's own text) is already covered,
  but by Phase 2/3's existing corruption tests (bit-flipped bytes, torn
  writes, truncated files), not by anything new this phase added; this
  phase's proxy operates at the network layer, not the filesystem layer.
- **Scenarios are single-shot, not a fuzzing/property-based suite** — each
  scenario is one deterministic, hand-picked sequence of events, not a
  search over many possible interleavings. Sufficient to prove the six
  named behaviors; a broader exploration (e.g. Jepsen-style linearizability
  checking under many random-but-seeded fault schedules) is a natural,
  larger follow-up beyond this phase's scope.

## Phase 12: expanded benchmarking (complete, 2026-09-04)

### Design decisions

**Extend Phase 6's methodology to real multi-node clusters, never touch
Phase 6's own results.** Same client-observed, real-network, real-fsync
measurement discipline as Phase 6 (`System.nanoTime()` around
`ForgeClient`/`PartitionedForgeClient` calls, nearest-rank percentiles via
the existing `LatencyStats`), extended to actual multi-process clusters
built on Phases 7–10's already-shipped components (`ConsistentHashRing`,
`PartitionedForgeClient`, `ReplicationServer`/`ReplicationFollower`,
`SnapshotServer`/`SnapshotClient`). New types (`DurationBenchmarkResult`,
`DistributedBenchmarkConstants`, `DistributedBenchmarkRunner`) mirror
Phase 6's own (`BenchmarkResult`, `BenchmarkConstants`, `BenchmarkRunner`)
rather than replacing them — both runners, both CSV formats, and both
results files coexist.

**Smaller repeat count and dataset sizes than Phase 6, disclosed rather
than silently applied** — `REPEATS = 2` vs. Phase 6's 3, because every
Phase 12 experiment first builds a real cluster from scratch (unlike Phase
6, which mostly reuses one populated dataset across repeats), making the
full cost-benefit trade-off different. Recorded in
`DistributedBenchmarkConstants`'s own Javadoc and in BENCHMARKS.md §7's
methodology note, per this project's "never silently change methodology"
discipline.

**Four new experiments, continuing the existing E-numbering**: E12 (node
scaling), E13 (replication overhead), E14 (recovery time vs. data size —
closing out DESIGN.md's originally-planned E9), E15 (replica catch-up time
vs. backlog size).

### Real bug found by E15 and fixed — a genuine `ConcurrentModificationException` in Phase-7-added production code

E15's `waitUntil` helper polls a follower's `ConcurrentLsmKeyValueStore.keys()`
in a tight loop while replication is actively applying incoming records on a
different thread. This reliably threw:
```
java.util.ConcurrentModificationException
  at java.base/java.util.TreeMap$PrivateEntryIterator.nextEntry
  at com.forge.storage.ConcurrentLsmKeyValueStore.collectLiveKeys(ConcurrentLsmKeyValueStore.java:522)
  at com.forge.storage.ConcurrentLsmKeyValueStore.keys(ConcurrentLsmKeyValueStore.java:501)
```

**Root cause**: `keys()` snapshotted the `active`/`frozen` MemTable
*references* under `stateLock.readLock()`, released the lock, then iterated
their live `.entries()` (a mutable `TreeMap`) **outside** the lock that
protects them from concurrent writer mutation — unsafe for `active`
specifically, since a concurrent `put`/`applyReplicated` call mutates it in
place while a reader might be mid-iteration. This is a genuine
thread-safety bug in Phase 7's `keys()` addition, not a test bug — the kind
of bug this project's own "adversarial review" process exists to catch,
just caught one phase later than it was introduced because nothing before
E15 exercised `keys()` under sustained concurrent writes.

**Fix**: restructured `keys()` so the `active`/`frozen` MemTable key
collection happens **inside** the `stateLock.readLock()` critical section;
only the SSTable scan runs lock-free afterward, which remains safe because
`SSTableReader` is immutable once written. Added Javadoc explaining
precisely why the SSTable case is safe unlocked but the MemTable case is
not. Added a targeted regression test,
`keysNeverThrowsConcurrentModificationExceptionUnderSustainedConcurrentWrites`
(one writer doing 3,000 puts concurrently with four readers busy-polling
`keys()`), to `ConcurrentLsmKeyValueStoreTest`.

### Implementation

New `forge-bench` classes: `DurationBenchmarkResult` (CSV row for
"how-long-did-this-take" experiments, alongside the existing
`BenchmarkResult` for throughput/latency-series experiments),
`DistributedBenchmarkConstants`, `DistributedBenchmarkRunner` (orchestrates
E12–E15, writes `phase12-throughput-*.csv` and `phase12-duration-*.csv` to
`forge-bench/results/`). One production fix: `ConcurrentLsmKeyValueStore.keys()`
in `forge-storage`, described above.

### A design flaw found in E12 itself, fixed before trusting its numbers

E12's first version held **total** client concurrency fixed at 4 threads
regardless of node count, so a 4-node run spread those 4 threads across 4
nodes (~1 thread's worth of pressure per node) while a 1-node run put all 4
against one node — the configurations weren't comparable, and the run came
back with throughput reading as flat (~260–290 ops/sec) across every node
count, which would have been a misleading number to publish: it measured
"a fixed thread pool spread thinner," not "does adding nodes add
capacity." Fixed by scaling concurrency with node count
(`E12_CONCURRENCY_PER_NODE = 4` threads *per node*, fixed 100 ops per
thread, so total measured work scales with node count too). This is a
benchmark-methodology finding, not a FORGE bug — flagged and fixed the same
way Phase 6 §6's E4/E5 methodology bugs were: found by looking hard at the
output before trusting it, root-caused, fixed, re-run. See BENCHMARKS.md
§7.1 for the full writeup and the corrected (still non-obvious) result.

### Tests

`mvn clean test` from the repo root — **407/407 tests pass, 0 failures, 0
errors** (406 from Phase 11 + 1 new `keys()` regression test):

```
forge-common  : 42   (unchanged)
forge-storage : 234  (233 + 1 new keys() concurrency regression test)
forge-server  : 15   (unchanged)
forge-client  : 11   (unchanged)
forge-cluster : 73   (unchanged)
forge-bench   : 14   (unchanged — no new unit tests; DistributedBenchmarkRunner
                       is exercised by actually running it, same as Phase 6's
                       BenchmarkRunner)
forge-tests   : 18   (unchanged)
BUILD SUCCESS
```

The full `DistributedBenchmarkRunner` suite (E12–E15) was run end-to-end
twice this phase: once with E12's flawed fixed-concurrency design (numbers
discarded, not published — see above), and once after the fix, producing
the numbers reported in BENCHMARKS.md §7 (`phase12-throughput-2026-09-04T16-26-21.899646Z.csv`,
`phase12-duration-2026-09-04T16-26-21.899646Z.csv`).

### Known limitations (Phase 12)

- **E12's throughput-vs-node-count result is genuinely inconclusive about
  FORGE's own scalability**, because every node in this benchmark runs on
  one physical machine sharing one disk — see BENCHMARKS.md §7.1's full
  discussion. This is the same "no second machine available" limitation
  already disclosed for Phase 6's E4; it now also blocks a clean read of
  node-scaling, which E4 didn't need to worry about.
- **E13 found real, unexplained-by-direct-profiling replication overhead on
  the leader's own write latency as follower count grows** (roughly 2–2.5×),
  which is not obviously consistent with Phase 9's "local write never
  blocked by replication" design intent. A candidate mechanism
  (`notifyReplicationListeners` running synchronously on the writer's own
  thread before `put()` returns) is identified in BENCHMARKS.md §7.2 but
  not confirmed by profiling, and not fixed this phase — Phase 12's mandate
  was measurement, and changing Phase 9's locked replication notification
  path would need its own dedicated review under this project's freeze
  rule. Flagged as a concrete, well-scoped follow-up.
- **E6 (partition/rebalance cost) and E8 (replication ack policy) remain
  unbuilt** — `PartitionRebalancer.migrate` has never been timed, and
  FORGE's replication is async-only so there is no sync-mode comparison to
  run. E7 (failure-detector tuning) also remains unbuilt. None were part of
  this phase's four new experiments; still honestly "not run" rather than
  approximated.
- **E12/E13/E15's REPEATS=2 and smaller op counts than Phase 6** are a
  deliberate, disclosed trade-off (see Design decisions above), not an
  oversight — every Phase 12 experiment pays real cluster/replication setup
  cost per repeat that Phase 6's experiments mostly don't.

## Next task

**Phase 13: compaction + Bloom filters.** Not started. Continuing
sequentially per the master directive.
