# FORGE — Resume Material

Every metric below was actually measured by this project's own benchmark
harness (see BENCHMARKS.md for methodology and raw CSVs). Where no valid
measurement exists for a claim, the wording is qualitative rather than a
invented number — per this project's own standing rule.

## Title / one-liner

**FORGE — a distributed key-value database built from scratch in Java**,
implementing a WAL-backed LSM storage engine, consistent-hash
partitioning, leader-follower replication, crash recovery, deterministic
chaos testing, LSM compaction with Bloom filters, and Raft consensus —
every core mechanism hand-built and adversarially tested, not assembled
from existing libraries.

## Tech stack

Java 21 (virtual threads), Maven multi-module, JUnit 5, SLF4J/Logback.
No database, consensus, or coordination library used for any core
mechanism (RocksDB/LevelDB, etcd/ZooKeeper, Raft frameworks) — every one
is implemented directly.

## Resume bullets

**Technical version** (for a technical resume / engineering audience):

- Designed and implemented a distributed key-value store in Java from
  first principles: a checksummed write-ahead log, an LSM storage engine
  with MemTable/SSTable flush and full compaction, consistent-hash
  partitioning with virtual nodes, asynchronous leader-follower
  replication, and a real Raft consensus implementation for leader
  election and failover — 444 automated tests across 7 Maven modules, 0
  failures.
- Built and ran a real Raft consensus subsystem (terms, majority-vote
  election, AppendEntries log replication, the paper's Figure 8
  commit-safety rule) as the control plane for cluster leadership,
  validated with 13 fake-clock adversarial unit scenarios and 2 real-socket
  integration tests proving live leader-crash failover to a new term.
- Implemented LSM compaction and per-SSTable Bloom filters; measured (not
  estimated) a 90% reduction in on-disk footprint and a 3.4× reduction in
  miss-lookup latency on a synthetic overwrite-heavy workload.
- Built a deterministic (non-random) network fault-injection framework
  (delay/drop/duplicate/partition over real TCP) and used it to prove six
  named safety/liveness properties under leader crashes, network
  partitions, and message delay.
- Found and fixed real concurrency and correctness bugs under
  adversarial testing, including a `ConcurrentModificationException` in
  production code triggered by sustained concurrent load, and a Raft
  leader-election ordering bug (`nextIndex` computed before the
  leader's own log entry was appended) caught by a unit test before it
  ever reached a live cluster.
- Measured and documented every performance characteristic honestly,
  including a benchmark-design bug found and fixed in the project's own
  node-scaling experiment (before publishing corrected, still-inconclusive
  results) and an unresolved, disclosed anomaly in replication overhead
  scaling.

**ATS-friendly version** (keyword-forward, shorter):

- Built a distributed key-value database in Java: write-ahead logging,
  LSM storage engine, compaction, Bloom filters, consistent hashing,
  leader-follower replication, crash recovery, chaos/fault-injection
  testing, and Raft consensus for leader election and automated failover.
- 444 automated tests (JUnit 5) across storage, networking, partitioning,
  replication, recovery, chaos, and consensus; real multi-process/real-socket
  integration tests, not simulated in-process shortcuts.
- Implemented and benchmarked LSM compaction and Bloom filters: 90%
  on-disk space reclamation, 3.4× faster negative lookups (measured).
- Implemented Raft consensus (terms, leader election, log replication,
  commit-safety invariants) with real-socket multi-node election and
  crash-failover tests.
- Used Java 21 virtual threads for connection handling and RPC dispatch;
  Maven multi-module architecture with clean module boundaries.

## What's deliberately *not* claimed here

No latency/throughput number outside what BENCHMARKS.md actually
measured, no "production-grade," "enterprise-ready," "linearizable," or
"fault-tolerant" claims beyond what's precisely scoped in
docs/CONSISTENCY.md and docs/FAILURE_MODEL.md — a known, open gap (no
fencing between the control and data plane; no persistent Raft state) is
part of the honest story this project tells about itself, not something
to omit from a resume conversation if asked directly.
