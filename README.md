# FORGE

A distributed key-value database, built incrementally, from scratch, in Java.

**Status: all 8 core phases complete** (partitioning, membership/failure
detection, replication, recovery, chaos testing, distributed benchmarking,
compaction/Bloom filters, and Raft consensus) — 444/444 tests passing. See
[PROGRESS.md](PROGRESS.md) for the full phase-by-phase log, including real
bugs found and fixed along the way, and this README's [Limitations](#21-limitations)
section for what's honestly not done yet.

## 1. What FORGE is, and why it exists

FORGE is a learning-by-building project: every core distributed-systems
mechanism — write-ahead logging, an LSM storage engine, concurrency
control, a wire protocol, consistent-hash partitioning, failure detection,
leader-follower replication, crash recovery, compaction, and Raft
consensus — is implemented by hand rather than pulled in as a library
(RocksDB, etcd, Raft frameworks), so each concept is actually understood,
not just wired together. See [docs/DESIGN.md §9](docs/DESIGN.md#9-implementation-constraints)
for the exact, binding list of what's built vs. given infrastructure (only
the JDK standard library and JUnit).

Every number in this project is real. [BENCHMARKS.md](docs/BENCHMARKS.md)
never contains an estimated or invented figure — every throughput,
latency, or amplification number was produced by actually running the
harness in this repository, with the raw CSV kept alongside it.

## 2. Architecture, at a glance

```
                    ┌─────────────┐         ┌─────────────┐
   client ─────────▶│ ForgeServer │◀───────▶│ ForgeServer │◀── client
  (ForgeClient /     │   node A    │  Raft   │   node B    │
   Partitioned-      │ (leader)    │ (elect/ │ (follower)  │
   ForgeClient)      └──────┬──────┘  fail-  └──────┬──────┘
                             │         over)         │
                      WAL + LSM engine         WAL + LSM engine
                      (MemTable, SSTables,     (MemTable, SSTables,
                       compaction, Bloom        compaction, Bloom
                       filters)                 filters)
                             │                        ▲
                             └───── replication ──────┘
                              (async, WAL-sequence-numbered)

   Consistent-hash ring maps each key to exactly one partition/leader.
   HeartbeatService/FailureDetector track liveness between all nodes.
```

Full component-by-component detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Guarantees and their honest limits: [docs/CONSISTENCY.md](docs/CONSISTENCY.md)
and [docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md).

## 3. Module structure

```
forge-common/   Wire protocol (Request/Response), framing, shared types.
forge-storage/  MemTable, WAL, SSTables, compaction, Bloom filters, and a
                minimal single-node REPL — the durable storage engine.
forge-server/   TCP server + connection handling on top of forge-storage.
forge-client/   Client library (ForgeClient).
forge-cluster/  Consistent hashing/partitioning, membership & failure
                detection, replication, snapshot recovery, chaos/fault
                injection, Raft consensus.
forge-bench/    Load generators + latency/throughput/amplification
                measurement — every number in BENCHMARKS.md comes from here.
tests/          Cross-module integration tests (real multi-node processes).
```

## 4. Storage engine

Each node's data lives in a WAL-backed LSM tree
(`ConcurrentLsmKeyValueStore`): writes land in an in-memory `MemTable`
after a synchronous WAL append+fsync; once the MemTable crosses a size
threshold it freezes and flushes to an immutable, sorted, on-disk SSTable.
Reads check the active MemTable, then the frozen one (if a flush is in
flight), then SSTables newest-to-oldest. See
[docs/ARCHITECTURE.md §3.1](docs/ARCHITECTURE.md#31-storage-engine-single-node-durable).

## 5. Write-ahead log

Every mutation is appended to a checksummed, length-prefixed binary WAL
and `fsync`'d before being acknowledged — the single load-bearing
durability guarantee everything else in this project rests on. Corruption
in the WAL's tail (a torn write from a crash) is detected via CRC32 and
truncated, since it was never acknowledged; see
[docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md) for why WAL and SSTable
corruption are handled differently.

## 6. LSM tree, compaction, and Bloom filters

Flushing produces immutable SSTables (header + checksummed framed
records). Once enough SSTables accumulate, a full compaction merges every
live table into one, keeping only the newest version of each key and
dropping tombstones that no older generation could still need (Phase 13).
Each SSTable carries a Bloom filter sidecar (tombstone-inclusive, so a
deleted key's own table is never incorrectly skipped) that lets a point
lookup skip a table entirely when a key is definitely absent. Measured,
not claimed: compaction reclaimed 90% of a synthetic workload's on-disk
footprint and cut miss-lookup latency 3.4× — see
[BENCHMARKS.md §8](docs/BENCHMARKS.md#8-phase-13--compaction-readwrite-amplification-e16).

## 7. Concurrency

One `ReentrantReadWriteLock` per store guards exactly the in-memory
generation pointers (`active`/`frozen`/`sstables`); all slow disk I/O
(flush, compaction) runs lock-free after a brief locked hand-off, so
concurrent readers/writers are never blocked on disk. SSTables are
reference-counted (`acquire`/`release`/`retire`) so compaction can safely
delete a merged-away file while another thread is mid-scan of it — a real
hazard Phase 13 introduced and had to solve. See
[PROGRESS.md](PROGRESS.md)'s Phase 4 and Phase 13 sections.

## 8. Wire protocol

A small, sealed, exhaustively-switched binary protocol
(`forge-common`'s `Request`/`Response`) for GET/PUT/DELETE, framed over
TCP with length-prefixing (`FrameCodec`). Extended additively, never
breaking compatibility — e.g. Phase 7 added an `ERROR_NOT_OWNER` response
code for partition-routing errors without touching the existing
GET/PUT/DELETE cases.

## 9. Partitioning

Consistent hashing with virtual nodes (SHA-256-based, 128 virtual nodes
per physical node) maps each key to exactly one owning node
(`ConsistentHashRing`/`ClusterTopology`). `PartitionedForgeClient` routes
each request directly to the owning node; a node that receives a request
for a key it doesn't own rejects it (`ERROR_NOT_OWNER`) rather than
silently serving or forwarding it. `PartitionRebalancer` migrates a key
range between nodes safely (PUT-to-destination before DELETE-from-source).

## 10. Membership & failure detection

A fixed-timeout heartbeat detector (`FailureDetector`, real UDP
datagrams via `HeartbeatService`) tracks ALIVE/SUSPECT/DEAD per node.
Deliberately a passive, clock-driven state machine with no threads of its
own, so its entire state-machine logic is tested with a fake clock and
zero real sleeping. Honest, named tradeoff: a slow-but-alive node **will**
eventually be misclassified dead — see
[docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md).

## 11. Replication

Asynchronous leader-follower replication, built directly on WAL sequence
numbers (`ReplicationServer`/`ReplicationFollower`) — no separate
replication log. A leader's local write is never blocked waiting for a
follower; `ReplicationOutcome` (APPLIED/ALREADY_APPLIED/GAP_DETECTED)
makes catch-up idempotent and gap-safe. **Measured cost of this
asynchrony**: leader-local write latency roughly doubles with one follower
and nearly triples with two — a real, investigated finding, not asserted
— see [BENCHMARKS.md §7.2](docs/BENCHMARKS.md#72-e13--replication-overhead).

## 12. Recovery

A restarted node replays its WAL from the last flushed watermark. A node
too far behind for WAL replay alone bootstraps from a full snapshot
(`SnapshotServer`/`SnapshotClient`, reusing the same
temp-file/`force()`/atomic-rename crash safety as a normal flush) before
resuming WAL-based catch-up exactly where the snapshot left off.

## 13. Chaos / fault injection

A real TCP proxy (`FaultInjectingTcpProxy`) that deterministically (never
randomly) delays, drops, duplicates, or severs traffic between a real
client and server. Six named scenarios (leader crash, follower rejoin,
network partition, message delay, bootstrap-crash, stale membership) each
state their assumptions, safety property, and liveness property up front
— see [PROGRESS.md](PROGRESS.md)'s Phase 11 section for what each proved.

## 14. Consensus & automated failover (Raft)

A real Raft implementation (`forge-cluster`'s `consensus` package) —
terms, majority-vote leader election, AppendEntries replication, and the
paper's Figure 8 commit-safety rule (an entry from an older term is never
committed by direct majority count alone). This is genuinely **not** a
rename of §11's replication: Raft's log here carries only a
one-entry-per-election control marker; real KV writes still flow
exclusively through the replication path above. Ten-plus adversarial
scenarios (split votes, stale leaders, log inconsistency, the Figure 8
trap, and more) are proven with a fake clock in `RaftNodeTest`; a real
3-node election and crash-failover are proven over real sockets in
`RaftClusterIntegrationTest`. **Honestly scoped**: no persistent Raft
state, and not yet wired into live data-plane failover — see
[PROGRESS.md](PROGRESS.md)'s Phase 14 section.

## 15. Benchmarks

Every number is measured, client-observed, and reproducible from this
repository — never estimated. Highlights (full detail and methodology in
[BENCHMARKS.md](docs/BENCHMARKS.md)):

- Single-node PUT is fsync-bound at ~250 ops/sec regardless of client
  concurrency (§3) — the WAL's single-writer serialization, exactly as
  designed.
- GET scales with concurrency up to available CPU cores (~105K ops/sec at
  16 connections) before flattening (§3).
- Replica catch-up time scales linearly with backlog size at almost
  exactly the same ~250-260 ops/sec ceiling as any other fsync-bound write
  path (§7.4) — a clean, explainable result extending the fsync-ceiling
  finding into replication.
- Compaction reclaimed 90% of a synthetic overwrite-heavy workload's
  on-disk footprint and cut miss-lookup latency 3.4× (§8).
- Node-scaling on a single machine does **not** show clean linear
  throughput scale-up even with per-node client concurrency held constant
  — investigated honestly in §7.1 as likely single-disk fsync contention,
  not asserted as a FORGE design flaw, and explicitly flagged as something
  this benchmark (single machine, no second machine available) cannot
  resolve on its own.

## 16. How to build and test

Requires Java 21+ and Maven.

```bash
mvn clean install    # builds all 7 modules, runs all tests
```

444 tests across `forge-common`, `forge-storage`, `forge-server`,
`forge-client`, `forge-cluster`, `forge-bench`, and `tests` (0 failures).

## 17. How to run a single node

```bash
mvn -pl forge-server -am install -DskipTests
mvn -pl forge-server exec:java -Dexec.args="<dataDirectory> <port>"
# both arguments optional — defaults to ./forge-data and port 7070
```

Then, from your own code or a REPL, talk to it with `ForgeClient`:

```java
try (ForgeClient client = ForgeClient.connect("localhost", 7070)) {
    client.put("hello", "world".getBytes(StandardCharsets.UTF_8));
    Optional<byte[]> value = client.get("hello");
}
```

Inspect a **stopped** node's storage-engine state:

```bash
mvn -pl forge-server exec:java -Dexec.args="status <dataDirectory>"
```

## 18. How to run a multi-node cluster / partitioning / replication / failover

There is currently no single "launch a full N-node cluster" CLI (see
[Limitations](#21-limitations)) — but every one of these behaviors is
real, already wired together, and demonstrated end-to-end by the
integration tests below, each spinning up genuine separate
`ForgeServer`/`ReplicationServer`/`RaftCluster` instances with real socket
traffic between them:

```bash
# consistent-hash routing across real nodes:
mvn -pl tests -am test -Dtest=PartitioningIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
# leader-follower WAL streaming:
mvn -pl forge-cluster -am test -Dtest=ReplicationIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
# bootstrap-from-snapshot recovery:
mvn -pl forge-cluster -am test -Dtest=SnapshotTransferIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
# real election + leader-crash failover:
mvn -pl forge-cluster -am test -Dtest=RaftClusterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

See [docs/DEMO.md](docs/DEMO.md) for a scripted walkthrough of these, in
order, with what to look for in the output.

## 19. How to run the failure-injection demos

```bash
mvn -pl forge-cluster -am test -Dtest=ChaosScenarioTest -Dsurefire.failIfNoSpecifiedTests=false
```

Runs all six named chaos scenarios (leader crash, follower rejoin,
network partition, message delay, bootstrap-crash, stale membership)
against real sockets with deterministic (not random) fault injection.

## 20. How to run the benchmarks

```bash
mvn install -DskipTests
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.BenchmarkRunner              # Phase 6: single-node baselines
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.DistributedBenchmarkRunner    # Phase 12: multi-node/replication
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.CompactionBenchmarkRunner     # Phase 13: compaction amplification
```

Each writes a timestamped CSV to `forge-bench/results/` and prints a
human-readable summary — see [BENCHMARKS.md](docs/BENCHMARKS.md) for what
the numbers mean.

## 21. Limitations

Stated plainly, not softened — see [docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md)
and [docs/CONSISTENCY.md](docs/CONSISTENCY.md) for full detail on the
first three:

- **No fencing/epoch mechanism** — two nodes can both believe themselves
  leader of the same partition after a split-brain event; nothing today
  prevents it. Raft (§14) is the mechanism that *would* close this once
  wired into the data plane, which it isn't yet.
- **Replication is async-only** — a write acknowledged by a leader can be
  lost if the leader crashes before replicating it anywhere.
- **Raft has no persistent state** — a node that crashes and restarts
  mid-term rejoins as a brand-new participant; a narrow theoretical
  safety gap relative to the paper, disclosed in `RaftNode`'s Javadoc.
- **No authentication, authorization, or transport encryption** — every
  TCP connection is trusted.
- **No multi-node CLI launcher** — real multi-node behavior is proven by
  the integration tests (§18), not a standalone "start a cluster" command.
- **No network-queryable admin/metrics endpoint** — real metrics exist as
  Java accessors (`RaftCluster.currentLeader()`, `FailureDetector.snapshot()`,
  `ConcurrentLsmKeyValueStore.status()`) but aren't exposed remotely; see
  [docs/OPERATIONS.md](docs/OPERATIONS.md).
- **Compaction is full-table-rewrite, not leveled**, and runs
  synchronously inline rather than on a background thread.
- **No cluster membership changes to a live Raft group** (no joint
  consensus) — the peer set is fixed at construction.
- Every phase's own, more specific limitations are recorded in
  [PROGRESS.md](PROGRESS.md) as they were found.

## 22. Future work

In roughly the order it would naturally continue:

1. Wire Raft's `isConfirmedLeader()` into `ForgeServer`'s ownership
   predicate and `ReplicationServer`/`ReplicationFollower`'s lifecycle for
   real, automated data-plane failover — the single biggest gap between
   "consensus exists" and "consensus controls the system."
2. Persistent Raft state (mirroring the WAL's own crash-safety
   discipline), closing the crash-mid-term gap.
3. A leveled compaction strategy, avoiding full-dataset rewrites.
4. A network-queryable admin/metrics endpoint, following Phase 7's
   precedent for additive wire-protocol extension.
5. Session guarantees (read-your-writes) for clients that move between
   replicas.
6. Multi-machine (not just multi-process, single-machine) benchmarking,
   to properly separate FORGE's own behavior from this project's
   single-disk/single-CPU test environment — see BENCHMARKS.md §7.1's
   explicit caveat about what the current node-scaling numbers can't prove.

## 23. Further reading

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — full component design
- [docs/DESIGN.md](docs/DESIGN.md) — roadmap, phase contracts, and two
  recorded corrections where an earlier plan and the actual
  implementation diverged
- [docs/CONSISTENCY.md](docs/CONSISTENCY.md) — exactly what a GET/PUT
  guarantees
- [docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md) — what's assumed to fail,
  and how each layer responds
- [docs/OPERATIONS.md](docs/OPERATIONS.md) — inspecting a running node
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — every measured number, with
  methodology
- [docs/DEMO.md](docs/DEMO.md) — a scripted, reproducible walkthrough
- [docs/INTERVIEW_GUIDE.md](docs/INTERVIEW_GUIDE.md) — implementation-grounded Q&A
- [docs/RESUME.md](docs/RESUME.md) — resume bullets sourced from what's
  actually built and measured
- [PROGRESS.md](PROGRESS.md) — the full session-by-session log: every
  design decision, bug found and fixed, and test count, phase by phase
