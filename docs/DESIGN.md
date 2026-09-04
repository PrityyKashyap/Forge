# FORGE — Roadmap & Design Rationale

This is the implementation roadmap referenced by PROGRESS.md. Phases are meant
to be done **in order**, one at a time, each fully tested and understood before
the next begins.

## 1. Learning prerequisites

You don't need all of this before starting — Phase 1 only really needs the
first bullet. But you'll hit each of these in roughly this order, so it's
worth knowing what's coming.

- **Java fundamentals**: interfaces, generics, exceptions, `try-with-resources`.
- **Java concurrency**: `synchronized`, `ReentrantLock`, `volatile` and memory
  visibility, `ExecutorService`/thread pools, `ConcurrentHashMap` /
  `ConcurrentSkipListMap`. (Needed from Phase 4 on.)
- **File I/O and durability**: `RandomAccessFile` / `FileChannel`,
  buffered vs. unbuffered writes, and critically `fsync` (`FileChannel.force()`)
  — the difference between "written" and "durable." (Phase 2–3.)
  Also, in later systems phases, be aware that OS page cache means a write can
  survive a **process** crash without surviving a **power-loss** crash unless
  fsynced — worth knowing the limit of what "durable" means here.
- **Basic hashing & data structures**: how a hash table works, what a sorted
  map buys you (range scans, deterministic iteration order), what consistent
  hashing solves that `mod N` doesn't. (Phase 3, Phase 7.)
- **Networking basics**: TCP sockets, blocking I/O, what a "wire protocol"
  is. (Phase 5.)
- **Distributed systems concepts**, introduced exactly when first needed:
  CAP theorem and consistency models (Phase 7–9), quorums (Phase 9), failure
  detectors (Phase 8), replication models (Phase 9), split-brain (Phase 9–10),
  fault injection / chaos engineering (Phase 11).

Concepts are explained in depth *in the phase that needs them*, not upfront —
reading a CAP theorem explainer before you have a system to apply it to tends
not to stick.

## 2. Consistency & durability guarantees

FORGE's guarantees are not fixed for the whole project — they get *stronger
and more specific* as phases land, and each stage is explicit about what it
does **not** yet guarantee so early phases are never mistaken for the
finished contract.

### Durability (what a successful `PUT` means)

| Phase | Guarantee |
|---|---|
| 1 (in-memory only) | **None.** Explicitly not durable — a crash loses everything. This phase exists to get GET/PUT/DELETE semantics right before durability is layered in, not to be mistaken for a finished store. |
| 2–3 (WAL + flush) | A `PUT`/`DELETE` ack means the mutation was appended to the WAL **and fsynced** (`FileChannel.force()`) before the ack was returned. Invariant: **no acknowledged write is lost by a single-process crash or restart** — recovery replays the WAL. Torn/partial writes are guarded by per-record checksums (a write that fails its checksum on replay is treated as never having happened, i.e. truncate the log at the first bad record). Does **not** cover disk hardware failure or storage-media corruption below the checksum — single-node durability has a ceiling until replication exists. |
| 9+ (replication) | Durability extends to: **no acknowledged write is lost as long as fewer than the configured ack quorum of replicas fail simultaneously.** Two ack policies are implemented and benchmarked against each other (this is Experiment 5 in §4), not just one assumed: <br>• **Async replication** (default to start): leader acks after its own WAL fsync; followers apply afterward. Lower latency, but a write acked by the leader can be lost if the leader dies before any follower replicates it. <br>• **Sync replication** (opt-in): leader acks only after ≥1 follower has fsynced the same entry to its own WAL. Higher latency, survives leader loss without losing the acked write. |

### Consistency (what a `GET` may return)

| Phase | Guarantee |
|---|---|
| 1–6 (single node) | **Linearizable.** One process, one lock/serialization discipline (established explicitly in Phase 4 — not accidental), so every read reflects the most recently completed write. |
| 7 (partitioned, no replication) | Still linearizable **per key** — each key lives on exactly one node. No cross-key transactions or atomicity are provided (consistent with ARCHITECTURE.md's non-goals), so "consistency" here never means multi-key consistency. |
| 9 (replicated) | • Reads served by the **leader** of a partition: linearizable, *as long as the leader is genuinely still the leader* — this assumption breaks under split-brain (see §7, §8), which is exactly why fencing matters in Phase 9–10. <br>• Reads served by a **follower**: **eventually consistent with bounded staleness.** The bound is not asserted, it's measured — replication lag (§3) is the metric that makes "how stale" a number instead of a hand-wave. |
| 10 (recovery) | A node that just rejoined is **not** guaranteed caught-up the instant it's reachable again. Its state converges to the cluster's once catch-up completes (tracked via replication lag hitting zero). Whether such a node is allowed to serve reads *before* catch-up completes is a deliberate design decision made in Phase 10, not a default — the option that ships is documented there, along with the staleness it trades for availability. |

### The load-bearing invariant underneath all of it

Every consistency/durability claim above ultimately rests on **the WAL append
order at a partition's leader being the single source of truth for "what
happened, in what order."** Replication is "ship this order to followers";
recovery is "replay this order"; failure detection exists to protect the
assumption that there's exactly one leader producing this order at a time.
When a guarantee above turns out to be violated, the WAL-ordering invariant is
the first thing to check.

## 3. Observable metrics

Defined once, referenced from every phase and from ARCHITECTURE.md §3.11 — no
component invents its own ad hoc metric.

| Metric | Definition | Measured by | First measurable at |
|---|---|---|---|
| **Throughput** | Completed ops/sec, reported separately for GET/PUT/DELETE | `forge-bench` load generator | Phase 1 (in-process), Phase 6 (over the network) |
| **Latency (p50/p95/p99)** | Wall-clock time from client issuing a request to receiving the response, percentiles over a fixed run window | `forge-bench` | Phase 1 (in-process), Phase 6 (over the network) |
| **Storage overhead** | On-disk bytes used ÷ logical bytes stored (i.e. write/space amplification from the WAL, flushed files, and — later — pre-compaction duplication) | `forge-storage` reports raw file sizes; `forge-bench` computes the ratio | Phase 3 (first on-disk files exist) |
| **Recovery time** | Wall-clock time from process start to "fully caught up" (WAL replay complete for a restarted node; replication catch-up complete for a rejoining node) | `forge-server` / `forge-cluster` timestamp the start and completion of recovery | Phase 2 (restart replay), extended at Phase 10 (rejoin catch-up) |
| **Replication lag** | Time delta between a write being committed at the leader's WAL and the same entry being applied at a given follower | `forge-cluster` (each follower reports the leader-assigned sequence number/timestamp it has applied; leader/bench computes the delta) | Phase 9 |
| **Failure rate** | (a) False-positive rate of the failure detector (alive nodes marked dead) and (b) true-positive detection time (real crash → detected) | `forge-cluster` failure detector, exercised under Phase 11's injected faults | Phase 8 (basic), fully exercised at Phase 11 |

Every metric above must be **actually emitted by running code**, not
estimated — a metric that exists only in this table and not in a log line or
a `forge-bench` report doesn't count as "defined" for a given phase.

## 4. Experiments (what will actually produce BENCHMARKS.md data)

Each experiment names the metric(s) it produces, the variable being swept,
and the phase that first makes it runnable. This is the concrete plan behind
BENCHMARKS.md — no benchmark numbers exist until an experiment below is
actually run.

| # | Experiment | Metric(s) | Variable swept | Runnable from |
|---|---|---|---|---|
| E1 | Single-threaded in-memory baseline | Throughput, latency | — (baseline) | Phase 1 |
| E2 | Durability cost | Throughput, latency | WAL off vs. on vs. fsync-every-write vs. batched fsync | Phase 2–3 |
| E3 | Concurrency scaling | Throughput, latency | Client thread count (1, 2, 4, 8, 16, ...) | Phase 4 |
| E4 | Network overhead | Throughput, latency | In-process call vs. loopback TCP vs. LAN | Phase 5–6 |
| E5 | Read/write mix & key skew | Throughput, latency, replication lag (once available) | Read:write ratio; uniform vs. hot-key (Zipfian) key distribution | Phase 6, extended Phase 9 |
| E6 | Partition/rebalance cost | Recovery time, throughput dip during rebalance | Number of nodes; key movement on node add/remove | Phase 7 |
| E7 | Failure-detector tuning | Failure rate (false-positive rate, detection time) | Heartbeat interval, timeout/missed-heartbeat threshold | Phase 8, extended Phase 11 |
| E8 | Replication ack policy | Throughput, latency, durability (measured as: writes lost after a killed leader, expected to be zero for sync, possibly nonzero for async) | Async vs. sync replication | Phase 9 |
| E9 | Recovery time vs. data size | Recovery time | WAL size / data-file size at time of crash | Phase 10 |
| E10 | Chaos suite | Failure rate, replication lag, recovery time, correctness (post-hoc invariant check) | Injected fault type: kill -9 leader, kill -9 follower, network partition (majority/minority side), slow/delayed node, repeated flapping | Phase 11 |
| E11 | Storage overhead over time | Storage overhead | Write volume before vs. after compaction (Phase 12★ only) | Phase 3 (pre-compaction), Phase 12★ (post-compaction) |

## 5. Implementation phases

Each phase, when we get to it, follows the same loop: explain what/why/concept
→ design → implement → test → run tests → review the code together → interview
questions. Below is the roadmap only — no code yet.

| # | Phase | Delivers |
|---|-------|----------|
| 0 | Project setup | Maven multi-module skeleton, git repo, module boundaries, build runs |
| 1 | In-memory KV core | Single-threaded `Map`-backed GET/PUT/DELETE + a REPL to poke at it |
| 2 | Write-ahead log | Every mutation durably logged before ack; crash-and-replay recovery |
| 3 | Persistent storage engine | MemTable + flush-to-disk data files; WAL truncation after flush |
| 4 | Concurrency | Thread-safe engine; multiple in-process client threads; correctness under race |
| 5 | Network layer | TCP server + wire protocol + real client library/CLI talking over a socket |
| 6 | Concurrent server | Many simultaneous socket clients; connection-handling model; first real benchmark (E1–E4) |
| 7 | Partitioning | Consistent hashing ring; multiple node processes; client-side routing by key |
| 8 | Membership & failure detection | Heartbeats between nodes; suspect/dead state machine (E7 begins) |
| 9 | Replication | Leader-follower per partition; write propagation; sync vs. async ack policy (E5, E8) |
| 10 | Recovery | Crashed node rejoin (WAL replay + catch-up); new-node bootstrap (full copy) (E9) |
| 11 | **Fault injection / chaos testing** | Fault-injection harness (process kill, network partition, slowdown, flapping) exercising Phases 8–10 together; correctness + failure-rate + lag measured under real injected faults (E10) |
| 12 | Benchmarking suite | Full experiment sweep (E1–E11 as applicable) run and recorded in BENCHMARKS.md with methodology |
| 13★ | Compaction & read optimization | Merge on-disk files, bloom filters, bound read amplification (E11 completed) |
| 14★ | Automated failover / simplified consensus | Leader election on leader failure, without full Raft — **gated, see §9** |

★ = stretch phases, attempted only once 0–12 are solid. Nothing here is
promised as "in scope" beyond what's actually built and tested — this table
is a plan, not a changelog.

Phase 11 sits **after** replication (9) and failure detection (8) precisely
because it needs both to exist to have something worth breaking — chaos
testing a system with no replicas or no failure detector is just "the process
crashed," not a distributed-systems test.

## 6. Why leader-follower, not Dynamo-style quorums, for replication

Two well-known replication models exist for a KV store like this:

1. **Leader-follower per partition** (like classic MySQL replication, or Kafka
   partition leadership): one leader accepts writes, followers replicate from
   it. Reads from the leader are strongly consistent; reads from a follower
   may lag. Failover requires picking a new leader when the old one dies.
2. **Leaderless / quorum-based** (Dynamo/Cassandra-style): any replica can
   accept a write; a write succeeds once `W` replicas ack it, a read queries
   `R` replicas and reconciles; `W + R > N` guarantees overlap. Needs a way to
   resolve conflicting versions of the same key (vector clocks, last-write-wins,
   etc.) because concurrent writes to different replicas are possible by design.

**FORGE starts with leader-follower.** It's conceptually simpler (no
concurrent-write conflict resolution needed within a partition — the leader
serializes all writes to it), and it maps directly onto the WAL you already
built in Phase 2: "replication" becomes "ship my WAL entries to my followers
in order," which is a natural extension rather than a new concept. Quorum-based
leaderless replication is a legitimate and interesting alternative — it's a
good candidate for a stretch phase or a `v2` branch once leader-follower is
working and understood, precisely because contrasting the two once you've
built one is where the real insight is.

## 7. Testing strategy

- **Unit tests** (JUnit 5), per module, from Phase 1 onward: engine
  correctness (GET after PUT, DELETE removes, overwrite semantics), WAL
  format round-trips, partition-map math (consistent hashing distribution).
- **Concurrency / stress tests**: many threads hammering the same key and
  different keys concurrently, checked against a known-correct sequential
  model (e.g. compare final state to a single-threaded reference run with the
  same operation log).
- **Crash-recovery tests**: kill the process (not just close it cleanly) at
  specific points — mid-WAL-append, mid-flush, mid-compaction — and assert
  that recovery produces a consistent state. This is where most "real"
  storage-engine bugs live, so it's not optional even though it's fiddly to
  write.
- **Integration tests** (in `tests/`): spin up 3+ node processes, exercise
  partitioning/replication/failure-detection end to end (kill -9 a node,
  assert the cluster detects it and reroutes; bring it back, assert it
  catches up).
- **Fault injection / chaos testing (Phase 11, formalized)**: a dedicated
  harness that injects, on a running multi-node cluster: process kills
  (leader and follower, separately), network partitions (both the
  majority-side and minority-side view), artificial slowness/delay on a
  node, and repeated flapping (a node going up/down rapidly). Each injected
  scenario has an **expected outcome asserted in code** (e.g. "after killing
  the leader, a new leader is elected within N seconds and no acknowledged
  write is lost" for sync replication) — this is what turns chaos testing
  into a test suite rather than manual poking.

No phase is "done" until its tests exist and pass — this is enforced, not
aspirational. Phase 11 in particular is not optional polish: it's the phase
that actually validates the consistency/durability claims made in §2, rather
than taking them on faith.

## 8. Potential failure modes to design against

- Disk full during a WAL append or flush.
- Process crash between WAL append and MemTable apply (must replay correctly).
- Crash mid-flush or mid-compaction (must not lose or duplicate data — needs
  either atomic rename of completed files or a way to detect/discard partial
  ones).
- Data corruption from a torn write (partial sector/page written) — argues
  for per-record checksums in the WAL/data file format (see §2).
- Network partition splitting the cluster into two halves that both think
  they're primary for a partition (**split-brain**) — directly relevant once
  Phase 9's leader-follower replication and Phase 14★'s failover exist, and
  is one of the scenarios Phase 11 must actually inject and check.
- Replica lag causing stale reads from a follower — a consistency-model
  choice (§2), not just a bug, and one the client/API should be honest about.
- Clock skew between nodes affecting heartbeat-based failure detection.
- False-positive failure detection (a slow-but-alive node marked dead) causing
  unnecessary failover/thrashing — measured directly as part of the
  "failure rate" metric (§3).
- Hot key / skewed load overwhelming one partition while others sit idle.
- Unbounded MemTable growth if flushing can't keep up with write rate
  (needs backpressure or a bounded MemTable size that forces a flush).
- Reconnect storms (thundering herd) when a node comes back after being
  marked dead and every client tries it at once.

## 9. Implementation constraints

These are binding, not aspirational — they're the direct implementation of
"do not hide complexity behind unnecessary libraries" and "do not use a
third-party distributed database as the actual database engine."

**Implemented by us, no substitutes:**
- Write-ahead log (format, append, fsync, replay)
- Storage engine (MemTable, on-disk files, later compaction)
- Concurrency control over the engine
- Wire protocol and network server (plain `java.net`/`java.nio` sockets —
  no Netty, no RPC framework)
- Consistent hashing / partition map
- Cluster membership and failure detection (no gossip library, no SWIM
  implementation pulled from a dependency)
- Replication (log shipping, ack policy, catch-up)
- Recovery (WAL replay, bootstrap)

**Allowed as given infrastructure** (these aren't the point of the project):
JDK standard library, JUnit 5 for tests, Maven for the build. Nothing that
performs a core mechanism above on FORGE's behalf.

**Explicitly not allowed** as a substitute for a core mechanism: RocksDB,
LevelDB, etcd, ZooKeeper, Redis, Raft/Paxos libraries (e.g. Atomix, JGroups'
consensus features), Netty, or any embedded/distributed database used as the
actual storage or replication engine.

**Raft/Paxos gate (Phase 14★):** automated consensus-based failover is
**not** attempted until:
1. Phases 0–12 are complete, tested, and passing, **and**
2. Phase 11's chaos suite has been run against the simpler, hand-built
   failover mechanism (heartbeat-triggered, manual or deterministic
   lowest-ID-alive election) **and its actual limitations have been observed
   and written down** — e.g. a specific split-brain scenario it handles
   badly, or a specific downtime window it can't shrink further.

In other words: Raft/Paxos is a response to a demonstrated, measured
shortcoming of the simpler mechanism, not a default upgrade — consistent
with "do not claim a feature exists until it has actually been implemented
and tested," applied here to *why* a feature would be added at all.

## 10. Core distributed-systems concepts, mapped to where they show up

- **CAP theorem / consistency models** — Phase 7 (partitioning forces the
  question), sharpens at Phase 9 (replication makes it concrete: what does a
  read return during a partition or replica lag?), validated at Phase 11.
- **Quorums (N/W/R)** — relevant even in the leader-follower model when
  deciding "how many followers must ack before a write is considered
  committed" (§2's sync vs. async ack policy), and central if the leaderless
  stretch phase is attempted.
- **Partitioning strategies** — hash vs. range partitioning, consistent
  hashing with virtual nodes, rebalancing cost on membership change (Phase 7).
- **Failure detectors** — fixed-timeout heartbeats to start; phi-accrual or
  gossip-based (SWIM-style) as a documented "here's what production systems
  do differently and why" comparison, not a requirement (Phase 8, tuned via
  E7, stress-tested via E10).
- **Write-ahead logging & durability** — fsync semantics, log replay, the gap
  between "acked" and "on disk" (Phase 2–3, formalized in §2).
- **LSM-tree vs. B-tree storage engines** — FORGE's MemTable+WAL+flush design
  *is* a simplified LSM tree; understanding why LSM trees trade read
  amplification for write throughput is the payoff of Phase 3 (Phase 13★ for
  the deeper version with compaction and bloom filters).
- **Idempotency & retries** — what happens if a client retries a `PUT` that
  actually succeeded but the ack was lost (Phase 5–6, sharpens in Phase 9).
- **Split-brain & fencing** — Phase 9–10, and centrally in Phase 14★ if
  automated failover is attempted; explicitly one of Phase 11's injected
  scenarios.
- **Fault injection / chaos engineering** — Phase 11 in its own right: the
  discipline of turning "what if the network partitions?" from a design
  conversation into an automated, assertable test.
- **Vector clocks / logical clocks** — only needed if the leaderless
  replication stretch path is taken; otherwise the leader's WAL order serves
  as the "logical clock" for free (§2's load-bearing invariant), which is
  itself worth noticing.

## 11. Per-feature contracts

Every feature below is specified the same way, before it's built: what can
go wrong (failure assumptions), what must always hold (correctness
invariant), what a caller can rely on (consistency guarantee), and what
happens after things go wrong (recovery behavior). These are commitments to
verify in that feature's tests and, for the distributed features, in
Phase 11's chaos suite — not just prose.

### Write-ahead log (Phase 2)
- **Failure assumptions**: the process can crash at any instant, including
  mid-append; a disk write can be torn (partially written); the OS page
  cache can lose any data that wasn't fsynced before a crash.
- **Correctness invariant**: the log is append-only; every entry carries a
  monotonically increasing sequence number; an entry counts as "committed"
  only once `force()` (fsync) returns successfully for it.
- **Consistency guarantee**: an acknowledged write is durable across a
  single-process crash (§2).
- **Recovery behavior**: on startup, scan the log from the start, verify each
  entry's checksum, replay all valid entries in sequence order into the
  MemTable, and truncate the log at the first invalid or incomplete record
  (treat it, and everything after it, as never having happened).

### Storage engine — MemTable + on-disk files (Phase 3)
- **Failure assumptions**: a crash can happen mid-flush; two on-disk files
  must never both be treated as authoritative for the same key without a
  defined tiebreak.
- **Correctness invariant**: a flush writes to a temporary file, fsyncs it,
  then atomically renames it into place — an on-disk data file is either
  fully complete or doesn't exist yet, never half-written and visible. The
  MemTable is cleared only after the renamed file is confirmed durable.
- **Consistency guarantee**: at every moment, (on-disk files) ∪ (current
  MemTable, itself recoverable from the WAL) equals the full, correct data
  set — no key is ever missing or duplicated across the two.
- **Recovery behavior**: after WAL replay rebuilds the MemTable, reconcile
  against existing on-disk files by key, newest write wins, using the WAL
  sequence number (not wall-clock time) to decide "newest."

### Concurrency control (Phase 4)
- **Failure assumptions**: threads can be interrupted, but the process is
  either fully running or fully down — no partial-thread-crash corruption
  inside one live process.
- **Correctness invariant**: writes to the same key are totally ordered; a
  concurrent read never observes a partially applied write.
- **Consistency guarantee**: linearizable within a single node (§2).
- **Recovery behavior**: not this layer's job directly — a restarted process
  starts with no held locks and rebuilds its state entirely from WAL/storage
  recovery above.

### Partitioning (Phase 7)
- **Failure assumptions**: the partition map can be transiently stale on a
  given node or client (e.g. mid-rebalance); a request can arrive at a node
  that no longer — or doesn't yet — own the key.
- **Correctness invariant**: in a converged (non-rebalancing) state, each key
  maps to exactly one partition; ownership never overlaps.
- **Consistency guarantee**: no strong guarantee is made *during* an active
  rebalance — a node that receives a request for a key it doesn't
  authoritatively own must reject/redirect it rather than silently serving a
  possibly-wrong answer (fail closed, not open).
- **Recovery behavior**: on a partition-map change, the losing node keeps
  serving its range until the gaining node has fully bootstrapped it and
  confirmed ownership; only then does routing switch over.

### Membership & failure detection (Phase 8)
- **Failure assumptions**: heartbeats can be delayed or dropped independent
  of whether the peer is actually alive (an unreliable network is not the
  same fact as a dead node); clocks across nodes are not perfectly
  synchronized.
- **Correctness invariant**: a node is marked dead only after missing a
  configured number of consecutive heartbeats within a configured timeout —
  never on a single missed beat.
- **Consistency guarantee**: the cluster's view of "who's alive" is
  eventually consistent, not instantaneous — different nodes may transiently
  disagree.
- **Recovery behavior**: a node previously marked dead is re-admitted to the
  membership view automatically once its heartbeats resume, with no cluster
  restart required.

### Replication (Phase 9)
- **Failure assumptions**: the leader can crash at any point, including
  mid-replication, leaving followers at different, inconsistent-with-each-
  other points in the log relative to the leader and to one another.
- **Correctness invariant**: followers apply WAL entries in exactly the
  order the leader committed them — no reordering, and no permanent gaps.
- **Consistency guarantee**: as in §2 — linearizable leader reads (while it
  is genuinely the leader), eventually consistent follower reads with a
  measured replication-lag bound; durability per the active ack policy
  (async or sync).
- **Recovery behavior**: a follower that falls behind requests the missing
  suffix of the log from the leader, or performs a full resync if it's fallen
  behind further than the leader retains; on leader loss, whichever failover
  mechanism is active picks the replacement (heuristic/manual in Phase 9,
  formal election only if Phase 14★ is reached).

### Recovery — rejoin & bootstrap (Phase 10)
- **Failure assumptions**: a node can be gone for an arbitrary amount of
  time and return with an intact WAL, a corrupted one, or no data at all
  (e.g. a replaced disk).
- **Correctness invariant**: rejoining never regresses committed state — a
  returning node must never overwrite a key with a value older than what's
  already committed cluster-wide for that key.
- **Consistency guarantee**: eventual — a rejoining node is not consistent
  the instant it's reachable, only once catch-up completes (tracked via
  replication lag reaching zero).
- **Recovery behavior**: same-identity restart replays its own WAL, then
  catches up from its leader; a genuinely new/replacement node instead
  performs a full bootstrap copy from a healthy replica before it's allowed
  to serve any traffic.
