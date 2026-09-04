# FORGE — Failure Model

What FORGE assumes can go wrong, what it assumes can't, and exactly which
component is responsible for each failure. Every claim here is backed by a
specific class and, where relevant, a specific test — this document
summarizes and cross-references PROGRESS.md's per-phase detail rather than
replacing it.

## 1. The fundamental assumption: crash-stop, not Byzantine

A node either works correctly or stops (crashes, hangs, is killed, is
partitioned away) — it never sends corrupted or maliciously incorrect data
on purpose. FORGE does not defend against a compromised or buggy node that
actively lies to the rest of the cluster. This is a deliberate, disclosed
scope boundary (ARCHITECTURE.md §5), not an oversight.

**Data corruption is still handled** — that's a different concern from
Byzantine behavior. A bit flip on disk, a torn write from a crash mid-write,
or a truncated file are all detected via checksums (CRC32, on every WAL
record and every SSTable record/header) and treated as fatal for that
region of data (WAL: skip the corrupt tail, since it was never
acknowledged; SSTable: fail loudly, since it represents already-acknowledged
data — see `WriteAheadLog`'s and `SSTableReader`'s class Javadocs for the
precise reasoning behind treating the two differently).

## 2. Per-layer failure assumptions

| Layer | Can fail how | Detected/handled by | Where |
|---|---|---|---|
| Disk write | Process crash mid-write, leaving a partial file | Checksums + temp-file/`force()`/atomic-rename discipline | WAL (Phase 2), SSTable (Phase 3) |
| Disk write | Bit-level corruption of already-written bytes | CRC32 checksums, fatal on mismatch | WAL/SSTable readers |
| Network (client↔server, node↔node) | Message delay, drop, duplication, or a severed connection | TCP's own guarantees where used (ordering, no corruption) plus application-level idempotency (replication sequence numbers) and timeouts | Phase 5 (client/server), Phase 9 (replication), Phase 11 (chaos proxy proves these) |
| Network (heartbeats) | Datagram loss, indistinguishable from a dead peer | Fixed-timeout failure detector; a slow-but-alive node **will** be misclassified dead — a disclosed, unavoidable tradeoff, not a bug | `FailureDetector` (Phase 8) |
| A node process | Crash, restart, or permanent departure | WAL replay on restart (Phase 2-3); snapshot bootstrap for a node too far behind to catch up from WAL alone (Phase 10); Raft election for control-plane leadership loss (Phase 14) | See PROGRESS.md's respective phase sections |
| The network as a whole | Partition (a subset of nodes can't reach another subset) | Chaos-tested directly (Phase 11's six named scenarios); Raft's majority-quorum rule ensures at most one side of a partition can elect a leader | `ChaosScenarioTest`, `RaftNodeTest` scenario 10 |
| A leader specifically | Crash before a write is replicated anywhere | **Data loss of that specific unreplicated write** — disclosed plainly, see §3 below | Phase 9's design notes, Phase 11 scenario A |
| Clocks | Not assumed synchronized across nodes | Every timeout compares a node's own clock to its own prior observations, never to another node's timestamp | `FailureDetector`, `RaftNode` |

## 3. What is explicitly *not* protected against — stated plainly, not softened

- **A write acknowledged by a leader that crashes before replicating it
  anywhere is lost.** FORGE's replication (Phase 9) is asynchronous — a
  `PUT`'s local WAL fsync completing is what makes it durable *on that
  node*; forwarding to followers happens after, never blocking the local
  write. There is no synchronous-replication mode to opt into (E13 in
  BENCHMARKS.md measures the real cost this asynchronicity is trading
  against). This is Phase 11 Scenario A's exact, tested subject.
- **Raft consensus (Phase 14) has no persistent state.** A node that
  crashes and restarts mid-term rejoins as a brand-new participant at term
  0 — safe for liveness, but a real (if narrow) gap relative to the Raft
  paper's crash-safety guarantee for `votedFor`. See `RaftNode`'s class
  Javadoc for the precise window this affects.
- **No Byzantine fault tolerance** (§1).
- **No authentication, authorization, or transport encryption.** Every TCP
  connection is trusted. Not a distributed-systems concept this project
  explores; see ARCHITECTURE.md §5.
- **A slow-but-alive node will be misclassified as dead** once it misses
  heartbeats for `deadTimeout` — the fundamental, named tradeoff of any
  fixed-timeout failure detector (`FailureDetector`'s own class Javadoc).
- **Raft is not yet wired into live data-plane failover.** Electing a new
  leader (control plane) does not yet automatically redirect
  `ReplicationServer`/`ReplicationFollower` (data plane) — see
  PROGRESS.md's Phase 14 known limitations.

## 4. What *is* proven, and how

Every claim below has a specific, real (not simulated-in-the-same-process)
test behind it:

- **A follower that already has data survives its leader's crash and
  remains fully readable** — Phase 11 Scenario A.
- **A follower that disappears and rejoins reaches full parity again,
  with no lost or duplicated record** — Phase 11 Scenario B;
  idempotency guaranteed by `ReplicationOutcome`/WAL sequence numbers.
- **A network partition between leader and follower never blocks the
  leader's local writes, and the follower's partial data stays
  consistent** — Phase 11 Scenario C.
- **Significant message delay (200ms/hop) doesn't corrupt anything, only
  delays convergence** — Phase 11 Scenario D.
- **A crash during bootstrap snapshot transfer leaves no partially-loaded
  destination — all-or-nothing** — Phase 11 Scenario E; Phase 10's
  `SnapshotTransferIntegrationTest`.
- **Disagreeing membership views between two nodes never corrupt either
  detector's own state, and converge once heartbeats resume** — Phase 11
  Scenario F.
- **A real 3-node Raft cluster elects exactly one leader that all nodes
  agree on, and failing that leader over produces a new leader at a
  strictly higher term** — `RaftClusterIntegrationTest`.
- **An old-term log entry is never committed by direct majority count
  alone (Raft's Figure 8 safety property)** — `RaftNodeTest` scenario 7.
