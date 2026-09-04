# FORGE — Consistency Guarantees

What a `GET`/`PUT`/`DELETE` actually guarantees, stated precisely and
matched against what the code actually does — not what an earlier plan
said it would do. See DESIGN.md §2 for the full phase-by-phase history,
including two corrections recorded there where the original roadmap's plan
and the actual implementation diverged (sync replication was never built;
no fencing/epoch mechanism exists). This document is the current-state
summary; DESIGN.md is the annotated history of how it got here.

## 1. Single node (no partitioning or replication)

**Linearizable.** One `ConcurrentLsmKeyValueStore` per node, one
`stateLock` (`ReentrantReadWriteLock`) serializing every mutation against
every read of the in-memory tiers, with SSTables immutable once written.
Every `GET` reflects the most recently *completed* `PUT`/`DELETE` on that
node. No multi-key transactions or atomicity — each key is independent.

## 2. Partitioned, no replication (Phase 7 alone)

Still linearizable **per key** — consistent hashing assigns each key to
exactly one node, so a single-node guarantee applies key-by-key. There is
no cross-key consistency and never has been (ARCHITECTURE.md §5).

## 3. Replicated (Phase 9 added)

- **A partition's leader**: linearizable for reads/writes it serves
  itself, *for as long as it is genuinely the sole leader of that
  partition*. **This assumption has a real, open gap**: nothing in this
  codebase currently prevents two nodes from both believing themselves
  leader of the same partition after a network partition or split-brain
  event (`ForgeServer`'s ownership is a static predicate, never updated by
  Raft or anything else at runtime). See §5 below and
  `docs/FAILURE_MODEL.md`.
- **A follower**: eventually consistent, with a *measured*, not asserted,
  staleness bound — replication lag (BENCHMARKS.md's E15/§7.4) is what
  makes "how stale" a number. A follower serves whatever it currently has
  at any moment, including mid-catch-up — there is no gate that refuses or
  labels reads as stale while behind (a real gap: see §4).
- **Durability**: async-only. A leader acknowledges a write once its own
  WAL fsync completes, before any follower has it. **A write acknowledged
  by the leader can be lost if the leader crashes before replicating it to
  any follower** — this is Phase 11 Scenario A's exact, tested subject,
  not a hypothetical.

## 4. What "eventually consistent" does *not* mean here

FORGE does not implement session guarantees (read-your-writes,
monotonic reads) across a client's requests to different replicas of the
same partition. A client that writes to a leader and then happens to read
from a lagging follower can observe a value older than what it just wrote.
Nothing tracks a client's own causal history to prevent this.

## 5. Consensus (Phase 14) and what it does and doesn't change

Raft (terms, majority-vote election, AppendEntries commit) exists and is
real, tested, and correct as a **standalone control-plane mechanism** —
proven to elect exactly one leader per Raft group and reject a stale
leader once a higher term is observed (`RaftNodeTest` scenario 10,
`RaftClusterIntegrationTest`). **It is not yet wired into the data plane**
described in §3: no code today asks "is Raft confirming me as leader?"
before `ForgeServer` accepts a write or `ReplicationServer` starts
streaming. `RaftCluster.isConfirmedLeader()` is the exact signal such
wiring would consume — see PROGRESS.md's Phase 14 known limitations for
why that integration wasn't rushed into already-tested Phase 9 code.

**Until that wiring exists, the split-brain gap named in §3 is real and
open** — Raft's existence in this codebase does not, by itself, close it.

## 6. Summary table

| Scope | Guarantee | Real gap, if any |
|---|---|---|
| Single node | Linearizable | None known |
| Partitioned, single copy | Linearizable per key | None known |
| Leader of a replicated partition | Linearizable while genuinely sole leader | No fencing — split-brain can produce two simultaneous "leaders" |
| Follower of a replicated partition | Eventually consistent, measured staleness | No catch-up gate; a client can read arbitrarily stale data with no signal |
| Across a client's own requests | None (no session guarantees) | By design; not attempted |
| Raft-elected leadership | Correct, real majority-quorum election | Not connected to the data plane yet |
