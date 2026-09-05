# FORGE — Consistency Guarantees

What a `GET`/`PUT`/`DELETE` actually guarantees, stated precisely and
matched against what the code actually does — not what an earlier plan
said it would do. See DESIGN.md §2 for the full phase-by-phase history,
including corrections recorded there where the original roadmap's plan
and the actual implementation diverged. This document is the current-state
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
  itself, *for as long as it is genuinely, currently the authoritative
  leader of that partition*. As of Phase 15, "currently the authoritative
  leader" is a real, checked condition on every write — see §5.
- **A follower**: eventually consistent, with a *measured*, not asserted,
  staleness bound — replication lag (BENCHMARKS.md's E15/§7.4) is what
  makes "how stale" a number. A follower serves whatever it currently has
  at any moment, including mid-catch-up — there is no gate that refuses or
  labels reads as stale while behind. **This is unchanged by Phase 15,
  deliberately**: reads are never fenced, on any node, including a
  demoted former leader — see §5.
- **Durability**: async-only. A leader acknowledges a write once its own
  WAL fsync completes, before any follower has it. **A write acknowledged
  by the leader can be lost if the leader crashes before replicating it to
  any follower** — this is Phase 11 Scenario A's exact, tested subject,
  not a hypothetical, and Phase 15 does not change it (see §5 — Raft's own
  commit mechanism is entirely separate from KV write durability).

## 4. What "eventually consistent" does *not* mean here

FORGE does not implement session guarantees (read-your-writes,
monotonic reads) across a client's requests to different replicas of the
same partition. A client that writes to a leader and then happens to read
from a lagging follower can observe a value older than what it just wrote.
Nothing tracks a client's own causal history to prevent this.

## 5. Consensus and data-plane fencing (Phase 14 + 15)

Phase 14 built real Raft (terms, majority-vote election, AppendEntries,
commit) as a **standalone control-plane mechanism**. Phase 15 wired its
result into write acceptance:

**When is a write accepted?** Only when the receiving node's `ForgeServer`
both owns the key's partition (Phase 7's static ownership predicate — a
separate, unrelated check) *and* `RaftCluster.canServeAuthoritatively()`
returns true at the moment the request is processed. That method is
`isConfirmedLeader() && hasRecentQuorumContact(leaseDuration)` — not
`isConfirmedLeader()` alone. The distinction matters precisely because a
leader that wins an election and is then silently partitioned away stays
`isConfirmedLeader()` forever (nothing ever tells it otherwise); the
lease-based `hasRecentQuorumContact` check is what makes such a node
notice, purely from the passage of time on its own clock, that it can no
longer prove it has a majority — see `RaftNode`'s Javadoc and
`docs/FAILURE_MODEL.md` for the exact mechanism.

**Is this fencing instantaneous?** No — stated plainly. A partitioned
leader can still incorrectly accept a write for up to approximately
`leaseDuration` (default: the cluster's configured minimum election
timeout) after it actually loses contact, before its own lease expires.
This is a real, bounded window, not zero. `StaleLeaderFencingIntegrationTest`
explicitly waits out this window before asserting fencing has taken
effect.

**Is the split-brain gap closed?** For the specific scenario tested —
a leader genuinely, bidirectionally network-partitioned from the rest of
the cluster, alive but never receiving a single message about a new
term — yes, within the disclosed lease window: proven end-to-end in
`StaleLeaderFencingIntegrationTest` and chaos Scenario I. What is **not**
claimed: perfect, zero-latency exclusion (impossible without either the
lease window or synchronous per-write quorum confirmation, and this
project deliberately kept replication asynchronous — see DESIGN.md §2);
protection against a node lying about its own state (out of scope — see
ARCHITECTURE.md §5's Byzantine-fault-tolerance non-goal); or multi-partition
deployments (this phase's abstractions model one Raft group per replica
set — see PROGRESS.md's Phase 15 known limitations for what a
multi-partition extension would need).

**What does Raft's "commit" mean here, versus a KV write being durable?**
Two genuinely different things, not to be confused. Raft's own log
(nothing but one no-op marker per election) is committed in the textbook
sense — a majority has durably (for this implementation's disclosed,
in-memory-only definition of "durably," see `RaftNode`'s Javadoc)
acknowledged it. **KV writes are not tracked by Raft's commit mechanism at
all** — they still move exclusively through Phase 9's async, best-effort
replication, exactly as before. A write can be "accepted" by an
authoritative leader (Raft says so) while still being vulnerable to the
same async-replication data-loss window §3 already discloses. Conflating
these two would be exactly the kind of fabricated guarantee this
project's honesty requirements forbid.

**Are reads fenced too?** No, deliberately. `ConnectionHandler` only
checks `WriteAuthority` for `PUT`/`DELETE`; `GET` is answered from local
data on any node, leader or not, exactly as before Phase 15. A demoted
former leader can still serve a (possibly stale, possibly locally
divergent — see `StaleReplicaRecovery`'s Javadoc) `GET` indefinitely.
Fencing only ever gates the *write* path.

## 6. Summary table

| Scope | Guarantee | Real gap, if any |
|---|---|---|
| Single node | Linearizable | None known |
| Partitioned, single copy | Linearizable per key | None known |
| Leader of a replicated partition | Linearizable while currently, recently-confirmed authoritative | Bounded staleness window (≈one lease duration) before a partitioned leader self-fences; single-partition scope only |
| Follower of a replicated partition | Eventually consistent, measured staleness | No catch-up gate; a client can read arbitrarily stale data with no signal — including from a demoted former leader |
| Across a client's own requests | None (no session guarantees) | By design; not attempted |
| Raft-elected leadership | Correct, real majority-quorum election, now gating writes | No persistent Raft state (Phase 14); bounded (not instantaneous) fencing latency |
