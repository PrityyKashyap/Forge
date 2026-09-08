# FORGE — Interview Guide

Every answer below is grounded in what's actually implemented and tested
in this repository — file names and behaviors you can go verify, not
generic textbook answers. Cross-references point at PROGRESS.md's
phase-by-phase detail and BENCHMARKS.md's measured numbers.

## Storage engine

**Why a WAL, and why fsync on every write?**
Without a WAL, a crash between "value is in the MemTable" and "MemTable is
flushed to disk" loses acknowledged writes. `WriteAheadLog.appendPut`/
`appendDelete` call `FileChannel.force(true)` synchronously before
returning — this is the single load-bearing durability guarantee
everything else in the project rests on (DESIGN.md §2's "load-bearing
invariant"). The cost is real and measured: PUT is capped at ~250 ops/sec
regardless of client concurrency (BENCHMARKS.md §3) — that ceiling *is*
the fsync cost, isolated and quantified.

**Why an LSM tree instead of a B-tree?**
Writes become sequential appends (WAL) plus periodic sorted-batch flushes
(SSTables), both cheap on real disks; a B-tree's in-place random-access
updates are a different, more read-optimized tradeoff. This project
deliberately explores the LSM side, per DESIGN.md §9's constraint that a
core mechanism be actually built and understood, not assumed.

**Why does an SSTable use CRC32 checksums, and why is corruption there
treated differently from WAL corruption?**
Both use CRC32 per-record. But a WAL's tail record failing its checksum
means "the process crashed mid-write and this write was never
acknowledged" — safe to truncate and move on. An SSTable's checksum
failing means data that was already flushed and acknowledged is now
corrupt — `SSTableReader` treats this as fatal, never silently skipped,
because silently discarding it would be a silent loss of a promise
already made to a caller. See `SSTableReader`'s class Javadoc.

**Walk through exactly what happens on crash recovery.**
On construction, `ConcurrentLsmKeyValueStore` discovers existing SSTables,
computes a watermark (the highest sequence number any SSTable already
reflects), opens the WAL, calls `ensureNextSequenceNumberAtLeast(watermark+1)`,
then replays only WAL records *newer* than the watermark into a fresh
MemTable. Records already reflected in an SSTable are never replayed
twice. All of this runs in the constructor, before the object is visible
to any other thread — no locking needed during recovery.

**What are sequence numbers for, precisely?**
A total order over every mutation a node has ever durably applied — used
three ways: (1) SSTable watermarks, to know what's already flushed and
skip re-replaying it; (2) replication catch-up, where a follower asks "send
me everything after sequence N"; (3) idempotency —
`ReplicationOutcome.ALREADY_APPLIED` uses sequence-number comparison to
detect and safely no-op a re-delivered record.

**Why does `StoredEntry.Value` defensively copy its byte array on
construction and on every read?**
Without it, a caller mutating a `byte[]` they handed to `put()` (or one
they got back from `get()`) would silently corrupt the store's internal
state — a classic mutable-aliasing bug. The copy cost is paid once per
call; the alternative is a bug that only manifests when some caller
happens to mutate an array they shouldn't have kept a reference to.

## Concurrency

**How does `ConcurrentLsmKeyValueStore` avoid blocking readers on slow
disk I/O?**
Exactly one `ReentrantReadWriteLock` (`stateLock`) guards three in-memory
pointers (`active`, `frozen`, `sstables`) — never a disk operation
directly. A flush's actual write/fsync/rename happens entirely outside
any lock hold; only the brief "freeze the MemTable" and "register the new
SSTable reader" steps take the lock, each for microseconds. This is why a
`get()` scanning a large SSTable never blocks a concurrent `put()`.

**What's the actual concurrency hazard Phase 13's compaction introduced,
and how was it solved?**
Every phase through 12 could treat an SSTable file as permanent once
written. Compaction needs to delete a merged-away file *while the store
stays live*, including while a `get()` that already released the lock is
mid-scan of that exact file — closing/deleting it out from under that scan
would throw `ClosedChannelException`. Solved with reference counting on
`SSTableReader` (`acquire()`/`release()`/`retire()`): every lock-free scan
acquires its snapshot's readers while still holding the lock, and a
compaction only actually closes+deletes a retired file once every
outstanding acquire has released it.

**Describe a real concurrency bug this project found and fixed.**
`ConcurrentLsmKeyValueStore.keys()` originally snapshotted the `active`
MemTable's reference under the read lock, then iterated its live
`TreeMap` *after* releasing the lock — safe for immutable SSTables, unsafe
for `active`, which a concurrent writer keeps mutating. This threw a real,
reproducible `ConcurrentModificationException` under a benchmark's tight
polling loop (Phase 12). Fixed by moving the MemTable key-collection
inside the locked section; only the SSTable scan stays lock-free.
PROGRESS.md's Phase 12 section has the exact stack trace and fix.

## Networking / protocol

**Why is the wire protocol a sealed, exhaustively-switched type rather
than something more generic?**
`Request`/`Response` are sealed interfaces; every `switch` over them is
exhaustive, so adding a new variant is a compile error everywhere it isn't
handled — a real safety net for a protocol that will keep growing. Phase
7 proved this works: adding `ERROR_NOT_OWNER` required touching every
exhaustive switch, and the compiler caught every spot.

**Why UDP for heartbeats but TCP for everything else?**
Heartbeats are fire-and-forget and tolerate loss by design — that's UDP's
actual delivery semantics, not something bolted onto TCP's stronger
ordering/connection guarantees that heartbeats don't need. Client
traffic, replication, and Raft RPCs all need reliable, ordered delivery,
so they're TCP. See `HeartbeatService`'s class Javadoc.

## Partitioning

**Why consistent hashing with virtual nodes, not `Object.hashCode() % N`?**
Modulo hashing remaps *most* keys when the node count changes — every key
effectively moves. Consistent hashing remaps only the keys near the
affected point on the ring. Virtual nodes (128 per physical node here)
smooth out load distribution that a single hash point per node wouldn't.
`ConsistentHashRing` uses SHA-256, not Java's own `hashCode()`, for
stable, well-distributed placement independent of JVM implementation
details.

## Replication and failure handling

**What happens when a leader dies mid-replication?**
Whatever a follower already received stays intact and fully usable — the
follower's own store never depended on the leader staying alive to serve
what it already has (Phase 11 Scenario A). What's lost is anything the
leader had acknowledged locally but never forwarded to any follower —
disclosed plainly in docs/FAILURE_MODEL.md, not hidden.

**Why is replication asynchronous, and what does that actually cost?**
A leader's local write never waits for a follower's ack — `notifyReplicationListeners`
runs after the local WAL fsync completes and the lock is released.
Measured cost (BENCHMARKS.md §7.2, E13): leader-local PUT latency roughly
doubles with one follower attached and nearly triples with two — a real,
somewhat surprising finding, investigated (not just asserted) as likely
coming from that same notification loop running synchronously on the
writer's thread before `put()` returns.

**What happens during a network partition between leader and follower?**
The leader keeps accepting writes (never blocks on a follower). The
follower's already-replicated data stays fully consistent throughout.
Reconnecting after the partition heals resumes catch-up from exactly
where it left off, verified to reach full parity again (Phase 11 Scenario
C).

**Is there a fencing/epoch mechanism to prevent split-brain?**
No — stated plainly rather than assumed. `ForgeServer`'s partition
ownership is a static predicate set at construction; nothing updates it
based on Raft or any other signal at runtime. Two nodes could both
believe themselves leader of the same partition after a split-brain
event today. Raft's terms are exactly the mechanism that would close this
once wired into the data plane — see the consensus section below and
docs/CONSISTENCY.md §5.

## Recovery

**How does a node that's too far behind for WAL replay catch up?**
`SnapshotServer`/`SnapshotClient` transfer a full point-in-time-ish
snapshot (captured watermark + every live key/value), reusing
`SSTableWriter.write`'s exact temp-file/`force()`/atomic-rename crash
safety. The receiving `ConcurrentLsmKeyValueStore.loadSnapshot()` then
raises its WAL's next-sequence-number past the watermark, so a
`ReplicationFollower` started immediately after resumes precisely where
the snapshot left off — no gap, no re-fetching. Proven to converge
correctly even while the source is being concurrently written to
(`SnapshotTransferIntegrationTest`).

## Consensus (Raft)

**Walk through what happens when a leader dies, precisely.**
Followers stop receiving AppendEntries heartbeats. Once one follower's
randomized election timeout elapses, it increments its term, votes for
itself, and requests votes from every peer. If it collects a majority
(including its own vote), it becomes leader, appends a no-op entry for
the new term, and starts sending heartbeats. `RaftClusterIntegrationTest`
proves this over real sockets: closing the leader's process, the two
survivors converge on a new leader at a strictly higher term.

**What's the Figure 8 safety property, and why is it easy to get wrong?**
A leader must never commit a log entry from an *older* term purely by
counting replicas — only a later, current-term entry independently
reaching majority can commit it (and, as a side effect, everything before
it). Getting this wrong lets a committed entry later get silently
overwritten by a future leader that never actually had it. `RaftNode.recomputeCommitIndex()`
enforces this with one line (`if (termAt(n) != currentTerm) continue;`);
`RaftNodeTest` scenario 7 constructs a real instance of the trap and
confirms `commitIndex` correctly refuses to advance until the current-term
entry commits.

**How does a stale leader get rejected after a partition heals?**
Its AppendEntries carries its old term. Any node that has since moved to
a higher term rejects it immediately (`request.term() < currentTerm`) and
reports its own current term back — the stale leader, seeing a higher
term in the response, steps down to follower. `RaftNodeTest` scenario 10
tests exactly this.

**Is this really Raft, or is it Phase 9's replication renamed?**
Genuinely separate mechanics solving different problems. Raft's log here
carries exactly one no-op entry per election — a control-plane "who
leads" marker — never real KV commands. Phase 9's
`ReplicationServer`/`ReplicationFollower`, built on WAL sequence numbers,
remain the unmodified data plane. This was an explicit project
requirement ("do not merely rename the existing replication system
Raft") satisfied by construction, not just by naming.

**What's the biggest thing this Raft implementation still doesn't do?**
Post-Phase-15 audit: it now persists `currentTerm`/`votedFor`
(`RaftPersistentState`, mirroring the WAL's temp-file/force/rename
discipline), closing the double-vote-after-restart gap the paper is
strictest about. What it still doesn't persist is the log itself — for
this project that's a deliberate, low-cost choice (the log only ever
holds disposable no-op leadership markers), but it has one real,
disclosed consequence: a restarted node's log is always empty, so Raft's
own up-to-date-log election rule correctly refuses to elect it, no matter
how high its term climbs. In a 2-node cluster that's a genuine permanent
liveness dead end; at 3+ nodes it isn't, since the surviving majority can
elect without that node's vote at all. Found and documented via a flaky
test, not by inspection alone — see PROGRESS.md's audit section for the
exact story.

## Data-plane failover & fencing (Phase 15)

**How does Raft actually control the data plane?**
`ForgeServer` is constructed with a `WriteAuthority` — for a Raft-managed
partition, that's `PartitionLeadership`, which delegates to
`RaftCluster.canServeAuthoritatively()`. Every `PUT`/`DELETE` calls
`writeAuthority.canAcceptWrites()` before touching the store; a `false`
means `ERROR_NOT_LEADER`, never applied. Separately,
`ReplicationFollowerCoordinator` polls `RaftCluster.currentLeader()` and
keeps each node's `ReplicationFollower` pointed at whoever it currently
names. Two independent consumers of the same one signal — no new
coupling between Raft's internals and replication's internals.

**How do you prevent a stale leader from continuing to serve writes?**
Two layers. First, the fast path: any RPC or response carrying a higher
term makes a node step down immediately (`RaftNode.stepDownToFollower`,
unchanged since Phase 14). Second — the layer Phase 15 actually added —
the slow path for a node that never receives such a message because it's
genuinely partitioned away: `RaftNode.hasRecentQuorumContact(leaseDuration)`
tracks the timestamp of each peer's most recent successful AppendEntries
ack in the current term, and answers false once a majority's worth of
those acks are older than `leaseDuration`. `canServeAuthoritatively()` is
`isConfirmedLeader() && hasRecentQuorumContact(...)` — a leader that's
alive but isolated stops passing that check purely from the passage of
its own clock, without needing anyone to tell it anything.

**What happens during a network partition?**
The minority side's leader (if any) loses quorum contact and self-fences
within one lease duration; the majority side, if it has one, elects a new
leader and keeps serving. Proved directly: chaos Scenario I asserts both
halves *during* the partition (majority authoritative, minority fenced,
simultaneously); `StaleLeaderFencingIntegrationTest` and Scenario H add
the reconnect-afterward story (the old leader discovers the higher term
and stays fenced once healed).

**How does fencing interact with sequence numbers across a failover?**
This was the subtle part. Without `SequenceEpochs`, a promoted follower's
first new write would get the *next* sequence number after wherever it
had already caught up to — which could collide with a different write the
old leader had assigned that same number to, if the old leader got further
ahead locally than it ever replicated. `SequenceEpochs.bandStart(term)`
gives every term a disjoint billion-wide range; both the new leader
(on confirming) and every follower (on redirecting to it) ratchet their
own store into that band *before* any new data flows, so the numbers can
never collide — with zero changes to the WAL record format, the
replication wire format, or the gap-detection logic that already existed.

**What happens to async-replicated writes after a leader failure?**
Exactly what Phase 9/11 already disclosed, unchanged: whatever the old
leader had already shipped to a follower survives and is fully usable;
whatever it had only applied locally and never shipped is not recovered
by anything in this codebase — it's simply left behind in the old
leader's own local state. Phase 15 does not make this better or worse; it
only stops the old leader from adding *more* such orphaned writes once
its lease expires.

**How does an old leader rejoin safely?**
Its `RaftNode` now reloads its persisted `currentTerm`/`votedFor` on
restart (see above), but its log still starts empty either way, so it
still can't itself win an election against a peer with real log entries —
it steps down as soon as it hears the current term from a real message,
same as before persistence existed. Its `ReplicationFollowerCoordinator`
then checks: is this the *first* time (this process's life) it's
considered following anyone, and does its existing data imply an older
term than the cluster's current one? If so, it flags `needsFullResync()`
rather than guessing — its local data might hold orphaned writes an
incremental catch-up can't retroactively fix. A caller then invokes
`StaleReplicaRecovery.resyncFromSnapshot`, which wipes the node's local
directory and reloads it completely from a live `SnapshotServer` (Phase
10's existing transfer mechanism, unmodified). If the node was always
just a plain follower — never originated a write — later failovers skip
this check entirely and it just reconnects incrementally, since a pure
follower's data can never actually diverge.

**Why is Raft still not simply the existing replication system renamed?**
Same answer as Phase 14, now with a concrete data-plane consequence to
point at: Raft's own log still carries nothing but one no-op per
election. If Phase 15 had instead made Raft's log carry real KV writes,
it would have needed to either duplicate everything `ReplicationServer`/
`ReplicationFollower` already do correctly, or rip out Phase 9 entirely —
neither happened. What Phase 15 added is a *coordinate* (the term, via
`SequenceEpochs`) the two independent systems now both respect, not a
merger of the two.

**What are the exact consistency guarantees, precisely?**
Writes: accepted only by a node currently, recently confirmed
authoritative (bounded lease window, not instantaneous). Reads:
deliberately never fenced — any node, including a demoted former leader,
answers `GET` from local data. Durability: still async-only; a write
acknowledged by an authoritative leader can still be lost if that leader
crashes before replicating it anywhere — Raft's commit mechanism and KV
write durability are two separate things that were never merged. See
`docs/CONSISTENCY.md` §5 for the full, precise table.

## Benchmarking

**Walk through the most interesting benchmark result and what it taught
you.**
Node scaling (BENCHMARKS.md §7.1, E12): even after fixing an earlier
benchmark-design bug (holding total client concurrency fixed regardless
of node count, which understated the true comparison), throughput barely
improved from 1 to 4 nodes (263 → 321 ops/sec, far from linear) while p99
latency grew nearly 8×. Investigated, not asserted: every node in this
benchmark runs on one physical machine sharing one disk, so this most
likely measures disk-fsync contention across N processes, not a limit in
FORGE's partitioning logic — and that's stated as a real limitation of
the benchmark's environment, not smoothed over as a FORGE finding.

**What's a rejected optimization, and why?**
Sync replication (an ack-quorum-based durability mode) was in the
original design plan but never built — DESIGN.md §2 records this
correction explicitly rather than silently deleting the original plan.
Async-only was kept because it's what got built and tested; adding a sync
mode is real, disclosed future work, not something quietly abandoned.

**What would you change if this went to production tomorrow?**
Both biggest items on this list as of the original Phase 15 report — wiring
Raft into the data plane, and persisting Raft's `currentTerm`/`votedFor`
— are now done. What's left, in order: persist the Raft log too (closes
the 2-node liveness gap discovered while adding term/vote persistence —
see PROGRESS.md's audit section); extend fencing to a genuine
multi-partition deployment; add authentication/TLS (never attempted — out
of this project's scope); add a leveled compaction strategy if datasets
grew past what full-rewrite compaction handles comfortably. See README
§22/23 for the full list, in priority order.

## Post-Phase-15 engineering audit

**Why persist `currentTerm`/`votedFor` but not the log?**
The log only ever carries one no-op "I am leader" marker per election —
never real KV data (Phase 9's replication is the actual data path,
untouched). Losing it on restart costs a follower nothing (the existing
`nextIndex` back-off already repairs it from whichever leader is current).
Persisting `currentTerm`/`votedFor` closes a real, paper-cited safety gap
(double-voting across a restart) for a small, well-contained change;
persisting the log too would be a materially bigger one (durable storage
keyed to every log mutation/truncation) for data this project already
doesn't need to survive a restart as a follower. The honest cost of that
choice — a restarted node can't itself win an election again in a 2-node
cluster — is disclosed, not hidden; see the previous section.

**How did you find the 2-node liveness gap?**
A flaky test, not a design review. A "restart reloads its persisted term"
test using a 2-node cluster intermittently failed waiting for the
restarted node to become leader again — about 1 run in 5. `jstack`-style
reasoning wasn't needed this time; reading the actual `RaftNode` code
explained it immediately: the restarted node's empty log always loses
Raft's up-to-date-log check against its peer's non-empty one, regardless
of term, and the peer alone can never reach a 2-node majority either.
Structurally the same lesson as Phase 15's own chaos Scenario K (a
test that assumed a minority could still elect a leader) — recognized as
a test correctly exposing a real, narrow, disclosed property, not a
defect to code around, and fixed by testing the actually-correct behavior
(a 3-node cluster's surviving majority elects without that node's vote,
and it rejoins as a follower) instead.

**Why does a real multi-node launcher matter if the tests already prove
everything works?**
Because "the tests prove it" and "you can watch it happen" are different
kinds of confidence, and the project's own `docs/DEMO.md` had said outright
that only the first one existed. `ClusterNodeMain` is deliberately thin —
all the actual wiring is the same construction sequence
`FailoverReplicationIntegrationTest` already used — because the goal
wasn't a new mechanism, it was making an already-correct one demonstrable
as real, independent operating-system processes: a real election, a real
client write through the elected leader, a real rejection from a
follower, and a real `kill -9`-triggered failover, confirmed manually
(not just via the automated in-process tests) and recorded in
`docs/DEMO.md` Step 13.
