# FORGE — Demo Script

A reproducible walkthrough using only commands that actually exist and
actually work in this repository (every one was run and its real output
checked while writing this document). There is currently no single
"launch an N-node cluster and poke it with a CLI" tool — see README's
[Limitations](../README.md#22-limitations) — so this demo instead runs the
real integration tests and benchmark runners that already spin up genuine
multi-process behavior with real sockets, and shows you what to look for
in their output. Total time: 3-5 minutes.

Run everything from the repository root.

## Step 0 — Build once

```bash
mvn clean install -DskipTests
```

## Step 1 — A single node, real client, real durability

```bash
mvn -pl forge-server exec:java -Dexec.args="/tmp/forge-demo-node 17070"
```

Leave this running in one terminal. You'll see:
```
... INFO com.forge.server.Main -- FORGE server listening on port 17070 (data directory: /tmp/forge-demo-node)
```

Write to it from your own code with `ForgeClient.connect("localhost", 17070)`
(README §17), or via any of the integration tests below, which each start
their own throwaway servers the same way. Stop this server (Ctrl-C), then
inspect what it durably wrote — `status` is an **offline** tool, so it
only works while the server is stopped (docs/OPERATIONS.md):

```bash
mvn -pl forge-server exec:java -Dexec.args="status /tmp/forge-demo-node"
```

Restart the server (the Step 1 command again) — your data is still there;
that's WAL replay on startup (docs/FAILURE_MODEL.md §2).

## Step 2 — Consistent-hash partitioning across real nodes

```bash
mvn -pl tests -am test -Dtest=PartitioningIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

This starts several real `ForgeServer` processes-in-the-JVM behind a
`ConsistentHashRing`, routes keys through `PartitionedForgeClient`, and
asserts each key only ever lands on its owning node — a node asked for a
key it doesn't own returns `ERROR_NOT_OWNER` rather than serving it.
5/5 tests pass.

## Step 3 — Leader-follower replication

```bash
mvn -pl forge-cluster -am test -Dtest=ReplicationIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

Watch the log lines:
```
... INFO c.f.c.replication.ReplicationServer - follower f1 connected, requesting catch-up from sequence 1
```
Real `ReplicationServer`/`ReplicationFollower` pairs, real sockets,
streaming actual WAL records. 9/9 tests pass, including a follower
catching up while writes are concurrently happening on the leader.

## Step 4 — Recovery: bootstrap a far-behind node from a snapshot

```bash
mvn -pl forge-cluster -am test -Dtest=SnapshotTransferIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

Proves a node too far behind for WAL replay alone can bootstrap from a
full snapshot and then resume WAL-based catch-up exactly where the
snapshot left off — even while the source is still being written to.
6/6 tests pass.

## Step 5 — Chaos: kill the leader mid-replication

```bash
mvn -pl forge-cluster -am test -Dtest=ChaosScenarioTest -Dsurefire.failIfNoSpecifiedTests=false
```

Six named, deterministic (never random) fault scenarios: leader crash
during active replication, follower disappears and rejoins, network
partition, significant message delay, crash during bootstrap, and
disagreeing membership views. 6/6 pass — each one asserts both a safety
property (nothing corrupts or duplicates) and a liveness property (the
system recovers).

## Step 6 — Consensus: real election

```bash
mvn -pl forge-cluster -am test -Dtest=RaftClusterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

Three real `RaftCluster` instances, real `RaftRpcServer` sockets, elect
exactly one leader within a few hundred milliseconds; the test then
**closes the leader's process** (simulating a crash, no graceful
goodbye) and watches the two survivors independently converge on a *new*
leader at a strictly higher term. 2/2 tests pass. This is Raft's
control-plane election alone, proven separately from the data-plane
failover Steps 7-9 build on top of it. For the full set of adversarial
scenarios this consensus implementation is proven against (split votes,
stale leaders, the Raft "Figure 8" commit-safety trap, and more) with no
network involved and a fake clock for speed:

```bash
mvn -pl forge-cluster -am test -Dtest=RaftNodeTest -Dsurefire.failIfNoSpecifiedTests=false
```

17/17 tests pass in well under a second.

## Step 7 — The full failover story, with data actually moving

This is the strongest single demonstration in this project: a real
3-node cluster (`RaftCluster` + `ForgeServer` + `PartitionLeadership` +
`ReplicationFollowerCoordinator`, wired together exactly as
`docs/ARCHITECTURE.md` §3.13 describes) where (1) a leader is elected,
(2) partition ownership and the current Raft leader are both directly
queryable (`raftA.isConfirmedLeader()`, `raftA.currentTerm()`), (3) a
real client write lands on the leader, (4) real replication carries it to
both followers, (5) the leader is killed outright, (6) a real election
happens, (7) a new leader is confirmed, (8) another real client write
succeeds through the new leader, (9) the surviving follower's
`ReplicationFollowerCoordinator` automatically redirects itself and
receives that write, (10) the old leader "restarts" (a genuinely fresh
`RaftCluster` — Phase 14 keeps no persistent state, so this is exactly
what a real restart produces), (11) it discovers the new term and becomes
a follower, (12) it is resynced from a live snapshot of the new leader
(`StaleReplicaRecovery`, since its own data implies an older term), (13)
its data is verified identical to the new leader's, including the write
made after it failed:

```bash
mvn -pl forge-cluster -am test -Dtest=FailoverReplicationIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

Watch the log lines — every one of the 13 steps above is directly visible:
```
... now following leader fo-a (term 1) from sequence 1000000000
... follower fo-b connected, requesting catch-up from sequence 1000000000
... stopping current replication follower (switching to leader fo-c for term 2)
... now following leader fo-c (term 2) from sequence 2000000000
... wiped local data at .../a ahead of a full snapshot resync from localhost:PORT
... sent a 3-key snapshot at watermark 2000000000
... resync complete: .../a now holds 3 keys
```
1/1 test passes, consistently (run it a few times in a row — no flakiness).

## Step 8 — The mandatory case: a leader that's alive but stale

The harder failure mode: not a crash, but a leader that's still running
and would happily keep answering — genuinely, bidirectionally
network-partitioned (a real `FaultInjectingTcpProxy` on every link, not
`close()`) from the rest of the cluster, and never told a new term
exists:

```bash
mvn -pl forge-cluster -am test -Dtest=StaleLeaderFencingIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

The old leader's own election-timeout-driven lease expires purely from
the passage of time — nobody sends it anything — and a client write
against it is rejected with `ERROR_NOT_LEADER` while the new leader keeps
serving. After healing, it discovers the higher term, steps down, and
stays fenced. 1/1 test passes. For six more named, smaller scenarios
covering the same territory from different angles (partition observed
*during*, not just after; a deliberately delayed stale message; repeated
crashes; a follower crashing mid-catch-up):

```bash
mvn -pl forge-cluster -am test -Dtest=ChaosScenarioTest -Dsurefire.failIfNoSpecifiedTests=false
```

12/12 pass (the original six from Phase 11, plus six for Phase 15).

## Step 9 — Real, measured failover timing

```bash
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.FailoverBenchmarkRunner
```

Prints real election time, leadership-transition time, and time-to-first-successful-write
across 5 repeats — see
[BENCHMARKS.md §9](BENCHMARKS.md#9-phase-15--failover-timing-e17) for the
exact numbers from this project's own run and what they mean.

## Step 10 — Compaction and Bloom filters: real, measured amplification

```bash
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.CompactionBenchmarkRunner
```

Watch the printed summary — real numbers, this run's own: 28 SSTables
collapse into 1, on-disk bytes drop by ~90%, and miss-lookup latency
drops ~3.4× once Bloom filters have fewer tables to check. See
[BENCHMARKS.md §8](BENCHMARKS.md#8-phase-13--compaction-readwrite-amplification-e16)
for the full writeup of this exact run.

## Step 11 — The full benchmark suites (optional, ~3-4 minutes)

```bash
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.BenchmarkRunner            # Phase 6: single-node
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.DistributedBenchmarkRunner  # Phase 12: multi-node/replication
```

Each prints a full results table and writes a timestamped CSV to
`forge-bench/results/` — the exact source of every number in
[BENCHMARKS.md](BENCHMARKS.md).

## Step 12 — The whole suite, once, end to end

```bash
mvn clean test
```

472 tests, 0 failures — every one of the behaviors above, plus unit-level
coverage of the WAL, SSTable format, MemTable, concurrency, protocol
framing, and membership/failure-detection state machines, all in one run
(roughly 2 minutes).

---

**What this demo deliberately does not show**: a genuine "type a command,
watch three separate terminal windows react" experience, since no
standalone multi-node launcher exists yet (README §22/§23). Everything
shown above is real — real sockets, real separate server/client/consensus
objects, real crashes (`RaftCluster.close()` really does simulate one),
a real network partition (`FaultInjectingTcpProxy`), and a real snapshot
resync — just orchestrated by test/benchmark code rather than a CLI.
Building that launcher is the natural next step to make this demo more
visually compelling; see README §23 (Future Work).
