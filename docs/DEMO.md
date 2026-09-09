# FORGE — Demo Script

A reproducible walkthrough using only commands that actually exist and
actually work in this repository (every one was run and its real output
checked while writing this document). Steps 1-12 run the real integration
tests and benchmark runners that spin up genuine multi-process behavior
with real sockets inside one JVM, and show you what to look for in their
output; Step 13 is the real, standalone multi-process CLI launcher
(`ClusterNodeMain` — see README §19) as actually separate `java`
processes. Total time: 5-8 minutes including Step 13.

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

495 tests, 0 failures — every one of the behaviors above, plus unit-level
coverage of the WAL, SSTable format, MemTable, concurrency, protocol
framing, and membership/failure-detection state machines, all in one run
(roughly 2 minutes).

## Step 13 — The real multi-process launcher

Post-Phase-15 audit addition: every step above runs through test/benchmark
code that spins up real sockets and real objects, but still inside one
JVM. This step is the same failover story as Steps 7-9, plus the rejoin
resync story from Step 8's "old leader restarts" (10)-(13), as three
genuinely separate OS processes, driven only by a plain-text config file
and a real client — the actual command sequence, actual output, run once
while writing this document (updated 2026-09-09 with the current 6-field
config format and the `resync` subcommand added since the first version
of this step):

```bash
mvn -pl forge-cluster -am install -DskipTests

cat > /tmp/forge-demo/cluster.conf <<'EOF'
# nodeId  host       raftPort  replicationPort  clientPort  snapshotPort
n1        localhost  27001     27002            27003       27004
n2        localhost  27011     27012            27013       27014
n3        localhost  27021     27022            27023       27024
EOF

# one terminal each (or background processes, as done here) —
# forge-cluster's own pom.xml already defaults exec:java's mainClass to ClusterNodeMain:
mvn -pl forge-cluster exec:java -Dexec.args="/tmp/forge-demo/cluster.conf n1 /tmp/forge-demo/data-n1"
mvn -pl forge-cluster exec:java -Dexec.args="/tmp/forge-demo/cluster.conf n2 /tmp/forge-demo/data-n2"
mvn -pl forge-cluster exec:java -Dexec.args="/tmp/forge-demo/cluster.conf n3 /tmp/forge-demo/data-n3"
```

Each node's own log shows it coming up and finding the others; n3 won
the election this run (term 1):
```
INFO ClusterNodeMain -- FORGE cluster node 'n3' up — client port 27023, raft port 27021, data directory /tmp/forge-demo/data-n3
INFO ReplicationServer -- follower n1 connected, requesting catch-up from sequence 1000000000
INFO ReplicationServer -- follower n2 connected, requesting catch-up from sequence 1000000000
```
A real client against the real leader succeeds; the same write against a
real follower is rejected with the real protocol error, not silently
accepted; reads reach every node via real replication:
```
$ java ... Client put 27023 alpha 1     # through n3, the leader
PUT OK: alpha=1
$ java ... Client put 27003 alpha fail  # against n1, a follower
Exception in thread "main" com.forge.client.ForgeServerException: NOT_LEADER term=1 leader=n3
$ java ... Client get 27003 alpha       # n1's OWN store — got there by real replication
GET alpha = 1
```
Killing n3's process outright (`kill -9`) produces a real election among
the two survivors:
```
INFO ReplicationFollowerCoordinator -- n1: stopping current replication follower (switching to leader n2 for term 2)
INFO ReplicationFollowerCoordinator -- n1: now following leader n2 (term 2) from sequence 2000000000
```
Committed data survives, new writes and deletes work through the new
leader n2, and n1 (a plain follower, gracefully `SIGTERM`'d and restarted
with the same data directory) rejoins instantly with no resync needed —
ordinary WAL replay is enough since its own persisted term already
matched the cluster's. Restarting the crashed n3 is different: it
**was** the old leader, so its `ReplicationFollowerCoordinator` correctly
refuses to trust its own data:
```
$ mvn -pl forge-cluster exec:java -Dexec.args="/tmp/forge-demo/cluster.conf n3 /tmp/forge-demo/data-n3"
...
WARN ReplicationFollowerCoordinator -- n3: leader n2 is on term 2 but my own data implies term 1 — a full snapshot resync is needed before I can safely follow again; not attempting incremental catch-up
```
Stop it again, resync it against the real current leader (n2), then
restart it normally — the exact `resync` subcommand from README §19:
```
$ kill -9 <n3's pid>
$ mvn -pl forge-cluster exec:java -Dexec.args="resync /tmp/forge-demo/cluster.conf n3 /tmp/forge-demo/data-n3 n2"
...
INFO ClusterNodeMain -- resyncing 'n3' at /tmp/forge-demo/data-n3 from 'n2' (localhost:27014)
INFO StaleReplicaRecovery -- wiped local data at /tmp/forge-demo/data-n3 ahead of a full snapshot resync from localhost:27014
INFO StaleReplicaRecovery -- resync complete: /tmp/forge-demo/data-n3 now holds 3 keys
INFO ClusterNodeMain -- resync of 'n3' complete — it can now be started normally
$ mvn -pl forge-cluster exec:java -Dexec.args="/tmp/forge-demo/cluster.conf n3 /tmp/forge-demo/data-n3"
...
INFO ReplicationFollowerCoordinator -- n3: now following leader n2 (term 2) from sequence 2000000002   # no WARN this time
```
After this, `GET` against all three nodes for every key written before
*and* after the crash returns identical, correct values — verified
directly, not assumed.

See [README §19](../README.md#19-how-to-run-a-multi-node-cluster--partitioning--replication--failover)
for the full command reference and [ClusterNodeMain](../forge-cluster/src/main/java/com/forge/cluster/launcher/ClusterNodeMain.java)'s
Javadoc for scope (single-partition; no process-management of its own).

---

Every scenario above is real — real sockets, real separate
server/client/consensus objects, real crashes (`RaftCluster.close()` in
the in-JVM steps, `kill -9` on a real process in Step 13), a real network
partition (`FaultInjectingTcpProxy`), and a real snapshot resync. Steps
1-12 orchestrate that reality through test/benchmark code, which is the
fastest way to exercise every named scenario reproducibly; Step 13 is the
same reality as standalone, independently-started processes — the
"type a command, watch three terminals react" experience previous
revisions of this document said didn't exist yet.
