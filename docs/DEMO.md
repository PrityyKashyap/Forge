# FORGE — Demo Script

A reproducible walkthrough using only commands that actually exist and
actually work in this repository (every one was run and its real output
checked while writing this document). There is currently no single
"launch an N-node cluster and poke it with a CLI" tool — see README's
[Limitations](../README.md#21-limitations) — so this demo instead runs the
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

## Step 6 — Consensus: real election and leader-crash failover

```bash
mvn -pl forge-cluster -am test -Dtest=RaftClusterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
```

This is the closest thing to "watch a live cluster fail over" this
project has: three real `RaftCluster` instances, real `RaftRpcServer`
sockets, elect exactly one leader within a few hundred milliseconds; the
test then **closes the leader's process** (simulating a crash, no
graceful goodbye) and watches the two survivors independently converge on
a *new* leader at a strictly higher term. 2/2 tests pass. For the full
set of adversarial scenarios this consensus implementation is proven
against (split votes, stale leaders, the Raft "Figure 8" commit-safety
trap, and more) with no network involved and a fake clock for speed:

```bash
mvn -pl forge-cluster -am test -Dtest=RaftNodeTest -Dsurefire.failIfNoSpecifiedTests=false
```

13/13 tests pass in well under a second.

## Step 7 — Compaction and Bloom filters: real, measured amplification

```bash
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.CompactionBenchmarkRunner
```

Watch the printed summary — real numbers, this run's own: 28 SSTables
collapse into 1, on-disk bytes drop by ~90%, and miss-lookup latency
drops ~3.4× once Bloom filters have fewer tables to check. See
[BENCHMARKS.md §8](BENCHMARKS.md#8-phase-13--compaction-readwrite-amplification-e16)
for the full writeup of this exact run.

## Step 8 — The full benchmark suites (optional, ~3-4 minutes)

```bash
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.BenchmarkRunner            # Phase 6: single-node
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.DistributedBenchmarkRunner  # Phase 12: multi-node/replication
```

Each prints a full results table and writes a timestamped CSV to
`forge-bench/results/` — the exact source of every number in
[BENCHMARKS.md](BENCHMARKS.md).

## Step 9 — The whole suite, once, end to end

```bash
mvn clean test
```

444 tests, 0 failures — every one of the behaviors above, plus unit-level
coverage of the WAL, SSTable format, MemTable, concurrency, protocol
framing, and membership/failure-detection state machines, all in one run
(roughly 2 minutes).

---

**What this demo deliberately does not show**: a genuine "type a command,
watch three separate terminal windows react" experience, since no
standalone multi-node launcher exists yet (README §21/§22). Everything
shown above is real — real sockets, real separate server/client/consensus
objects, real crashes (`RaftCluster.close()` really does simulate one) —
just orchestrated by test/benchmark code rather than a CLI. Building that
launcher is the natural next step to make this demo more visually
compelling; see README §22 (Future Work), item 1.
