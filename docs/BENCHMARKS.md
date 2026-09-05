# FORGE — Benchmarks

## Metrics tracked (full definitions in [DESIGN.md §3](DESIGN.md#3-observable-metrics))

- **Throughput** — completed ops/sec, per operation type (GET/PUT/DELETE)
- **Latency** — p50/p95/p99, per operation type
- **Storage overhead** — on-disk bytes ÷ logical bytes stored
- **Recovery time** — time from process start to fully caught up
- **Replication lag** — time delta between leader commit and follower apply
- **Failure rate** — failure-detector false-positive rate and true-positive
  detection time

## Experiments planned (full definitions in [DESIGN.md §4](DESIGN.md#4-experiments-what-will-actually-produce-benchmarksmd-data))

| # | Experiment | Runnable from | Status |
|---|---|---|---|
| E1 | Single-threaded in-memory baseline | Phase 1 | **Run — Phase 6, §2 below** (over the network; the in-process number is now also produced as a byproduct of E4) |
| E2 | Durability cost (WAL off/on, fsync policy) | Phase 2–3 | Not run — see note below |
| E3 | Concurrency scaling | Phase 4 | **Run — Phase 6, §3 below** |
| E4 | Network overhead (in-process vs. loopback vs. LAN) | Phase 5–6 | **Run — Phase 6, §4 below** (in-process vs. loopback only; no second machine available for the LAN leg) |
| E5 | Read/write mix & key skew | Phase 6, extended Phase 9 | **Partially run — Phase 6, §5 below** (read/write mix run; key skew/Zipfian explicitly not implemented, see §5) |
| E6 | Partition/rebalance cost | Phase 7 | Not run (Phase 7 not started) |
| E7 | Failure-detector tuning | Phase 8, extended Phase 11 | Not run (Phase 8 not started) |
| E8 | Replication ack policy (async vs. sync) | Phase 9 | Not run (Phase 9 not started) |
| E9 | Recovery time vs. data size | Phase 10 | **Run — Phase 12, §7 below (as E14)** |
| E10 | Chaos suite (kill leader/follower, partition, slowdown, flapping) | Phase 11 | Not run as a *benchmark* — Phase 11's six scenarios prove correctness/safety/liveness properties (PROGRESS.md), not throughput/latency under fault; no chaos-scenario numbers exist |
| E11 | Storage overhead over time (pre/post compaction) | Phase 3, Phase 13 | **Run — Phase 13, §8 below (as E16)** |
| E12 | Node/cluster scaling (real multi-node PUT throughput) | Phase 7, extended Phase 9 | **Run — Phase 12, §7 below** |
| E13 | Replication overhead (leader-local PUT latency vs. follower count) | Phase 9 | **Run — Phase 12, §7 below** |
| E15 | Replica catch-up time vs. backlog size | Phase 9–10 | **Run — Phase 12, §7 below** |
| E16 | Compaction read/write amplification | Phase 13 | **Run — Phase 13, §8 below** |
| E17 | Failover timing (election, leadership transition, time to first write) | Phase 15 | **Run — Phase 15, §9 below** |

**E6/E7/E8 note**: none of these were run this phase. E6 (partition/rebalance
cost) and E8 (replication ack policy: async vs. sync) have no dedicated
benchmark built — FORGE's replication is async-only (Phase 9 design
decision, DESIGN.md §6), so there is no sync mode to compare against, and
`PartitionRebalancer.migrate` (Phase 7) has never been timed. E7
(failure-detector tuning) would require sweeping `HeartbeatService`'s
timeout/interval constants against false-positive/detection-time trade-offs,
which Phase 12 did not build. All three remain honestly "not run" rather
than approximated.

**E2 note**: `WriteAheadLog.appendPut`/`appendDelete` call `FileChannel.force(true)`
unconditionally on every mutation — there is no fsync-off or batched-fsync mode
in the codebase to sweep. Building one is a `forge-storage` change, not a
Phase 6 (`forge-server`/`forge-bench`) change, so it's out of scope here;
E2 stays "Not run." What Phase 6 *does* establish is that fsync is
unconditionally on, and quantifies its cost (see §2–§4: PUT/DELETE are
consistently ~250 ops/sec, ~4ms/op, essentially unaffected by concurrency —
this is the fsync cost, isolated and measured).

## §1 Methodology

**Environment** (same machine for every run below):
- Hardware: Apple Silicon Mac, arm64, 10 CPU cores, 16 GiB RAM
- OS: macOS 26.6.2 (build 25G83)
- JDK: OpenJDK 26.0.2.1 (Homebrew build), `--release 21` compiled bytecode
- Maven: 3.9.16

**Server placement**: the benchmark harness (`forge-bench`) starts a real
`ForgeServer` **in the same JVM process** as the load generator, listening on
an ephemeral loopback port (`127.0.0.1`). This is a genuine `java.net.Socket`
TCP connection through the real kernel network stack — not a fake/in-memory
shortcut — but client and server share this machine's CPU cores and the
JVM's heap/GC, which the numbers below should be read with in mind. No
second machine was available to run a true LAN leg of E4; that comparison is
explicitly marked not run rather than estimated.

**Persistence/fsync**: always on for every experiment below. `WriteAheadLog`
calls `FileChannel.force(true)` synchronously inside `appendPut`/`appendDelete`,
and there is no way to disable it. Every PUT/DELETE measurement below
includes a real fsync to this machine's disk.

**Measured side**: all throughput and latency numbers are **client-observed**
— wall-clock time from the benchmark driver issuing a `ForgeClient.put/get/delete`
call to that call returning, via `System.nanoTime()`. This includes
client-side request encoding, the full network round trip, server-side
decoding/dispatch/storage-engine work/response encoding, and client-side
response decoding — not server-internal processing time alone.

**Dataset / workload parameters** (exact values in
`forge-bench/src/main/java/com/forge/bench/BenchmarkConstants.java`, so code
and this document can't drift apart):
- Value size: 100 bytes, fixed, deterministic content (content is irrelevant
  to a throughput/latency measurement; only size is, and generating fresh
  random bytes per op would add allocation/RNG cost to the timed path itself)
- Keys: deterministic, zero-padded (`prefix-0000000123`), never randomly
  generated inside a timed op
- Warm-up: run untimed, immediately before each measured phase, on the same
  connection(s) that go on to run the measured phase — discarded, never
  mixed into reported numbers (E1: 300 ops; E3 GET: 300 total, PUT: 100
  total per concurrency level; E4: 200 ops; E5: 400 total)
- Each experiment run **3 times** (`REPEATS = 3`); every individual run's
  numbers are reported in the CSV, not just an average — see §6

**Throughput calculation**: `total completed ops ÷ elapsed wall-clock
seconds` for the whole measured phase. Elapsed time is measured once, around
the entire phase (all threads together) — never summed from individual
threads' durations, which would double-count time under concurrency.

**Latency calculation**: per-operation `System.nanoTime()` delta, one sample
per op. Percentiles use the nearest-rank method on a sorted-ascending copy of
the samples: for percentile *p* over *n* samples, the reported value is the
sample at 0-indexed position `ceil(p/100 × n) − 1`. E.g. for *n*=1000, p99 is
the 990th-smallest sample. Implemented in `LatencyStats`, unit-tested against
hand-computed expected values in `LatencyStatsTest` (7 tests, including that
sorting/percentile math isn't corrupted by unsorted input and doesn't mutate
its input).

**What "measured" vs. "derived" vs. "qualitative" means below**: a number
taken directly from a benchmark run (throughput, avg/p50/p95/p99/max
latency) is a **measured result**. A number computed by comparing two
measured results (e.g. "network overhead ≈ loopback avg − in-process avg")
is a **derived metric**, labeled as such. A statement about *why* a pattern
appears, not directly measured, is a **qualitative observation**, labeled as
such and never presented as a proven root cause without direct evidence.

**Raw data**: every individual run behind every number in this document is
in `forge-bench/results/phase6-benchmarks-2026-09-01T19-12-12.429128Z.csv`
(66 rows: one per experiment × configuration × repeat). Reproduce with:
```
mvn install -DskipTests
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.BenchmarkRunner
```

## §2 E1 — Sequential baseline (over the network)

Single connection, single thread, one operation type at a time, against a
freshly-populated dataset. 1000 measured ops per (op type, repeat).

Mean across 3 repeats (per-repeat numbers in the CSV):

| Op | Throughput (ops/sec) | Avg (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---|---:|---:|---:|---:|---:|
| GET | 33,950 | 0.030 | 0.053 | 0.061 | 0.126 |
| PUT | 251.9 | 3.969 | 4.085 | 4.795 | 12.237 |
| DELETE | 250.9 | 3.984 | 4.148 | 5.301 | 9.032 |

**Qualitative observation**: PUT and DELETE cost essentially the same
(~4ms) — both go through the identical WAL-append-and-fsync path
(`ConcurrentLsmKeyValueStore.put`/`delete` both call `wal.appendPut`/
`appendDelete`, each internally fsyncing); DELETE's tombstone insert is not
meaningfully more expensive than PUT's value insert. GET is ~130× faster
than PUT/DELETE because it never touches the WAL or fsync at all — see §4
for the direct isolation of this.

## §3 E3 — Concurrency scaling

GET-only and PUT-only, at connection counts {1, 2, 4, 8, 16, 32}. Total
measured ops per level held roughly constant (2000 for GET, 500 for PUT)
and split evenly across threads, so total work — not per-thread work — is
comparable across the sweep; see `ConcurrencyScalingExperiment`'s Javadoc.

Mean across 3 repeats per level:

| Concurrency | GET ops/sec | GET avg (ms) | GET p99 (ms) | PUT ops/sec | PUT avg (ms) | PUT p99 (ms) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 37,744 | 0.026 | 0.071 | 249.8 | 4.002 | 4.213 |
| 2 | 46,133 | 0.043 | 0.087 | 254.2 | 7.859 | 9.142 |
| 4 | 68,497 | 0.058 | 0.096 | 250.1 | 15.943 | 18.549 |
| 8 | 96,603 | 0.083 | 0.154 | 250.1 | 31.744 | 40.371 |
| 16 | 105,718 | 0.146 | 0.290 | 250.0 | 63.005 | 67.704 |
| 32 | 104,510 | 0.289 | 1.122 | 252.8 | 122.384 | 130.691 |

**Measured result, plainly stated**: PUT throughput does **not** scale with
concurrency — it stays pinned at ~250 ops/sec from 1 connection all the way
to 32, while PUT average latency grows almost exactly linearly with
concurrency (4.0 → 7.9 → 15.9 → 31.7 → 63.0 → 122.4 ms — each doubling of
concurrency roughly doubles latency).

**Qualitative observation, but a well-evidenced one**: this is
`WriteAheadLog`'s single-writer bottleneck, exactly as predicted in Phase 4's
design docs ("writes do not get faster under concurrency, only safer").
`appendPut`/`appendDelete` are `synchronized`, so only one caller's fsync can
be in flight at a time; every other concurrent PUT queues behind it. The
arithmetic is self-consistent: ~4ms/op ⇒ a theoretical ceiling of
1000ms/4ms ≈ 250 ops/sec, which is exactly the ceiling measured at every
concurrency level. The linear latency growth is the queueing-theory
signature of *N* threads sharing one serialized resource: at concurrency
*C*, a request waits behind roughly *C*−1 others ahead of it before its own
turn, so average latency ≈ *C* × (per-op service time) — 32 × ~4ms ≈ 128ms,
matching the measured 122ms closely.

GET, by contrast, scales well through 16 connections (37.7K → 105.7K
ops/sec) before flattening at 32 (104.5K) — consistent with this 10-core
machine's CPU becoming the limiting resource once connection count exceeds
available cores, rather than any lock inside FORGE (`get()` takes a
`ReentrantReadWriteLock` read lock, not `synchronized`, specifically so it
doesn't serialize the way WAL writes do).

## §4 E4 — Network overhead (in-process vs. loopback TCP)

Identical GET/PUT workload (800 measured ops), run two ways: directly
against a `ConcurrentLsmKeyValueStore` (no socket, no `FrameCodec`, no
`ForgeServer`) vs. through a real `ForgeClient` over loopback TCP. **Both
sides get a brand-new store — and, for the network side, a brand-new
server — for every repeat**, so neither side's numbers are contaminated by
work done in a different experiment or an earlier repeat (see §6 for why
this matters and what it looked like before this was fixed).

Mean across 3 repeats:

| Op | Mode | Throughput (ops/sec) | Avg (ms) | p99 (ms) |
|---|---|---:|---:|---:|
| GET | IN_PROCESS | 913,159 | 0.0010 | 0.0035 |
| GET | LOOPBACK_TCP | 25,818 | 0.0387 | 0.0726 |
| PUT | IN_PROCESS | 254.4 | 3.932 | 4.293 |
| PUT | LOOPBACK_TCP | 255.6 | 3.912 | 4.231 |

**Derived metric**: network+protocol overhead for GET ≈ 0.0387ms − 0.0010ms
≈ **0.038ms (38 microseconds) per operation**, a ~35× slowdown relative to
the in-process call. For PUT, the same subtraction gives ≈ −0.02ms — i.e.
no measurable overhead; loopback PUT is not actually slower than in-process
PUT.

**Qualitative observation**: this makes sense given §2/§3's fsync finding.
GET is fast enough in-process (~1 microsecond) that network/protocol
overhead (TCP round trip, `FrameCodec` encode/decode, socket syscalls) is
the dominant cost once a socket is in the picture. PUT is dominated by a
~4ms synchronous fsync regardless of path; 38 microseconds of network
overhead is noise next to 4,000 microseconds of disk force. In short: **the
network layer's cost is real and significant for reads, and completely
invisible for durable writes** — the storage engine's own bottleneck
already dwarfs it.

## §5 E5 — Mixed read/write workload

8 concurrent connections, 80% GET / 20% PUT (uniform key distribution over a
shared 5,000-key dataset), 2000 measured ops total. **Key skew (hot-key /
Zipfian distribution) — named as a variable to sweep in DESIGN.md's E5
definition — was not implemented this phase.** It wasn't part of the Phase 6
request, and a real Zipfian generator is nontrivial to get right honestly;
faking a "plausible" skewed distribution would be worse than not running it.
Marked **NOT RUN** for that sub-variable.

**E5 has two different, both-real answers depending on what's already
happened to the store it runs against** — see §6 for the full investigation.
The reliable, reproducible one:

**Isolated E5** (fresh server, fresh store, nothing run before it —
`E5OnlyDiagnosticRunner`), mean across 3 repeats:

| Op | Throughput (ops/sec) | Avg (ms) | p99 (ms) | Max (ms) |
|---|---:|---:|---:|---:|
| MIXED (combined) | 1,226.5 | 6.495 | 18.750 | 30.069 |
| GET slice | 971.1 | 5.649 | 16.996 | 28.665 |
| PUT slice | 255.5 | 9.705 | 22.672 | 30.067 |

Tight across repeats (1234.7 / 1242.9 / 1202.0 ops/sec — under 4% spread),
unlike the same workload run at the end of a long, heavily-loaded suite (§6).

**Derived metric, and it matches**: with 20% of ops being PUT and PUT
globally capped at ~250/sec (§3), the back-of-envelope prediction is
`measured_ops ÷ (0.20 × measured_ops ÷ 250)` = `250 ÷ 0.20` = **1,250 ops/sec**
— the isolated measurement (1,226.5) matches this within 2%. PUT dominates:
GET's own cost is negligible next to the WAL-serialized PUT floor it's
queuing behind.

## §6 What the benchmark run itself found (methodology-affecting)

Two real problems were found by running this suite and looking hard at the
output before trusting it — exactly the "look for benchmark contamination /
accidental synchronization" review this phase's process called for.

**Problem 1 (found and fixed): E4's original design shared one long-lived
server/store with E1/E3 for its network-side measurement, while giving the
in-process side a fresh store per repeat.** First run's raw numbers: loopback
GET at 26,393 / 25,876 / **45.4** ops/sec across the 3 repeats — a >500×
collapse in the third repeat, with average latency jumping from 0.038ms to
22ms. In-process GET stayed fast throughout (597K–987K ops/sec) because it
used a fresh store every repeat; only the network side, which kept
accumulating writes from the whole suite (tens of thousands of prior PUTs by
that point), degraded. This is consistent with FORGE having no compaction
yet: as more SSTables accumulate and flushes happen in the background, a
GET that misses the MemTable has more files to scan, and a concurrent
flush's disk I/O can transiently compete with other operations on the same
store. **Fixed** by giving the network side its own fresh server and store
per repeat too (`NetworkOverheadExperiment`, see its Javadoc) — after the
fix, loopback GET was stable across all 3 repeats (25,913 / 26,130 / 25,410
ops/sec; see §4's numbers, taken from the corrected run). This is flagged
as a **finding about the benchmark's own design**, not a FORGE correctness
bug — E1/E3/E5's shared-server design is intentional (they're meant to
reflect realistic sustained-server behavior), it was specifically E4's
apples-to-apples comparison that the sharing broke.

**Problem 2 (found, investigated, explained with concrete evidence — not
fully pinned down quantitatively, and that's reported plainly rather than
papered over): E5's numbers were wildly inconsistent between two full-suite
runs.** First full-suite run: E5 mixed throughput ≈1,200 ops/sec, avg
latency ≈6.5ms. Second full-suite run (the one that also produced the
corrected §4 numbers): E5 mixed throughput ≈43 ops/sec, avg latency ≈181ms,
p99 ≈441ms — a ~28× regression with no change to E5's own code or
configuration between the two runs. An isolated re-run of E5 alone (fresh
JVM, fresh server, nothing run before it) came back fast and stable all
three times (§5: 1,234.7 / 1,242.9 / 1,202.0 ops/sec) — so the slowdown is
not inherent to E5's workload; it's specifically tied to running it against
a store that has already absorbed a long, heavy prior workload (E1+E3, ~20K+
prior operations, in the full-suite case).

Two concrete, source-confirmed mechanisms explain *why* a heavily-loaded
store would hurt exactly this workload shape (concurrent, mixed GET+PUT)
without hurting E1/E3 (which never run GET and PUT concurrently against each
other):

1. **`ConcurrentLsmKeyValueStore.put`/`delete` hold the store's
   `stateLock.writeLock()` for the entire WAL append call, fsync included**
   (`ConcurrentLsmKeyValueStore.java` lines 159–178 and 216–235) — confirmed
   by reading the code, not inferred. Since `get()` needs `stateLock`'s read
   lock (line 195), **every concurrent GET blocks for the full ~4ms of any
   in-flight PUT/DELETE's fsync.** E1 and E3 never exercise this interaction
   (each runs one op type at a time), so it was never visible in their
   numbers; E5 exercises it on every single PUT among its 20%.
2. **`SSTableReader.get()` does a genuine, index-free linear scan of the
   on-disk file** — by design, per its own class Javadoc ("deliberately
   slower than an in-memory index — preloading would defeat the reason
   SSTables exist"). Measured directly with a small, isolated, in-process
   diagnostic (`SSTableScanCostDiagnosticRunner`, 8 forced flushes, 3,200
   keys, 3,200 timed GETs, no network, no concurrency — completes in
   seconds): a GET for a key sitting in the oldest of 8 SSTables cost 7–38
   microseconds (cheap — the scan stops at the first larger key in each
   newer, sorted file it has to skip), while a GET for a key at the *end* of
   the newest SSTable cost 87–158 microseconds (the scan has to read through
   roughly 400 preceding records first). Real, and it grows with accumulated
   data, but at 7–158 microseconds it's two to three orders of magnitude too
   small to explain a jump to 181ms by itself.

**What this adds up to, stated at the right confidence level**: mechanism 1
is confirmed and — being millisecond-scale, unlike mechanism 2's
microsecond-scale — is almost certainly the dominant contributor once a
concurrent mixed workload meets a store with enough accumulated PUT traffic
to matter; mechanism 2 is confirmed and real but secondary. What is **not**
established is the precise quantitative path from "these two mechanisms
exist" to "43 ops/sec in one specific run" — that would need direct
profiling (flight recorder / lock contention tracing) under the exact
failing conditions, which is beyond what this phase's process budgeted for.
Per this phase's "do not report a single lucky run, do not fabricate
numbers" requirement: **§5 reports the isolated E5 result as the reliable
baseline** (it reproduced cleanly three times), and **this section reports
the sustained-load degradation as a real, reproduced-once, mechanistically
well-motivated but not fully quantified phenomenon** — not smoothed over,
not asserted with false precision either.

**One methodology bug found and fixed along the way, unrelated to FORGE
itself**: `forge-bench/pom.xml`'s `exec-maven-plugin` originally hardcoded
`<mainClass>com.forge.bench.BenchmarkRunner</mainClass>` as a literal, so
`-Dexec.mainClass=...` on the command line was silently ignored — every
attempt to run an isolated diagnostic class actually re-ran the entire
Phase 6 suite instead, which is what first looked like an unexplained
multi-minute hang. Fixed by making it `<mainClass>${exec.mainClass}</mainClass>`
with a `<properties>` default of `BenchmarkRunner`, so the property is now
genuinely overridable. Caught by checking `jstack` output against what was
actually expected to be running, not assumed from elapsed time alone.

## §7 Phase 12 — Distributed benchmarks (E12–E15)

**Scope**: Phase 6 (§1–§6 above) measured a single node in isolation. Phase
12 extends the same measured-client, real-network, real-fsync methodology
to real multi-node clusters — actual `ForgeServer` processes, actual
`PartitionedForgeClient`/`ReplicationFollower` wire traffic, no mocks or
in-memory shortcuts anywhere. Phase 6's own results and CSV are unchanged
and untouched; this section only adds to them.

**Methodology differences from Phase 6, disclosed rather than silently
applied** (all constants in
`forge-bench/src/main/java/com/forge/bench/DistributedBenchmarkConstants.java`):
- **`REPEATS = 2`**, not Phase 6's 3. Every Phase 12 experiment first has to
  build a real cluster (spawn N `ForgeServer` processes, and for E13/E15,
  wire up real replication) from scratch before it can run — unlike Phase
  6's experiments, which mostly reuse one already-populated dataset across
  repeats. The same repeat count and dataset sizes as Phase 6 would make
  this suite take many times as long for comparatively little extra
  statistical confidence, so a smaller value was deliberately chosen and is
  recorded here rather than silently applied.
- **Smaller dataset/op counts** (E12: 400–1600 measured ops depending on
  node count, see below; E13: 200 ops; E14: 200/1000/3000 records; E15:
  200/1000/3000 backlog records) — same rationale.
- **Single machine, multiple processes** — exactly like Phase 6, every
  "node" in every experiment below is a real, separate `ForgeServer`
  process/thread pair, but all of them run on the **same physical machine**
  described in §1, sharing its CPU cores and its one physical disk. This
  matters a great deal for interpreting E12 below and is called out again
  there.

**Raw data**: `forge-bench/results/phase12-throughput-2026-09-04T16-26-21.899646Z.csv`
(12 rows: E12 × 3 node counts × 2 repeats, E13 × 3 follower counts × 2
repeats) and `forge-bench/results/phase12-duration-2026-09-04T16-26-21.899646Z.csv`
(12 rows: E14 × 3 dataset sizes × 2 repeats, E15 × 3 backlog sizes × 2
repeats). Reproduce with:
```
mvn install -DskipTests
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.DistributedBenchmarkRunner
```

### §7.1 E12 — Node/cluster scaling

Real clusters of 1, 2, and 4 nodes (consistent-hash-partitioned, Phase 7),
loaded through `PartitionedForgeClient` — one client instance per thread,
each doing PUTs against keys that route across whichever nodes actually own
them. **Concurrency scales with node count**: `E12_CONCURRENCY_PER_NODE = 4`
threads per node (so 4/8/16 threads for 1/2/4 nodes), with each thread
doing a fixed 100 measured PUTs regardless of node count — so total
measured work scales with node count too (400/800/1600 ops).

This is a deliberate fix to this experiment's own design, made this phase.
An earlier version held **total** concurrency fixed at 4 threads regardless
of node count. That made the 1-node and 4-node configurations not
comparable: the 4-node run spread only 4 threads across 4 nodes (~1
thread's worth of pressure per node), while the 1-node run put all 4
threads against that single node's WAL. Under that design, throughput came
back essentially flat (~260–290 ops/sec) at every node count — but that
flatness was an artifact of shrinking per-node load as nodes were added,
not a measurement of whether adding nodes adds capacity. Scaling
concurrency with node count keeps per-node client pressure comparable
across configurations, so a throughput difference actually reflects added
node capacity (or the lack of it).

Mean across 2 repeats:

| Nodes | Concurrency | Total ops | Throughput (ops/sec) | Avg (ms) | p99 (ms) |
|---:|---:|---:|---:|---:|---:|
| 1 | 4 | 400 | 263.7 | 15.111 | 17.914 |
| 2 | 8 | 800 | 263.6 | 29.707 | 55.488 |
| 4 | 16 | 1600 | 321.4 | 47.579 | 139.528 |

**Measured result, plainly stated**: throughput does **not** scale
proportionally with node count even with per-node client concurrency held
constant. 1→2 nodes (2× the concurrency, 2× the nodes) yields **no
measurable aggregate throughput gain** (263.7 → 263.6 ops/sec) while
average latency roughly doubles (15.1 → 29.7ms) and p99 more than triples
(17.9 → 55.5ms). 1→4 nodes (4× the concurrency, 4× the nodes) yields only a
**1.22× throughput increase** (263.7 → 321.4 ops/sec) — far from the 4×
increase a naive "N independent nodes should give N× capacity" reading
would predict — while p99 latency grows **7.8×** (17.9 → 139.5ms).

**Qualitative observation, stated at the right confidence level, not
overclaimed**: this pattern — throughput barely moving while latency grows
sharply — is the signature of requests queuing behind a bottleneck that
does **not** grow with node count. The leading candidate, consistent with
every prior phase's findings: **every node's WAL fsync is still capped at
the same ~250–300 ops/sec ceiling this project has measured since Phase 6
(§3), and all N nodes' WALs live on the same one physical disk on this
single-machine benchmark setup.** §1 already discloses that Phase 6's
numbers reflect one machine's CPU/JVM being shared between client and
server; Phase 12 adds N separate fsync-writing processes competing for that
same machine's single underlying disk write path. If the disk (or the
kernel's page-cache/fsync serialization in front of it) is the true shared
resource — not something inside FORGE's own code — then running more
`ForgeServer` processes doesn't add independent fsync capacity the way
running them on genuinely separate machines would; it adds more contention
for the same one. This would explain both halves of the result at once:
throughput is still gated near the single-node ceiling because the
disk can't actually serve N nodes' fsyncs in parallel any faster than it
serves one, while latency grows because more concurrent fsync-issuing
threads are now queuing for that same disk.

**What this is not**: this is not evidence that FORGE's partitioning or
replication logic fails to scale — Phase 7's consistent-hash routing and
the per-node storage engines are doing exactly what they're supposed to
(each node genuinely only serves the keys it owns; PROGRESS.md's Phase 7
integration test already confirms correct routing). It **is** evidence that
**this benchmark, run on one machine, cannot demonstrate true horizontal
scalability** — multiple `ForgeServer` processes on one box share the one
resource (disk fsync bandwidth) that Phase 6 already identified as FORGE's
dominant bottleneck. Confirming or ruling out the disk-contention
hypothesis directly (e.g. via per-process I/O accounting, or by re-running
on genuinely separate machines) is beyond what this phase's process
budgeted for, and is recorded here as unresolved rather than asserted as
proven. This limitation — no multi-machine environment available — already
applies to E4 in §1 and applies identically here.

### §7.2 E13 — Replication overhead

A real 1-leader cluster with 0, 1, or 2 real `ReplicationFollower`s
attached, each following live over a real socket connection. Sequential
(single-threaded, single connection) leader-local PUTs, measured with
`System.nanoTime()` exactly as in Phase 6 — this isolates the leader's own
write latency, not follower catch-up (that's E15).

Mean across 2 repeats:

| Followers | Throughput (ops/sec) | Avg (ms) | p99 (ms) |
|---:|---:|---:|---:|
| 0 | 258.8 | 3.864 | 6.091 |
| 1 | 132.6 | 7.549 | 11.256 |
| 2 | 102.4 | 9.776 | 16.020 |

**Measured result, plainly stated**: adding followers measurably increases
the leader's own local PUT latency — roughly **2× with one follower**
(3.9ms → 7.5ms) and **2.5× with two** (3.9ms → 9.8ms) — even though Phase 9
was explicitly designed so that replication is asynchronous and a leader's
local write is never supposed to *wait* for a follower's acknowledgment
(`ConcurrentLsmKeyValueStore.notifyReplicationListeners` is called only
*after* the local WAL append/fsync completes and `stateLock` is released —
see PROGRESS.md's Phase 9 section). This is a genuinely interesting result
that deserves honest investigation rather than a shrug, because it appears
to contradict the "local write is never blocked by replication" design
intent.

**Qualitative observation, explicitly not asserted as a proven root
cause**: the measurement includes only the leader-local PUT call itself —
it does not call into any follower code — so *if* replication is truly
fire-and-forget from the writer's perspective, follower count should not
be able to affect this number at all. Two plausible mechanisms, neither
confirmed by direct profiling this phase:
1. `notifyReplicationListeners` iterates the `CopyOnWriteArrayList` of
   registered listeners **synchronously, on the same thread that just
   wrote**, before `put()`/`delete()` returns (`ConcurrentLsmKeyValueStore.java`).
   With N followers attached, each PUT synchronously invokes N listener
   callbacks (`ReplicationServer`'s per-connection send) before the
   client's `put()` call unblocks — so while the *fsync* is never delayed
   by a follower, the *return of `put()`* still is, by however long it
   takes to hand the record off to N live socket writers on the calling
   thread. This is consistent with cost growing with follower count and
   would mean the design intent ("local write never blocked by
   replication") holds for the fsync/durability path but not for the full
   `put()` call latency as measured by this client-observed benchmark.
2. All followers, the leader, and the client run on the same one machine
   (see E12's disclosure above) — added follower processes add CPU and
   socket-handling contention on that shared machine, independent of any
   FORGE design choice.
Distinguishing these (and fixing mechanism 1, if confirmed, by making
`notifyReplicationListeners` genuinely async relative to `put()`'s return)
is flagged as a real, concrete, follow-up-worthy finding — not fixed this
phase, since Phase 12's mandate was measurement, not modification of
locked Phase 9 replication code without a specific justified need. See
PROGRESS.md's Phase 12 known limitations.

### §7.3 E14 — Recovery time vs. data size

Populate a store with N records, close it, then measure wall-clock time to
reopen it (full WAL replay, no snapshot) until it reports all N keys.

Mean across 2 repeats:

| Dataset size | Recovery time (s) |
|---:|---:|
| 200 | 0.0048 |
| 1,000 | 0.0083 |
| 3,000 | 0.0183 |

**Measured result**: recovery is fast and grows sub-linearly-looking but
consistently with data size across this range (200→3000 records, 15×
more data, only ~3.8× more time) — all three points are sub-20-millisecond,
i.e. WAL replay on close/reopen is not a practically significant startup
cost at these sizes. **Not extrapolated** beyond 3,000 records — no larger
size was measured.

### §7.4 E15 — Replica catch-up time vs. backlog size

Populate a leader with N records *before* a follower ever connects, then
attach a real `ReplicationFollower` and measure real wall-clock time until
it reaches full key-for-key parity with the leader (polling `keys()` —
see PROGRESS.md's Phase 12 bug fix below, which this experiment is what
originally triggered).

Mean across 2 repeats:

| Backlog size | Catch-up time (s) | Implied ops/sec |
|---:|---:|---:|
| 200 | 0.779 | ~257 |
| 1,000 | 3.830 | ~261 |
| 3,000 | 11.940 | ~251 |

**Derived metric, and it matches cleanly**: dividing backlog size by
catch-up time gives ~251–261 ops/sec at every backlog size — matching
Phase 6's and this phase's own §3/§7.1/§7.2 measured single-node WAL fsync
ceiling (~250–260 ops/sec) almost exactly. **Qualitative observation, well
evidenced**: this is consistent with the follower applying each replicated
record through the same `applyReplicated` → WAL-append → fsync path as any
local write, so catching up on a backlog is bottlenecked by the exact same
fsync ceiling as writing that much data locally would be — replication
catch-up isn't "free" or network-bound here, it's disk-fsync-bound on the
follower's side, at the same rate this project has measured for every
other fsync-bound path since Phase 6. This is the cleanest, most directly
explainable result in this phase's suite.

## §8 Phase 13 — Compaction read/write amplification (E16)

**Method**: a real `ConcurrentLsmKeyValueStore` (flush threshold 2,048
bytes, compaction deliberately disabled via a huge trigger count) is
populated by writing the same 50 keys, 100-byte values, **10 times each**
(500 total PUTs, 50 live keys at the end) — a workload designed to produce
many small SSTables full of immediately-stale, overwritten data, which is
exactly what compaction exists to reclaim. Because nothing consolidates
anything until compaction is explicitly triggered once, the on-disk total
measured just before that call is exactly the sum of every byte every
flush in this run physically wrote — a direct measurement, not an
estimate. Miss-lookup latency (2,000 GETs for keys guaranteed absent) is
measured client-side (in-process, `System.nanoTime()`) before and after
that same compaction call. Reproduce with:
```
mvn install -DskipTests
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.CompactionBenchmarkRunner
```
Raw data: `forge-bench/results/phase13-amplification-2026-09-04T16-57-00.165843Z.csv`.

| Metric | Before compaction | After compaction |
|---|---:|---:|
| SSTable count | 28 | 1 |
| On-disk data bytes | 66,172 | 6,574 |
| Bloom sidecar bytes | 1,456 | 92 |
| Miss-lookup avg latency | 0.017 ms | 0.005 ms |

Derived from the same run: logical bytes PUT (500 × (~5-byte key + 100-byte
value)) = 57,000; **write amplification (flush-to-disk) = 66,172 ÷ 57,000
≈ 1.16×**; **space reclaimed by compaction = 59,598 bytes (90.1% of the
pre-compaction footprint)**.

**Measured result, plainly stated**: compaction here reclaims essentially
all of the space overwrites had rendered dead — the post-compaction
footprint (6,574 bytes) is within a few hundred bytes of the theoretical
minimum for 50 live 100-byte values plus per-record framing overhead (entry
type, key/value length prefixes, CRC32 — see `SSTableFormat`), confirming
the merge is genuinely dropping every non-live version rather than merely
consolidating files without reclaiming anything. SSTable count drops from
28 to 1, and measured miss-lookup latency drops **3.4×** (0.017ms →
0.005ms) — a real, direct read-amplification measurement: a miss with 28
small tables to consult (even with a Bloom filter on each) is measurably
slower than a miss against one.

**Derived metric, and it's a clean, expected number**: 1.16× write
amplification at the flush layer is close to the theoretical floor —
FORGE's SSTable record format adds a fixed ~17 bytes of framing per record
(length prefix, entry type, key/value length fields, CRC32; see
`SSTableFormat`'s layout) on top of each ~105-byte key+value pair, and nothing
else duplicates data on the way from a PUT to a flushed SSTable (no WAL
bytes are double-counted here — this metric is deliberately scoped to
flush-to-SSTable amplification, not WAL-plus-SSTable). A ratio this close
to 1.0 is exactly what that framing overhead alone predicts, with no
unexplained inflation.

**What this is not**: a claim about behavior at a scale beyond what was
measured — 50 keys and 500 operations is small by design, to keep the
whole run fast and reproducible. The *pattern* (compaction reclaims
overwritten space, reduces file count, and measurably speeds up misses) is
real and directly measured; the specific byte counts and the 3.4× latency
figure are particular to this workload's size and would not be quoted as
production numbers at a different scale.

## §9 Phase 15 — Failover timing (E17)

**Method**: a real 3-node cluster (`RaftCluster` + `ForgeServer` +
`PartitionLeadership`, the exact same stack `StaleLeaderFencingIntegrationTest`/
`FailoverReplicationIntegrationTest` use for correctness) is built with
node A given a short election timeout (`[50, 80]ms`) so it deterministically
wins the first election, and B/C given a longer one (`[150, 300]ms`).
A confirms leadership, accepts one warm-up write, and is then closed
outright (`ForgeServer.close()` + `RaftCluster.close()` — a clean crash,
not a partition). Three `System.nanoTime()`-timed instants are recorded
from the moment of the crash: when a survivor becomes a confirmed leader
("election time"), when that leader's own `PartitionLeadership.canAcceptWrites()`
first returns true ("leadership transition time" — in this implementation
these two are nearly identical, since confirmation already requires a
majority ack, see below), and when a real client `PUT` through the new
leader actually completes ("time to first successful write"). 5 repeats.
Reproduce with:
```
mvn install -DskipTests
mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.FailoverBenchmarkRunner
```
Raw data: `forge-bench/results/phase15-failover-2026-09-05T05-52-50.890086Z.csv`.

| Run | Election time (s) | Leadership transition (s) | Time to first write (s) |
|---:|---:|---:|---:|
| 1 | 0.249 | 0.251 | 0.257 |
| 2 | 0.206 | 0.210 | 0.215 |
| 3 | 0.515 | 0.517 | 0.524 |
| 4 | 0.484 | 0.486 | 0.489 |
| 5 | 0.276 | 0.278 | 0.283 |
| **mean** | **0.346** | **0.348** | **0.354** |

**Measured result, plainly stated**: election time ranges ~0.21-0.52s
across 5 runs, averaging ~0.35s — consistent with, and bounded by, the
survivors' own configured randomized election timeout window
(`[150, 300]ms`) plus the time for the winning RequestVote round-trip and
a majority AppendEntries ack to complete. The spread run-to-run is exactly
what a *randomized* election timeout should produce (§14's `RaftNode`
Javadoc on why randomization exists) — it is not noise to be explained
away, it's the mechanism working as designed.

**Derived metric**: leadership transition time is only ~2-4ms after
election time, and time-to-first-write only ~5-7ms after that — both
small increments on top of the dominant election-time cost, matching
expectations: becoming "confirmed" only needs the no-op's majority ack
(which typically arrives alongside or immediately after the vote that won
the election), and a `PUT`'s own added cost is just one WAL fsync
(~4ms, §2) plus network round trips.

**What this does *not* measure, stated explicitly** (per this project's
"failure detection time" framing): this system has **no separate
failure-detection step** feeding into Raft failover — Raft's own election
timeout is the only mechanism that notices a leader is gone. Phase 8's
`FailureDetector` exists but is not wired into this decision. So there is
only one number here (election time), not two — reporting an additional
"detection time" would imply a second mechanism that does not exist in
this codebase.

**What this is not**: a production SLA or a claim about behavior under
real workload/network conditions — this is 3 processes on one machine,
loopback networking, no concurrent client load during the failover window.
The pattern (failover completes within roughly one configured election
timeout window, and the data plane resumes serving writes within
single-digit milliseconds of the control plane confirming a leader) is
real and measured; the specific numbers would differ on a real multi-machine
deployment or under load.
