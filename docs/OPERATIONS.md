# FORGE — Operations

How to inspect a FORGE node's state from the outside. Stated plainly up
front: this is a **lightweight, storage-engine-scoped** observability
surface, not a full metrics/admin HTTP endpoint. What exists is real and
useful; what doesn't exist is named explicitly rather than implied.

## 1. What you can inspect today

### Storage-engine status (offline CLI)

```bash
mvn -pl forge-server -am install -DskipTests
mvn -pl forge-server exec:java -Dexec.args="status <dataDirectory>"
```

Prints:

| Field | Meaning |
|---|---|
| `sstable count` | Number of on-disk SSTable files this store currently has |
| `sstable bytes (on disk)` | Sum of every SSTable file's size — the same number BENCHMARKS.md §8 uses for write-amplification/compaction-reclaim measurements |
| `active MemTable bytes` | Approximate size of the in-memory tier not yet flushed |
| `flush in progress` | Whether a MemTable is currently mid-flush (frozen, draining to disk) |
| `next WAL sequence number` | One past the last durably applied record — what a replication follower would need to catch up from |

**This is an offline tool**: it opens the store directory directly (the
same recovery path a normal server startup uses), so it must not be run
against a directory a live `ForgeServer` process already has open — two
processes cannot safely share one WAL/SSTable directory. Stop the server
first, or point it at a different node's directory in a multi-node setup.

Backing implementation: `ConcurrentLsmKeyValueStore.status()` /
`ConcurrentLsmKeyValueStore.StoreStatus` (`forge-storage`).

### Everything else — real, but Java-API-only, not network-queryable

Every one of these exists as a real accessor on a live, in-process Java
object — genuinely computed, not stubbed — but none is currently exposed
over the network for external/remote inspection of a running server:

| What | Accessor | Where |
|---|---|---|
| Cluster membership view | `FailureDetector.snapshot()` | `forge-cluster` (Phase 8) |
| Partition ownership | `ClusterTopology`/`ConsistentHashRing.ownerOf(key)` | `forge-cluster` (Phase 7) |
| Raft role/term/leader/fencing | `RaftCluster.role()` / `currentTerm()` / `currentLeader()` / `isConfirmedLeader()` / `canServeAuthoritatively()` | `forge-cluster` (Phase 14/15) |
| Write-fencing status for a partition | `PartitionLeadership.canAcceptWrites()` | `forge-cluster` (Phase 15) |
| Whether a node needs a full resync on rejoin | `ReplicationFollowerCoordinator.needsFullResync()` | `forge-cluster` (Phase 15) |
| Replication lag | `ReplicaState` (tracked internally by `ReplicationServer`) | `forge-cluster` (Phase 9) |
| Throughput/latency | `forge-bench`'s `BenchmarkResult`/`LatencyStats` | `forge-bench` (Phase 6/12) — measured by *driving load*, not queried from a live server |

## 2. What's honestly not built

- **No HTTP/network admin endpoint.** Everything above that says
  "Java-API-only" would need one to be remotely queryable. A small
  additive protocol message (following Phase 7's precedent for adding
  `ERROR_NOT_OWNER` to the existing sealed wire protocol) is the natural
  way to add this — not attempted this phase given the time already spent
  on Phases 7-14's core mechanisms.
- **No dashboard.** The master plan's "optional final UI" was explicitly
  gated on the core being complete and not delaying it; with the core
  phases (7-14) landing exactly on schedule and this being the very next
  item, a UI was deprioritized in favor of the required final
  documentation, demo, and report — see PROGRESS.md.
- **No aggregated cluster-wide view.** Each of the table above is
  per-node/per-object; nothing collects them into one cluster-level
  picture (e.g., "show me every node's replication lag at once").

## 3. Practical inspection during the demo (docs/DEMO.md)

Until a network admin endpoint exists, the practical way to observe a
running multi-node FORGE cluster's behavior is:
1. **Logs** — every component (`ForgeServer`, `ReplicationServer`/
   `ReplicationFollower`, `FailureDetector`/`HeartbeatService`, `RaftCluster`)
   logs role transitions, connections, and elections via SLF4J/Logback at
   INFO level. Running each node with its own visible console is the
   most direct way to watch a demo unfold in real time — post-Phase-15
   audit addition: `com.forge.cluster.launcher.ClusterNodeMain` (docs/DEMO.md
   §13) is a real CLI for actually doing this with genuinely separate
   processes, not just test code.
2. **The `status` subcommand** above, run against a stopped node's
   directory, for storage-engine-level inspection.
3. **`forge-bench`'s runners** for throughput/latency, which measure by
   generating real load against a running cluster rather than querying
   it passively.
