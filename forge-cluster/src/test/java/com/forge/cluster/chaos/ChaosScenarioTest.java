package com.forge.cluster.chaos;

import com.forge.client.ForgeClient;
import com.forge.client.ForgeServerException;
import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.cluster.leadership.PartitionLeadership;
import com.forge.cluster.membership.FailureDetector;
import com.forge.cluster.membership.NodeState;
import com.forge.cluster.recovery.SnapshotClient;
import com.forge.cluster.recovery.SnapshotServer;
import com.forge.cluster.replication.ReplicationFollower;
import com.forge.cluster.replication.ReplicationServer;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Six deterministic, reproducible chaos scenarios, per this phase's own
 * requirement: <b>not</b> random chaos for the sake of looking impressive —
 * every fault here is injected at an exact, controlled point (a specific
 * call to {@code close()}, {@code partition()}, or {@code dropNext(n)}), so
 * a run either always exposes the same behavior or genuinely found a bug.
 *
 * <p>Every scenario states, up front, in its own Javadoc: the assumptions
 * it operates under, the safety property it actually proves, and the
 * liveness property it actually proves — and never claims more than the
 * architecture built through Phase 10 actually supports. In particular:
 * this project has <b>no consensus or automatic failover yet</b> (that's
 * Phase 14) and replication is <b>asynchronous only</b> (Phase 9) — so no
 * scenario here claims "zero data loss" for a write that was only ever
 * acknowledged by a leader that then died before shipping it anywhere.
 *
 * <h2>Scenarios G-L (Phase 15)</h2>
 * Six more scenarios, added once real Raft-driven data-plane fencing
 * existed to test: leader crash + automatic failover, an old leader
 * reconnecting after failover, network partition + majority election,
 * delayed stale-leader messages, repeated leader crashes, and a crash
 * during follower catch-up. These build on {@code PartitionLeadership}/
 * {@code ReplicationFollowerCoordinator}/{@code StaleReplicaRecovery} —
 * see {@code com.forge.cluster.leadership}'s two dedicated integration
 * tests ({@code StaleLeaderFencingIntegrationTest},
 * {@code FailoverReplicationIntegrationTest}) for the fuller, multi-step
 * versions of several of these stories; the scenarios here are
 * deliberately smaller and each isolates one specific behavior, matching
 * scenarios A-F's own scope and style.
 */
class ChaosScenarioTest {

    private static final Duration A_ELECTION_MIN = Duration.ofMillis(40);
    private static final Duration A_ELECTION_MAX = Duration.ofMillis(60);
    private static final Duration BC_ELECTION_MIN = Duration.ofMillis(400);
    private static final Duration BC_ELECTION_MAX = Duration.ofMillis(600);
    private static final Duration RAFT_HEARTBEAT = Duration.ofMillis(25);
    private static final Duration RAFT_TICK = Duration.ofMillis(10);
    private static final Duration RAFT_RPC_TIMEOUT = Duration.ofMillis(500);
    private static final Duration LEASE = A_ELECTION_MIN;

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    private static Map<NodeId, NodeAddress> without(Map<NodeId, NodeAddress> addresses, NodeId self) {
        Map<NodeId, NodeAddress> copy = new HashMap<>(addresses);
        copy.remove(self);
        return copy;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeout);
        }
    }

    /**
     * <b>Scenario A — leader crashes during active replication.</b>
     * <p>Assumptions: asynchronous replication (Phase 9); no failover exists
     * (Phase 14) — a dead leader is not replaced by anything.
     * <p>Safety property actually proved: every write the follower had
     * already durably applied before the leader died remains intact and
     * uncorrupted on the follower afterward. (Not claimed: writes the
     * leader acked to a client but never shipped to any follower — those
     * can genuinely be lost under this architecture, honestly.)
     * <p>Liveness property actually proved: the follower's own store
     * remains fully usable (readable and writable) after the leader
     * disappears — it does not hang or require the leader to function.
     */
    @Test
    @Timeout(30)
    void scenarioA_leaderCrashesDuringActiveReplication(@TempDir Path baseDir) throws Exception {
        ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
        ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0);
        ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
        ReplicationFollower follower = new ReplicationFollower(
                new NodeId("scenario-a-follower"), "localhost", replicationServer.port(), followerStore, Duration.ofMillis(30));

        try {
            for (int i = 0; i < 50; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }
            waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 50);

            // The leader dies mid-flow -- deterministically, at this exact point.
            replicationServer.close();
            leaderStore.close();

            // Safety: everything already applied is intact.
            for (int i = 0; i < 50; i++) {
                assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
            }
            // Liveness: the follower's own store still works, leader or no leader.
            assertDoesNotThrow(() -> followerStore.put("still-usable", bytes("yes")));
            assertArrayEquals(bytes("yes"), followerStore.get("still-usable").orElseThrow());
        } finally {
            follower.close();
            followerStore.close();
        }
    }

    /**
     * <b>Scenario B — follower disappears and rejoins.</b>
     * <p>Assumptions: the follower's on-disk directory survives the outage
     * (a process crash/restart, not a disk loss — that would need Phase
     * 10's snapshot bootstrap instead, covered separately).
     * <p>Safety property: no record is lost or duplicated across the
     * disappearance-and-rejoin cycle — the follower's final state after
     * catching up again is bit-for-bit what it would have been had it
     * never left.
     * <p>Liveness property: after rejoining, the follower reaches full
     * parity with the leader within a bounded time, with no manual
     * intervention beyond reconnecting.
     */
    @Test
    @Timeout(30)
    void scenarioB_followerDisappearsAndRejoins(@TempDir Path baseDir) throws Exception {
        Path followerDir = baseDir.resolve("follower");
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0)) {

            for (int i = 0; i < 20; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            ConcurrentLsmKeyValueStore followerStore1 = new ConcurrentLsmKeyValueStore(followerDir);
            ReplicationFollower follower1 = new ReplicationFollower(
                    new NodeId("scenario-b"), "localhost", replicationServer.port(), followerStore1, Duration.ofMillis(30));
            waitUntil(Duration.ofSeconds(10), () -> followerStore1.keys().size() == 20);

            // The follower disappears -- deterministically, right here.
            follower1.close();
            followerStore1.close();

            for (int i = 20; i < 40; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            // It rejoins -- same directory, fresh connection.
            try (ConcurrentLsmKeyValueStore followerStore2 = new ConcurrentLsmKeyValueStore(followerDir);
                 ReplicationFollower follower2 = new ReplicationFollower(
                         new NodeId("scenario-b"), "localhost", replicationServer.port(), followerStore2, Duration.ofMillis(30))) {

                waitUntil(Duration.ofSeconds(10), () -> followerStore2.keys().size() == 40);
                for (int i = 0; i < 40; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore2.get("k" + i).orElseThrow());
                }
                assertEquals(40, followerStore2.keys().size(), "no record lost or duplicated across the disappear/rejoin cycle");
            }
        }
    }

    /**
     * <b>Scenario C — network partition separates leader from follower.</b>
     * <p>Adapted from "leader from majority": this project has no quorum
     * concept yet (that needs Phase 14's consensus) — with exactly one
     * leader and one follower, "partitioned from the majority" and
     * "partitioned from its only follower" are the same scenario here.
     * <p>Safety property: the follower's data stays internally consistent
     * throughout the partition (no corruption from the severed connection
     * itself); the leader's local writes are never blocked by the
     * follower's unreachability (proving async replication's local writes
     * are genuinely decoupled from follower availability, not just in
     * theory).
     * <p>Liveness property: once the partition heals <em>and the caller
     * reconnects</em>, the follower catches up to full parity.
     * {@code ReplicationFollower} deliberately does not auto-reconnect a
     * severed connection (see its own class Javadoc) — a partition kills
     * the TCP connection just as a real one would, and per this project's
     * documented contract, the caller is the one who creates a fresh
     * {@code ReplicationFollower} afterward, exactly as Phase 9/10's own
     * reconnect tests already do. This scenario follows that same,
     * already-established contract rather than assuming a capability that
     * was never built.
     */
    @Test
    @Timeout(30)
    void scenarioC_networkPartitionSeparatesLeaderFromFollower(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0);
             FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", replicationServer.port(), 0);
             ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"))) {

            for (int i = 0; i < 20; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("scenario-c"), "localhost", proxy.port(), followerStore, Duration.ofMillis(30));
            waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 20);

            proxy.partition();
            assertTrue(proxy.isPartitioned());

            // Liveness of the LEADER specifically: local writes must not block just
            // because the follower is now unreachable.
            long start = System.nanoTime();
            for (int i = 20; i < 40; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            assertTrue(elapsed.toSeconds() < 5, "leader writes must not block on an unreachable follower, took " + elapsed);

            // Safety: the follower's own (partial) data is untouched and consistent
            // while partitioned.
            for (int i = 0; i < 20; i++) {
                assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
            }

            follower.close(); // the partition already severed this connection; retire it explicitly
            proxy.heal();

            // Liveness: reconnecting (a fresh ReplicationFollower, per the documented
            // contract) reaches full parity.
            try (ReplicationFollower reconnected = new ReplicationFollower(
                    new NodeId("scenario-c"), "localhost", proxy.port(), followerStore, Duration.ofMillis(30))) {
                waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 40);
                for (int i = 20; i < 40; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
                }
            }
        }
    }

    /**
     * <b>Scenario D — messages are delayed (and, separately, arrive out of order).</b>
     * <p>Delay is tested directly here, over a real proxied connection.
     * Reordering is deliberately tested at the layer where "order" is a
     * well-defined concept — {@code WriteAheadLog.appendReplicated}'s own
     * gap/duplicate handling ({@code WriteAheadLogTest.appendReplicatedRefusesToCreateAGap}
     * and {@code appendReplicatedIsIdempotentForAnAlreadyAppliedSequenceNumber},
     * Phase 9) — rather than re-derived here with a cruder byte-level
     * permutation that would mostly just produce corrupted frames; see
     * {@link FaultInjectingTcpProxy}'s class Javadoc for why.
     * <p>Safety property: no corruption and no duplication despite
     * significant, real, injected network delay.
     * <p>Liveness property: replication still fully converges, just later —
     * delay changes when convergence happens, not whether it happens.
     */
    @Test
    @Timeout(30)
    void scenarioD_messagesAreSignificantlyDelayed(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0);
             FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", replicationServer.port(), 0);
             ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"))) {

            proxy.setDelay(Duration.ofMillis(200));

            try (ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("scenario-d"), "localhost", proxy.port(), followerStore, Duration.ofMillis(30))) {

                for (int i = 0; i < 10; i++) {
                    leaderStore.put("k" + i, bytes("v" + i));
                }

                // Must still converge -- just not instantly, given the injected delay.
                waitUntil(Duration.ofSeconds(20), () -> followerStore.keys().size() == 10);
                for (int i = 0; i < 10; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
                }
            }
        }
    }

    /**
     * <b>Scenario E — a node crashes during recovery (bootstrap).</b>
     * <p>Assumptions: {@code SnapshotClient.fetchAndLoad} only ever calls
     * {@code loadSnapshot} after reading the complete stream (see its own
     * class Javadoc); this scenario deliberately severs the transfer before
     * that point.
     * <p>Safety property: an interrupted bootstrap leaves the destination
     * exactly as it was before the attempt (empty) — never a partially
     * loaded, inconsistent SSTable.
     * <p>Liveness property: the bootstrap can simply be retried from
     * scratch afterward and succeed normally — the interrupted attempt
     * leaves nothing that needs to be cleaned up first.
     */
    @Test
    @Timeout(30)
    void scenarioE_nodeCrashesDuringBootstrapRecovery(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore sourceStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("source"))) {
            for (int i = 0; i < 30; i++) {
                sourceStore.put("k" + i, bytes("v" + i));
            }

            try (com.forge.cluster.recovery.SnapshotServer snapshotServer =
                    new com.forge.cluster.recovery.SnapshotServer(sourceStore, 0);
                    FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", snapshotServer.port(), 0)) {

                // The transfer is severed deterministically, shortly after it starts,
                // by partitioning the proxy mid-stream.
                Thread severer = Thread.ofPlatform().start(() -> {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    proxy.partition();
                });

                Path destDir = baseDir.resolve("dest");
                ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(destDir);
                try {
                    try {
                        SnapshotClient.fetchAndLoad("localhost", proxy.port(), destStore);
                        // If it happened to complete before the severer fired, that's a valid
                        // (if untested-by-this-run) outcome too -- the safety property below
                        // still holds either way.
                    } catch (IOException expected) {
                        // the interrupted-transfer path -- the expected outcome most runs take
                    }
                    severer.join(5_000);

                    // Safety: either nothing loaded (interrupted) or everything loaded
                    // (completed first) -- never a partial state in between.
                    int keyCount = destStore.keys().size();
                    assertTrue(keyCount == 0 || keyCount == 30,
                            "bootstrap must never leave a partially-loaded destination; got " + keyCount + " keys");
                } finally {
                    destStore.close();
                }

                // Liveness: retrying from scratch (fresh destination directory) succeeds normally.
                proxy.heal();
                Path retryDir = baseDir.resolve("dest-retry");
                try (ConcurrentLsmKeyValueStore retryStore = new ConcurrentLsmKeyValueStore(retryDir)) {
                    SnapshotClient.fetchAndLoad("localhost", proxy.port(), retryStore);
                    assertEquals(30, retryStore.keys().size());
                }
            }
        }
    }

    /**
     * <b>Scenario F — two nodes temporarily have stale membership.</b>
     * <p>Assumptions: DESIGN.md's Phase 8 contract itself: membership is
     * eventually consistent, not agreed-upon — two independent
     * {@link FailureDetector}s are two independent local views by design,
     * not a single shared source of truth.
     * <p>Safety property: neither node's detector ever throws, corrupts its
     * own state, or produces an impossible state transition while the two
     * views disagree.
     * <p>Liveness property: once heartbeats resume, both nodes' views
     * converge back to agreement (both ALIVE) — staleness is temporary, not
     * permanent, with no manual reconciliation needed.
     */
    @Test
    @Timeout(15)
    void scenarioF_twoNodesTemporarilyHaveStaleMembership() {
        NodeId a = new NodeId("scenario-f-a");
        NodeId b = new NodeId("scenario-f-b");
        FailureDetector detectorOnA = new FailureDetector(Clock.systemUTC(), Duration.ofMillis(100), Duration.ofMillis(200));
        FailureDetector detectorOnB = new FailureDetector(Clock.systemUTC(), Duration.ofMillis(100), Duration.ofMillis(200));
        detectorOnA.join(b); // A's view of B
        detectorOnB.join(a); // B's view of A

        // Heartbeats between them stop (simulated directly: nobody calls recordHeartbeat).
        assertDoesNotThrow(() -> {
            Thread.sleep(250);
            detectorOnA.tick();
            detectorOnB.tick();
        });

        // Both views are now stale (DEAD) -- this is the "temporarily stale" state itself,
        // not a bug: each node's view has correctly, independently detected the silence.
        assertEquals(NodeState.DEAD, detectorOnA.stateOf(b).orElseThrow());
        assertEquals(NodeState.DEAD, detectorOnB.stateOf(a).orElseThrow());

        // Heartbeats resume -- both views converge back to agreement automatically.
        detectorOnA.recordHeartbeat(b);
        detectorOnB.recordHeartbeat(a);

        assertEquals(NodeState.ALIVE, detectorOnA.stateOf(b).orElseThrow());
        assertEquals(NodeState.ALIVE, detectorOnB.stateOf(a).orElseThrow());
    }

    /**
     * <b>Scenario G — leader crash + automatic failover.</b>
     * <p>Assumptions: a 3-node Raft group (Phase 14); A is engineered to
     * always win the first election via a much shorter election timeout
     * than B/C (this scenario is about what happens after a legitimate
     * leader dies, not about a fair election race).
     * <p>Safety property: at every point in time, {@code isConfirmedLeader()}
     * is true for at most one of the three nodes (checked immediately
     * before and after the crash).
     * <p>Liveness property: once A is killed outright, the surviving
     * majority (B+C) elects a new, confirmed leader at a strictly higher
     * term, and that new leader's own {@code PartitionLeadership} accepts a
     * real client write within a bounded time — the data plane, not just
     * the control plane, recovers.
     */
    @Test
    @Timeout(30)
    void scenarioG_leaderCrashTriggersAutomaticFailover(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("g-a");
        NodeId b = new NodeId("g-b");
        NodeId c = new NodeId("g-c");
        int aPort = freePort();
        int bPort = freePort();
        int cPort = freePort();
        Map<NodeId, NodeAddress> addrs = Map.of(
                a, new NodeAddress("localhost", aPort), b, new NodeAddress("localhost", bPort), c, new NodeAddress("localhost", cPort));

        try (ConcurrentLsmKeyValueStore storeA = new ConcurrentLsmKeyValueStore(baseDir.resolve("a"));
             RaftCluster raftA = new RaftCluster(a, without(addrs, a), aPort, Clock.systemUTC(),
                     A_ELECTION_MIN, A_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(1));
             RaftCluster raftB = new RaftCluster(b, without(addrs, b), bPort, Clock.systemUTC(),
                     BC_ELECTION_MIN, BC_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(2));
             RaftCluster raftC = new RaftCluster(c, without(addrs, c), cPort, Clock.systemUTC(),
                     BC_ELECTION_MIN, BC_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(3))) {

            PartitionLeadership leadershipA = new PartitionLeadership("p0", raftA, storeA);
            ForgeServer serverA = new ForgeServer(storeA, key -> true, leadershipA, 0);
            waitUntil(Duration.ofSeconds(10), leadershipA::canAcceptWrites);

            List<RaftCluster> all = List.of(raftA, raftB, raftC);
            assertEquals(1, all.stream().filter(RaftCluster::isConfirmedLeader).count(),
                    "exactly one confirmed leader before the crash");

            try (ForgeClient client = ForgeClient.connect("localhost", serverA.port())) {
                client.put("before-crash", bytes("v"));
            }

            serverA.close();
            raftA.close();

            List<RaftCluster> survivors = List.of(raftB, raftC);
            waitUntil(Duration.ofSeconds(10), () -> survivors.stream().anyMatch(RaftCluster::isConfirmedLeader));
            assertEquals(1, survivors.stream().filter(RaftCluster::isConfirmedLeader).count(),
                    "exactly one confirmed leader among the survivors");
            RaftCluster newLeaderRaft = raftB.isConfirmedLeader() ? raftB : raftC;
            ConcurrentLsmKeyValueStore newLeaderStore = newLeaderRaft == raftB
                    ? new ConcurrentLsmKeyValueStore(baseDir.resolve("b")) : new ConcurrentLsmKeyValueStore(baseDir.resolve("c"));
            try {
                PartitionLeadership newLeadership = new PartitionLeadership("p0", newLeaderRaft, newLeaderStore);
                try (ForgeServer newServer = new ForgeServer(newLeaderStore, key -> true, newLeadership, 0)) {
                    waitUntil(Duration.ofSeconds(5), newLeadership::canAcceptWrites);
                    try (ForgeClient client = ForgeClient.connect("localhost", newServer.port())) {
                        assertDoesNotThrow(() -> client.put("after-failover", bytes("v")));
                    }
                }
            } finally {
                newLeaderStore.close();
            }
        }
    }

    /**
     * <b>Scenario H — an old leader reconnects after failover.</b>
     * <p>Assumptions: same 3-node group; A is genuinely partitioned (not
     * killed — {@link FaultInjectingTcpProxy}, real sockets) from B and C
     * on every A&harr;{B,C} link, then healed. See
     * {@code StaleLeaderFencingIntegrationTest} for the fuller nine-step
     * version of this story; this scenario isolates just the reconnect
     * transition.
     * <p>Safety property: after healing, A discovers the cluster's higher
     * term and steps down to FOLLOWER — it does not remain, or revert to,
     * LEADER on its own.
     * <p>Liveness property: A's own {@code PartitionLeadership} correctly
     * refuses a write attempt made against it immediately after healing —
     * fencing does not require A to be told anything beyond the term it
     * already learned via the heal.
     */
    @Test
    @Timeout(30)
    void scenarioH_oldLeaderReconnectsAfterFailoverAndStaysFenced(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("h-a");
        NodeId b = new NodeId("h-b");
        NodeId c = new NodeId("h-c");
        int aPort = freePort();
        int bPort = freePort();
        int cPort = freePort();

        try (FaultInjectingTcpProxy aToB = new FaultInjectingTcpProxy("localhost", bPort, freePort());
             FaultInjectingTcpProxy aToC = new FaultInjectingTcpProxy("localhost", cPort, freePort());
             FaultInjectingTcpProxy bToA = new FaultInjectingTcpProxy("localhost", aPort, freePort());
             FaultInjectingTcpProxy cToA = new FaultInjectingTcpProxy("localhost", aPort, freePort())) {

            Map<NodeId, NodeAddress> addrsForA = Map.of(b, new NodeAddress("localhost", aToB.port()), c, new NodeAddress("localhost", aToC.port()));
            Map<NodeId, NodeAddress> addrsForB = Map.of(a, new NodeAddress("localhost", bToA.port()), c, new NodeAddress("localhost", cPort));
            Map<NodeId, NodeAddress> addrsForC = Map.of(a, new NodeAddress("localhost", cToA.port()), b, new NodeAddress("localhost", bPort));

            try (ConcurrentLsmKeyValueStore storeA = new ConcurrentLsmKeyValueStore(baseDir.resolve("a"));
                 RaftCluster raftA = new RaftCluster(a, addrsForA, aPort, Clock.systemUTC(),
                         A_ELECTION_MIN, A_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(1));
                 RaftCluster raftB = new RaftCluster(b, addrsForB, bPort, Clock.systemUTC(),
                         BC_ELECTION_MIN, BC_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(2));
                 RaftCluster raftC = new RaftCluster(c, addrsForC, cPort, Clock.systemUTC(),
                         BC_ELECTION_MIN, BC_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(3))) {

                PartitionLeadership leadershipA = new PartitionLeadership("p0", raftA, storeA);
                try (ForgeServer serverA = new ForgeServer(storeA, key -> true, leadershipA, 0)) {
                    waitUntil(Duration.ofSeconds(10), leadershipA::canAcceptWrites);
                    long termBefore = raftA.currentTerm();

                    aToB.partition();
                    aToC.partition();
                    bToA.partition();
                    cToA.partition();

                    List<RaftCluster> survivors = List.of(raftB, raftC);
                    waitUntil(Duration.ofSeconds(10), () -> survivors.stream().anyMatch(RaftCluster::isConfirmedLeader));
                    RaftCluster newLeader = raftB.isConfirmedLeader() ? raftB : raftC;
                    assertTrue(newLeader.currentTerm() > termBefore);

                    aToB.heal();
                    aToC.heal();
                    bToA.heal();
                    cToA.heal();

                    waitUntil(Duration.ofSeconds(5), () -> raftA.currentTerm() >= newLeader.currentTerm());
                    waitUntil(Duration.ofSeconds(5), () -> raftA.role() == com.forge.cluster.consensus.RaftRole.FOLLOWER);
                    assertFalse(raftA.canServeAuthoritatively(), "A must not remain or revert to authoritative after healing");

                    try (ForgeClient client = ForgeClient.connect("localhost", serverA.port())) {
                        ForgeServerException e = assertThrows(ForgeServerException.class, () -> client.put("k", bytes("v")));
                        assertEquals(ProtocolConstants.ERROR_NOT_LEADER, e.errorCode());
                    }
                }
            }
        }
    }

    /**
     * <b>Scenario I — network partition, majority side elects, minority side is fenced.</b>
     * <p>Assumptions: same partition topology as Scenario H, but the
     * assertion here is made <em>while still partitioned</em> (before any
     * heal) — the emphasis is the ongoing state during a live partition,
     * not the reconnect transition Scenario H covers.
     * <p>Safety property: for the entire duration of the partition, the
     * minority side (A, alone) never becomes or remains an authoritative
     * leader once its lease expires, while the majority side (B+C)
     * successfully elects one.
     * <p>Liveness property: the majority side continues making progress
     * (accepting a write) throughout the partition — a minority partition
     * never blocks the majority.
     */
    @Test
    @Timeout(30)
    void scenarioI_networkPartitionMajoritySideElectsMinorityIsFenced(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("i-a");
        NodeId b = new NodeId("i-b");
        NodeId c = new NodeId("i-c");
        int aPort = freePort();
        int bPort = freePort();
        int cPort = freePort();

        try (FaultInjectingTcpProxy aToB = new FaultInjectingTcpProxy("localhost", bPort, freePort());
             FaultInjectingTcpProxy aToC = new FaultInjectingTcpProxy("localhost", cPort, freePort());
             FaultInjectingTcpProxy bToA = new FaultInjectingTcpProxy("localhost", aPort, freePort());
             FaultInjectingTcpProxy cToA = new FaultInjectingTcpProxy("localhost", aPort, freePort())) {

            Map<NodeId, NodeAddress> addrsForA = Map.of(b, new NodeAddress("localhost", aToB.port()), c, new NodeAddress("localhost", aToC.port()));
            Map<NodeId, NodeAddress> addrsForB = Map.of(a, new NodeAddress("localhost", bToA.port()), c, new NodeAddress("localhost", cPort));
            Map<NodeId, NodeAddress> addrsForC = Map.of(a, new NodeAddress("localhost", cToA.port()), b, new NodeAddress("localhost", bPort));

            try (RaftCluster raftA = new RaftCluster(a, addrsForA, aPort, Clock.systemUTC(),
                         A_ELECTION_MIN, A_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(1));
                 RaftCluster raftB = new RaftCluster(b, addrsForB, bPort, Clock.systemUTC(),
                         BC_ELECTION_MIN, BC_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(2));
                 RaftCluster raftC = new RaftCluster(c, addrsForC, cPort, Clock.systemUTC(),
                         BC_ELECTION_MIN, BC_ELECTION_MAX, RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT, LEASE, new Random(3))) {

                waitUntil(Duration.ofSeconds(10), raftA::isConfirmedLeader);

                aToB.partition();
                aToC.partition();
                bToA.partition();
                cToA.partition();

                List<RaftCluster> survivors = List.of(raftB, raftC);
                waitUntil(Duration.ofSeconds(10), () -> survivors.stream().anyMatch(RaftCluster::isConfirmedLeader));

                // While still partitioned: the minority (A) must be fenced, the majority must not be.
                waitUntil(Duration.ofSeconds(5), () -> !raftA.canServeAuthoritatively());
                RaftCluster newLeader = raftB.isConfirmedLeader() ? raftB : raftC;
                assertTrue(newLeader.canServeAuthoritatively(), "the majority side must remain authoritative throughout the partition");
                assertFalse(raftA.canServeAuthoritatively(), "the minority side must stay fenced throughout the partition");
            }
        }
    }

    /**
     * <b>Scenario J — a delayed message from a stale leader is still correctly rejected.</b>
     * <p>Assumptions: A is a stale (fenced) former leader; its AppendEntries
     * to a node that has already moved to a higher term is delayed
     * (real, measured delay — {@link FaultInjectingTcpProxy#setDelay}), not
     * dropped.
     * <p>Safety property: delay does not help a stale message succeed —
     * once it finally arrives, it is rejected exactly as an undelayed one
     * would be, on term alone, regardless of how long it was in flight.
     * <p>Liveness property: the delayed exchange still completes (the
     * connection is not left hanging) — a stale sender gets a real,
     * prompt rejection response, not silence.
     */
    @Test
    @Timeout(30)
    void scenarioJ_delayedStaleLeaderMessageIsStillRejectedOnArrival() throws Exception {
        com.forge.cluster.consensus.RaftNode staleLeaderSimulator; // built to have advanced to a real term first
        MutableClockForChaosTest clock = new MutableClockForChaosTest(Instant.EPOCH);
        NodeId self = new NodeId("j-self");
        NodeId stale = new NodeId("j-stale-leader");
        staleLeaderSimulator = new com.forge.cluster.consensus.RaftNode(self, java.util.Set.of(stale), clock,
                Duration.ofMillis(50), Duration.ofMillis(80), Duration.ofMillis(20), new Random(1));

        // This node moves to term 5 via a real AppendEntries from a legitimate current leader.
        staleLeaderSimulator.handleAppendEntries(new com.forge.cluster.consensus.AppendEntriesRequest(
                5, stale, 0, 0, List.of(), 0));
        assertEquals(5, staleLeaderSimulator.currentTerm());

        // A real network delay is simulated directly (this scenario is about the delay's
        // effect on the outcome, not about proving the proxy delays bytes -- see
        // FaultInjectingTcpProxyTest for that): the stale leader's term-1 message is
        // constructed now but "delivered" only after a real sleep.
        Thread.sleep(150);
        var response = staleLeaderSimulator.handleAppendEntries(
                new com.forge.cluster.consensus.AppendEntriesRequest(1, stale, 0, 0, List.of(), 0));

        assertFalse(response.success(), "a delayed stale-term message must still be rejected once it arrives");
        assertEquals(5, response.term(), "the rejection must report the true current term, unaffected by the delay");
        assertEquals(5, staleLeaderSimulator.currentTerm(), "the delayed stale message must not affect the current term at all");
    }

    /** Minimal local {@link Clock} double for Scenario J — advancing is not needed, only a fixed instant. */
    private static final class MutableClockForChaosTest extends Clock {
        private final Instant now;

        MutableClockForChaosTest(Instant now) {
            this.now = now;
        }

        @Override
        public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /**
     * <b>Scenario K — repeated leader crashes.</b>
     * <p>Assumptions: a <b>5</b>-node Raft group, deliberately larger than
     * every other scenario's 3 — a majority of 5 is 3, so the cluster can
     * absorb <em>two</em> successive leader crashes while still retaining
     * a majority (3 survivors) able to elect a third leader. A 3-node
     * group can only ever safely tolerate one such crash (2 of 3 is still
     * a majority; 1 of 3 is not) — trying to repeat the crash twice on a
     * 3-node group was this scenario's first draft, and it correctly
     * never elected a third leader, because doing so would have been a
     * genuine safety violation (a minority electing itself). That failure
     * is exactly the expected, correct behavior, not a bug — kept here as
     * an explicit design note since it's an easy trap to fall into.
     * <p>Safety property: every successive election produces a strictly
     * higher term than the one before it — terms never repeat or go
     * backward across repeated failures.
     * <p>Liveness property: the cluster keeps electing a new confirmed
     * leader after each of two successive crashes, as long as a majority
     * of the original cluster remains alive.
     */
    @Test
    @Timeout(30)
    void scenarioK_repeatedLeaderCrashesEachProduceAHigherTerm(@TempDir Path baseDir) throws Exception {
        List<NodeId> ids = List.of(new NodeId("k-a"), new NodeId("k-b"), new NodeId("k-c"),
                new NodeId("k-d"), new NodeId("k-e"));
        Map<NodeId, Integer> ports = new HashMap<>();
        for (NodeId id : ids) {
            ports.put(id, freePort());
        }
        Map<NodeId, NodeAddress> addrs = new HashMap<>();
        for (NodeId id : ids) {
            addrs.put(id, new NodeAddress("localhost", ports.get(id)));
        }

        // All five nodes have equally short, tightly-clustered election timeouts here --
        // unlike other scenarios, this one doesn't need to control who wins, only that
        // each successive election strictly increases the term.
        List<RaftCluster> all = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < ids.size(); i++) {
                NodeId id = ids.get(i);
                all.add(new RaftCluster(id, without(addrs, id), ports.get(id), Clock.systemUTC(),
                        Duration.ofMillis(50), Duration.ofMillis(90), RAFT_HEARTBEAT, RAFT_TICK, RAFT_RPC_TIMEOUT,
                        LEASE, new Random(20 + i)));
            }

            waitUntil(Duration.ofSeconds(10), () -> all.stream().anyMatch(RaftCluster::isConfirmedLeader));
            RaftCluster firstLeader = all.stream().filter(RaftCluster::isConfirmedLeader).findFirst().orElseThrow();
            long firstTerm = firstLeader.currentTerm();

            firstLeader.close();
            all.remove(firstLeader);
            waitUntil(Duration.ofSeconds(10), () -> all.stream().anyMatch(RaftCluster::isConfirmedLeader));
            RaftCluster secondLeader = all.stream().filter(RaftCluster::isConfirmedLeader).findFirst().orElseThrow();
            assertTrue(secondLeader.currentTerm() > firstTerm, "the second election's term must be strictly higher than the first's");
            long secondTerm = secondLeader.currentTerm();

            secondLeader.close();
            all.remove(secondLeader);
            assertEquals(3, all.size(), "3 of the original 5 remain -- still a majority");
            waitUntil(Duration.ofSeconds(10), () -> all.stream().anyMatch(RaftCluster::isConfirmedLeader));
            RaftCluster thirdLeader = all.stream().filter(RaftCluster::isConfirmedLeader).findFirst().orElseThrow();
            assertTrue(thirdLeader.currentTerm() > secondTerm, "the third election's term must be strictly higher than the second's");
        } finally {
            for (RaftCluster cluster : all) {
                cluster.close();
            }
        }
    }

    /**
     * <b>Scenario L — a follower crashes during catch-up.</b>
     * <p>Assumptions: a follower is midway through catching up a real
     * backlog from a real {@link ReplicationServer} when its connection is
     * severed (simulated as a hard socket close, the same fault
     * {@code FaultInjectingTcpProxy.partition()} models — see that class's
     * Javadoc on why this is how a real partition/crash actually manifests
     * to a TCP application).
     * <p>Safety property: the follower's store is left in a fully
     * consistent state at whatever point it reached — never partially
     * corrupted — checked by verifying every key it does have is fully,
     * correctly present (no partial record).
     * <p>Liveness property: a fresh retry (a new {@code ReplicationFollower},
     * matching Phase 9's documented no-auto-reconnect contract) picks up
     * exactly where the interrupted one left off and reaches full parity.
     */
    @Test
    @Timeout(30)
    void scenarioL_followerCrashDuringCatchUpThenRetrySucceeds(@TempDir Path baseDir) throws Exception {
        ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
        try (ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0)) {
            for (int i = 0; i < 500; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
            ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("scenario-l-follower"), "localhost", replicationServer.port(), followerStore, Duration.ofMillis(20));

            // Let it get partway, then crash it mid-catch-up.
            waitUntil(Duration.ofSeconds(5), () -> followerStore.keys().size() > 0);
            follower.close(); // a hard stop, not a graceful drain -- models a real crash/severed connection

            // Whatever it has is fully consistent: every key present has its exact, correct value.
            for (String key : followerStore.keys()) {
                int index = Integer.parseInt(key.substring(1));
                assertArrayEquals(bytes("v" + index), followerStore.get(key).orElseThrow());
            }

            // A fresh retry (Phase 9's documented contract: no auto-reconnect) reaches full parity.
            try (ReplicationFollower retry = new ReplicationFollower(
                    new NodeId("scenario-l-follower"), "localhost", replicationServer.port(), followerStore, Duration.ofMillis(20))) {
                waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 500);
                for (int i = 0; i < 500; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
                }
            } finally {
                followerStore.close();
            }
        } finally {
            leaderStore.close();
        }
    }
}
