package com.forge.cluster.leadership;

import com.forge.client.ForgeClient;
import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.cluster.recovery.SnapshotServer;
import com.forge.cluster.recovery.StaleReplicaRecovery;
import com.forge.cluster.replication.ReplicationServer;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Proves the full Raft-driven data-plane failover-and-recovery flow end to
 * end, with real data actually moving: a real leader, real followers
 * genuinely catching up via {@link ReplicationFollowerCoordinator}, a real
 * failover to a new leader at a higher term, replication continuing
 * through the new leader, and the old leader rejoining via a full
 * {@link StaleReplicaRecovery} snapshot resync (the one case incremental
 * catch-up cannot safely handle — see that class's Javadoc).
 *
 * <p>Every node runs a {@link ReplicationServer} continuously regardless of
 * current leadership (see {@code ReplicationFollowerCoordinator}'s class
 * Javadoc for why) — what changes on failover is only which node's
 * {@code ReplicationFollower} points at which leader.
 */
class FailoverReplicationIntegrationTest {

    private static final Duration A_ELECTION_MIN = Duration.ofMillis(40);
    private static final Duration A_ELECTION_MAX = Duration.ofMillis(60);
    private static final Duration BC_ELECTION_MIN = Duration.ofMillis(400);
    private static final Duration BC_ELECTION_MAX = Duration.ofMillis(600);
    private static final Duration HEARTBEAT = Duration.ofMillis(25);
    private static final Duration TICK = Duration.ofMillis(10);
    private static final Duration RPC_TIMEOUT = Duration.ofMillis(500);
    private static final Duration LEASE = A_ELECTION_MIN;
    private static final Duration ACK_INTERVAL = Duration.ofMillis(20);
    private static final Duration COORDINATOR_POLL = Duration.ofMillis(20);

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

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    @Test
    @Timeout(60)
    void leaderFailsOverAndDataKeepsFlowingThroughTheNewLeaderThenOldLeaderResyncsOnRejoin(@TempDir Path baseDir)
            throws Exception {
        NodeId a = new NodeId("fo-a");
        NodeId b = new NodeId("fo-b");
        NodeId c = new NodeId("fo-c");

        int aRaftPort = freePort();
        int bRaftPort = freePort();
        int cRaftPort = freePort();
        Map<NodeId, NodeAddress> raftAddresses = Map.of(
                a, new NodeAddress("localhost", aRaftPort),
                b, new NodeAddress("localhost", bRaftPort),
                c, new NodeAddress("localhost", cRaftPort));

        try (ConcurrentLsmKeyValueStore storeA = new ConcurrentLsmKeyValueStore(baseDir.resolve("a"));
             ConcurrentLsmKeyValueStore storeB = new ConcurrentLsmKeyValueStore(baseDir.resolve("b"));
             ConcurrentLsmKeyValueStore storeC = new ConcurrentLsmKeyValueStore(baseDir.resolve("c"));
             ReplicationServer replA = new ReplicationServer(storeA, 0);
             ReplicationServer replB = new ReplicationServer(storeB, 0);
             ReplicationServer replC = new ReplicationServer(storeC, 0)) {

            Map<NodeId, NodeAddress> replicationAddresses = Map.of(
                    a, new NodeAddress("localhost", replA.port()),
                    b, new NodeAddress("localhost", replB.port()),
                    c, new NodeAddress("localhost", replC.port()));

            try (RaftCluster raftA = new RaftCluster(a, without(raftAddresses, a), aRaftPort, Clock.systemUTC(),
                            A_ELECTION_MIN, A_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(1));
                    RaftCluster raftB = new RaftCluster(b, without(raftAddresses, b), bRaftPort, Clock.systemUTC(),
                            BC_ELECTION_MIN, BC_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(2));
                    RaftCluster raftC = new RaftCluster(c, without(raftAddresses, c), cRaftPort, Clock.systemUTC(),
                            BC_ELECTION_MIN, BC_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(3));
                    ReplicationFollowerCoordinator coordA = new ReplicationFollowerCoordinator(
                            a, raftA, storeA, replicationAddresses, ACK_INTERVAL, COORDINATOR_POLL);
                    ReplicationFollowerCoordinator coordB = new ReplicationFollowerCoordinator(
                            b, raftB, storeB, replicationAddresses, ACK_INTERVAL, COORDINATOR_POLL);
                    ReplicationFollowerCoordinator coordC = new ReplicationFollowerCoordinator(
                            c, raftC, storeC, replicationAddresses, ACK_INTERVAL, COORDINATOR_POLL)) {

                PartitionLeadership leadershipA = new PartitionLeadership("p0", raftA, storeA);
                try (ForgeServer serverA = new ForgeServer(storeA, key -> true, leadershipA, 0)) {

                    // --- Normal operation: A leads, B and C follow and actually catch up. ---
                    waitUntil(Duration.ofSeconds(10), leadershipA::canAcceptWrites);
                    long termT = raftA.currentTerm();

                    try (ForgeClient client = ForgeClient.connect("localhost", serverA.port())) {
                        client.put("k1", "v1".getBytes());
                        client.put("k2", "v2".getBytes());
                    }
                    waitUntil(Duration.ofSeconds(5), () -> storeB.get("k2").isPresent());
                    waitUntil(Duration.ofSeconds(5), () -> storeC.get("k2").isPresent());
                    assertArrayEquals("v1".getBytes(), storeB.get("k1").orElseThrow());
                    assertArrayEquals("v1".getBytes(), storeC.get("k1").orElseThrow());

                    // --- A is killed outright (this test's failure mode; the stale-but-alive
                    // case is StaleLeaderFencingIntegrationTest's job). ---
                    serverA.close();
                    raftA.close();
                    coordA.close();

                    // B and C elect a new leader at a higher term using their intact majority.
                    waitUntil(Duration.ofSeconds(10), () -> raftB.isConfirmedLeader() || raftC.isConfirmedLeader());
                    RaftCluster newLeaderRaft = raftB.isConfirmedLeader() ? raftB : raftC;
                    ConcurrentLsmKeyValueStore newLeaderStore = newLeaderRaft == raftB ? storeB : storeC;
                    NodeId newLeaderId = newLeaderRaft == raftB ? b : c;
                    RaftCluster survivorRaft = newLeaderRaft == raftB ? raftC : raftB;
                    ConcurrentLsmKeyValueStore survivorStore = newLeaderRaft == raftB ? storeC : storeB;
                    assertTrue(newLeaderRaft.currentTerm() > termT);

                    // The new leader needs its own ForgeServer + PartitionLeadership, exactly
                    // like A had (this simulates the surrounding infra reacting to the failover —
                    // a real deployment would already have one running per node; see
                    // docs/ARCHITECTURE.md §3.12 on this phase's scope).
                    PartitionLeadership newLeadership = new PartitionLeadership("p0", newLeaderRaft, newLeaderStore);
                    try (ForgeServer newServer = new ForgeServer(newLeaderStore, key -> true, newLeadership, 0)) {
                        waitUntil(Duration.ofSeconds(5), newLeadership::canAcceptWrites);

                        // The surviving follower's coordinator must redirect itself to the new leader.
                        waitUntil(Duration.ofSeconds(5),
                                () -> survivorRaft.currentLeader().map(newLeaderId::equals).orElse(false));

                        try (ForgeClient client = ForgeClient.connect("localhost", newServer.port())) {
                            client.put("k3", "v3".getBytes());
                        }
                        waitUntil(Duration.ofSeconds(5), () -> survivorStore.get("k3").isPresent());
                        assertArrayEquals("v3".getBytes(), survivorStore.get("k3").orElseThrow(),
                                "data written through the NEW leader must reach the surviving follower");

                        // --- Old leader A "rejoins": a fresh Raft node (Phase 14 has no persistent
                        // state, so this is a genuinely fresh RaftNode, exactly as a real restart
                        // would produce) learns of the new, higher term and must resync rather than
                        // trust its own stale local data. ---
                        Map<NodeId, NodeAddress> rejoinAddresses = Map.of(
                                b, new NodeAddress("localhost", bRaftPort), c, new NodeAddress("localhost", cRaftPort));
                        try (RaftCluster raftARejoined = new RaftCluster(a, rejoinAddresses, aRaftPort, Clock.systemUTC(),
                                A_ELECTION_MIN, A_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(9))) {
                            waitUntil(Duration.ofSeconds(5), () -> raftARejoined.currentTerm() >= newLeaderRaft.currentTerm());

                            long aImpliedTerm = SequenceEpochs.impliedTerm(storeA.lastAppliedSequenceNumber());
                            assertTrue(aImpliedTerm < raftARejoined.currentTerm(),
                                    "A's own data must imply an older term than the cluster's current one");

                            // StaleReplicaRecovery talks to a SnapshotServer (Phase 10's full-transfer
                            // protocol) -- a genuinely different wire protocol from ReplicationServer's
                            // (Phase 9's incremental-tail protocol), so the new leader needs one running.
                            ConcurrentLsmKeyValueStore resyncedA;
                            try (SnapshotServer snapshotServer = new SnapshotServer(newLeaderStore, 0)) {
                                resyncedA = StaleReplicaRecovery.resyncFromSnapshot(
                                        storeA, baseDir.resolve("a"), 4L * 1024 * 1024,
                                        "localhost", snapshotServer.port());
                            }
                            try {
                                assertArrayEquals("v1".getBytes(), resyncedA.get("k1").orElseThrow());
                                assertArrayEquals("v2".getBytes(), resyncedA.get("k2").orElseThrow());
                                assertArrayEquals("v3".getBytes(), resyncedA.get("k3").orElseThrow(),
                                        "the resynced old leader must have the data written after it failed too");
                            } finally {
                                resyncedA.close();
                            }
                        }
                    }
                }
            }
        }
    }

    private static Map<NodeId, NodeAddress> without(Map<NodeId, NodeAddress> addresses, NodeId self) {
        Map<NodeId, NodeAddress> copy = new HashMap<>(addresses);
        copy.remove(self);
        return copy;
    }
}
