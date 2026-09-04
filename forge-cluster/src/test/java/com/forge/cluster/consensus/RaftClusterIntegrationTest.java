package com.forge.cluster.consensus;

import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Real sockets, real processes-within-the-JVM, real wall-clock timing —
 * unlike {@link RaftNodeTest}, which proves the state machine's logic with
 * a fake clock, this proves the actual networked wiring
 * ({@link RaftRpcServer}, {@link RaftWireFormat}, {@link RaftCluster}'s
 * dispatch loop) genuinely elects a leader and fails over across real
 * node-to-node RPCs. Kept fast with short timeouts and a bounded poll,
 * same discipline as {@code HeartbeatServiceTest}/{@code ReplicationIntegrationTest}.
 */
class RaftClusterIntegrationTest {

    private static final Duration ELECTION_MIN = Duration.ofMillis(150);
    private static final Duration ELECTION_MAX = Duration.ofMillis(300);
    private static final Duration HEARTBEAT = Duration.ofMillis(40);
    private static final Duration TICK = Duration.ofMillis(15);
    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(2);

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

    private static RaftCluster startNode(NodeId id, int port, Map<NodeId, NodeAddress> peerAddresses, long seed)
            throws IOException {
        Map<NodeId, NodeAddress> peersOnly = new HashMap<>(peerAddresses);
        peersOnly.remove(id);
        return new RaftCluster(id, peersOnly, port, Clock.systemUTC(), ELECTION_MIN, ELECTION_MAX, HEARTBEAT,
                TICK, RPC_TIMEOUT, new Random(seed));
    }

    private static Optional<RaftCluster> findConfirmedLeader(List<RaftCluster> nodes) {
        return nodes.stream().filter(RaftCluster::isConfirmedLeader).findFirst();
    }

    @Test
    @Timeout(30)
    void threeNodeClusterElectsExactlyOneLeaderAndFollowersAgreeOnIt() throws Exception {
        NodeId n1 = new NodeId("raft-a");
        NodeId n2 = new NodeId("raft-b");
        NodeId n3 = new NodeId("raft-c");
        Map<NodeId, NodeAddress> addresses = new HashMap<>();
        addresses.put(n1, new NodeAddress("localhost", freePort()));
        addresses.put(n2, new NodeAddress("localhost", freePort()));
        addresses.put(n3, new NodeAddress("localhost", freePort()));

        RaftCluster a = startNode(n1, addresses.get(n1).port(), addresses, 1);
        RaftCluster b = startNode(n2, addresses.get(n2).port(), addresses, 2);
        RaftCluster c = startNode(n3, addresses.get(n3).port(), addresses, 3);
        List<RaftCluster> all = List.of(a, b, c);
        try {
            waitUntil(Duration.ofSeconds(10), () -> findConfirmedLeader(all).isPresent());

            RaftCluster leader = findConfirmedLeader(all).orElseThrow();
            long term = leader.currentTerm();
            assertEquals(1, all.stream().filter(RaftCluster::isConfirmedLeader).count(),
                    "exactly one node must be a confirmed leader at once");

            waitUntil(Duration.ofSeconds(5), () -> all.stream()
                    .allMatch(n -> n.currentLeader().isPresent()));
            for (RaftCluster node : all) {
                assertEquals(leader.currentLeader(), node.currentLeader(),
                        "every node must agree on who the current leader is");
            }

            // Leadership must be stable across several real heartbeat cycles, not a one-shot fluke.
            Thread.sleep(HEARTBEAT.toMillis() * 5);
            assertTrue(leader.isConfirmedLeader(), "the elected leader must remain leader while heartbeats continue");
            assertEquals(term, leader.currentTerm(), "the term must not change while nothing has failed");
        } finally {
            a.close();
            b.close();
            c.close();
        }
    }

    @Test
    @Timeout(30)
    void killingTheLeaderTriggersFailoverToASurvivorWithAHigherTerm() throws Exception {
        NodeId n1 = new NodeId("raft-x");
        NodeId n2 = new NodeId("raft-y");
        NodeId n3 = new NodeId("raft-z");
        Map<NodeId, NodeAddress> addresses = new HashMap<>();
        addresses.put(n1, new NodeAddress("localhost", freePort()));
        addresses.put(n2, new NodeAddress("localhost", freePort()));
        addresses.put(n3, new NodeAddress("localhost", freePort()));

        RaftCluster a = startNode(n1, addresses.get(n1).port(), addresses, 11);
        RaftCluster b = startNode(n2, addresses.get(n2).port(), addresses, 12);
        RaftCluster c = startNode(n3, addresses.get(n3).port(), addresses, 13);
        List<RaftCluster> all = List.of(a, b, c);
        try {
            waitUntil(Duration.ofSeconds(10), () -> findConfirmedLeader(all).isPresent());
            RaftCluster firstLeader = findConfirmedLeader(all).orElseThrow();
            long firstTerm = firstLeader.currentTerm();

            firstLeader.close(); // simulates a crash: no graceful goodbye, just gone
            List<RaftCluster> survivors = all.stream().filter(n -> n != firstLeader).toList();

            waitUntil(Duration.ofSeconds(10), () -> findConfirmedLeader(survivors).isPresent());
            RaftCluster newLeader = findConfirmedLeader(survivors).orElseThrow();

            assertTrue(newLeader.currentTerm() > firstTerm,
                    "a new election after a leader crash must produce a strictly higher term");
            waitUntil(Duration.ofSeconds(5), () -> survivors.stream()
                    .map(RaftCluster::currentLeader)
                    .distinct()
                    .count() == 1);
            assertEquals(1, survivors.stream().map(RaftCluster::currentLeader).distinct().count(),
                    "both survivors must agree on the same new leader");
        } finally {
            closeQuietly(a);
            closeQuietly(b);
            closeQuietly(c);
        }
    }

    private static void closeQuietly(RaftCluster cluster) {
        try {
            cluster.close();
        } catch (RuntimeException e) {
            // already closed or closing twice — acceptable in test cleanup
        }
    }
}
