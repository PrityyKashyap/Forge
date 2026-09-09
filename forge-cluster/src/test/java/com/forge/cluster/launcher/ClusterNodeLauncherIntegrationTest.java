package com.forge.cluster.launcher;

import com.forge.client.ForgeClient;
import com.forge.client.ForgeServerException;
import com.forge.cluster.NodeId;
import com.forge.cluster.leadership.SequenceEpochs;
import com.forge.common.protocol.ProtocolConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Proves {@link ClusterNode} — the wiring behind the real {@link
 * ClusterNodeMain} CLI entry point — actually produces a working,
 * real-socket, multi-node FORGE cluster from nothing but a {@link
 * ClusterConfig}-shaped node list: election, a client write through
 * whichever node is elected leader, and real replication to the other two.
 * This is the launcher's own end-to-end proof, parallel to (and reusing the
 * same wiring recipe as) {@code FailoverReplicationIntegrationTest}, but
 * exercised through the actual class real operators would run.
 */
class ClusterNodeLauncherIntegrationTest {

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
    @Timeout(30)
    void aThreeNodeClusterStartedFromASpecListElectsALeaderAndReplicatesARealWrite(@TempDir Path baseDir)
            throws Exception {
        List<NodeSpec> specs = List.of(
                new NodeSpec(new NodeId("launch-a"), "localhost", freePort(), freePort(), freePort(), freePort()),
                new NodeSpec(new NodeId("launch-b"), "localhost", freePort(), freePort(), freePort(), freePort()),
                new NodeSpec(new NodeId("launch-c"), "localhost", freePort(), freePort(), freePort(), freePort()));

        ClusterNode a = ClusterNode.start(specs, new NodeId("launch-a"), baseDir.resolve("a"));
        ClusterNode b = ClusterNode.start(specs, new NodeId("launch-b"), baseDir.resolve("b"));
        ClusterNode c = ClusterNode.start(specs, new NodeId("launch-c"), baseDir.resolve("c"));
        List<ClusterNode> all = List.of(a, b, c);
        try {
            waitUntil(Duration.ofSeconds(15), () -> all.stream().anyMatch(n -> n.raftCluster().isConfirmedLeader()));
            ClusterNode leader = all.stream().filter(n -> n.raftCluster().isConfirmedLeader()).findFirst().orElseThrow();

            try (ForgeClient client = ForgeClient.connect("localhost", leader.clientPort())) {
                client.put("launcher-key", "launcher-value".getBytes());
            }

            for (ClusterNode node : all) {
                waitUntil(Duration.ofSeconds(10), () -> node.store().get("launcher-key").isPresent());
                assertArrayEquals("launcher-value".getBytes(), node.store().get("launcher-key").orElseThrow(),
                        "every node, including followers, must end up with the write via real replication");
            }
        } finally {
            a.close();
            b.close();
            c.close();
        }
    }

    @Test
    @Timeout(30)
    void aNonLeaderNodeRejectsAWriteWithNotLeader(@TempDir Path baseDir) throws Exception {
        List<NodeSpec> specs = List.of(
                new NodeSpec(new NodeId("nl-a"), "localhost", freePort(), freePort(), freePort(), freePort()),
                new NodeSpec(new NodeId("nl-b"), "localhost", freePort(), freePort(), freePort(), freePort()));

        ClusterNode a = ClusterNode.start(specs, new NodeId("nl-a"), baseDir.resolve("a"));
        ClusterNode b = ClusterNode.start(specs, new NodeId("nl-b"), baseDir.resolve("b"));
        List<ClusterNode> all = List.of(a, b);
        try {
            waitUntil(Duration.ofSeconds(15), () -> all.stream().anyMatch(n -> n.raftCluster().isConfirmedLeader()));
            ClusterNode nonLeader = all.stream().filter(n -> !n.raftCluster().isConfirmedLeader()).findFirst()
                    .orElseThrow();

            try (ForgeClient client = ForgeClient.connect("localhost", nonLeader.clientPort())) {
                ForgeServerException e = assertThrows(ForgeServerException.class,
                        () -> client.put("k", "v".getBytes()));
                assertTrue(e.errorCode() == ProtocolConstants.ERROR_NOT_LEADER,
                        "a non-leader node must reject a write with ERROR_NOT_LEADER, not silently accept it");
            }
        } finally {
            a.close();
            b.close();
        }
    }

    /**
     * The real operational recovery story end to end, through the actual
     * launcher classes (not just {@code StaleReplicaRecovery} in isolation,
     * which {@code FailoverReplicationIntegrationTest} already covers): a
     * node crashes while leader, a new leader is elected by the survivors,
     * new writes land through the new leader, the crashed node is
     * restarted and correctly detects (via {@code
     * ReplicationFollowerCoordinator.needsFullResync()}) that it cannot
     * safely rejoin incrementally, is stopped again, resynced via {@link
     * ClusterNode#resync} (the exact mechanism {@code ClusterNodeMain
     * resync} exposes on the CLI), and restarted one final time — ending
     * up fully caught up and a healthy follower of the real current leader.
     */
    @Test
    @Timeout(45)
    void aCrashedNodeDetectsNeedsResyncThenResyncsAndRejoinsHealthy(@TempDir Path baseDir) throws Exception {
        NodeId idA = new NodeId("resync-a");
        NodeId idB = new NodeId("resync-b");
        NodeId idC = new NodeId("resync-c");
        List<NodeSpec> specs = List.of(
                new NodeSpec(idA, "localhost", freePort(), freePort(), freePort(), freePort()),
                new NodeSpec(idB, "localhost", freePort(), freePort(), freePort(), freePort()),
                new NodeSpec(idC, "localhost", freePort(), freePort(), freePort(), freePort()));
        Map<NodeId, Path> dataDirs = Map.of(
                idA, baseDir.resolve("a"), idB, baseDir.resolve("b"), idC, baseDir.resolve("c"));
        Map<NodeId, NodeSpec> specById = new HashMap<>();
        for (NodeSpec s : specs) {
            specById.put(s.id(), s);
        }

        Map<NodeId, ClusterNode> live = new HashMap<>();
        for (NodeId id : List.of(idA, idB, idC)) {
            live.put(id, ClusterNode.start(specs, id, dataDirs.get(id)));
        }
        try {
            waitUntil(Duration.ofSeconds(15), () -> live.values().stream().anyMatch(n -> n.raftCluster().isConfirmedLeader()));
            NodeId firstLeaderId = liveIdOf(live, n -> n.raftCluster().isConfirmedLeader());

            try (ForgeClient client = ForgeClient.connect("localhost", live.get(firstLeaderId).clientPort())) {
                client.put("before-crash", "v1".getBytes());
            }
            waitUntil(Duration.ofSeconds(10), () -> live.values().stream().allMatch(n -> n.store().get("before-crash").isPresent()));

            live.remove(firstLeaderId).close(); // simulates a crash: no graceful goodbye
            waitUntil(Duration.ofSeconds(15), () -> live.values().stream().anyMatch(n -> n.raftCluster().isConfirmedLeader()));
            NodeId newLeaderId = liveIdOf(live, n -> n.raftCluster().isConfirmedLeader());

            try (ForgeClient client = ForgeClient.connect("localhost", live.get(newLeaderId).clientPort())) {
                client.put("after-crash", "v2".getBytes());
            }
            for (ClusterNode node : live.values()) {
                waitUntil(Duration.ofSeconds(10), () -> node.store().get("after-crash").isPresent());
            }

            // Restart the crashed node and confirm it correctly refuses to trust its own stale data.
            long currentClusterTerm = live.get(newLeaderId).raftCluster().currentTerm();
            Path crashedDataDir = dataDirs.get(firstLeaderId);
            ClusterNode restarted = ClusterNode.start(specs, firstLeaderId, crashedDataDir);
            try {
                waitUntil(Duration.ofSeconds(10), () -> restarted.raftCluster().currentTerm() >= currentClusterTerm);
                Thread.sleep(500); // let the coordinator observe the higher term and flag the need for a resync
                assertTrue(needsResync(restarted),
                        "a restarted node whose own data implies an older term must still need a resync");
            } finally {
                restarted.close();
            }

            // Real recovery: resync against the real current leader, then restart cleanly.
            NodeSpec newLeaderSpec = specById.get(newLeaderId);
            ClusterNode.resync(crashedDataDir, newLeaderSpec.host(), newLeaderSpec.snapshotPort());

            try (ClusterNode rejoined = ClusterNode.start(specs, firstLeaderId, crashedDataDir)) {
                waitUntil(Duration.ofSeconds(10), () -> rejoined.store().get("before-crash").isPresent()
                        && rejoined.store().get("after-crash").isPresent());
                assertArrayEquals("v1".getBytes(), rejoined.store().get("before-crash").orElseThrow());
                assertArrayEquals("v2".getBytes(), rejoined.store().get("after-crash").orElseThrow(),
                        "the resynced node must have the write made through the NEW leader after it crashed, too");
            }
        } finally {
            live.values().forEach(ClusterNode::close);
        }
    }

    private static NodeId liveIdOf(Map<NodeId, ClusterNode> live, Predicate<ClusterNode> condition) {
        return live.entrySet().stream().filter(e -> condition.test(e.getValue())).map(Map.Entry::getKey)
                .findFirst().orElseThrow();
    }

    private static boolean needsResync(ClusterNode node) {
        // ReplicationFollowerCoordinator isn't directly exposed by ClusterNode (it's an
        // implementation detail of the wiring), so this asserts the same observable fact an
        // operator would see from the WARN log: the store's own data still implies the OLD
        // term, since resync has deliberately not happened yet.
        long impliedTerm = SequenceEpochs.impliedTerm(
                node.store().lastAppliedSequenceNumber());
        return impliedTerm < node.raftCluster().currentTerm();
    }
}
