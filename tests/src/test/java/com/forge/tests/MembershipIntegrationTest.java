package com.forge.tests;

import com.forge.cluster.ClusterTopology;
import com.forge.cluster.ConsistentHashRing;
import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.PartitionedForgeClient;
import com.forge.cluster.membership.FailureDetector;
import com.forge.cluster.membership.HeartbeatService;
import com.forge.cluster.membership.NodeState;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Phase 7 (partitioning) and Phase 8 (membership/failure detection)
 * together, on the same real 3-node cluster: each node runs both a real
 * {@link ForgeServer} serving its shard of the data and a real
 * {@link HeartbeatService} announcing its liveness over real UDP to the
 * other two — this is what "every distributed-systems feature must
 * integrate with the existing storage and networking layers" means in
 * practice, not two features that happen to live in the same module.
 */
class MembershipIntegrationTest {

    private record ClusterNode(NodeId id, ConcurrentLsmKeyValueStore store, ForgeServer server,
            FailureDetector detector, HeartbeatService heartbeats) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            heartbeats.close();
            server.close();
            store.close();
        }
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

    @Test
    @Timeout(30)
    void aKilledNodesDataPartitionAndItsMembershipStatusAreBothObservedCorrectlyByItsPeers(@TempDir Path baseDir)
            throws Exception {
        NodeId a = new NodeId("mi-a");
        NodeId b = new NodeId("mi-b");
        NodeId c = new NodeId("mi-c");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(a, b, c));
        Duration suspectTimeout = Duration.ofMillis(200);
        Duration deadTimeout = Duration.ofMillis(400);
        Duration heartbeatInterval = Duration.ofMillis(30);

        List<ClusterNode> nodes = new ArrayList<>();
        try {
            for (NodeId id : List.of(a, b, c)) {
                ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(baseDir.resolve(id.value()));
                ForgeServer server = new ForgeServer(store, key -> ring.ownerOf(key).equals(id), 0);
                FailureDetector detector = new FailureDetector(Clock.systemUTC(), suspectTimeout, deadTimeout);
                HeartbeatService heartbeats = new HeartbeatService(id, 0, detector, heartbeatInterval);
                nodes.add(new ClusterNode(id, store, server, detector, heartbeats));
            }

            // Every node's heartbeat service learns about the other two.
            for (ClusterNode self : nodes) {
                for (ClusterNode peer : nodes) {
                    if (!self.id().equals(peer.id())) {
                        self.heartbeats().addPeer(peer.id(), new NodeAddress("localhost", peer.heartbeats().port()));
                    }
                }
            }

            ClusterTopology topology = ClusterTopology.empty();
            for (ClusterNode node : nodes) {
                topology = topology.withNode(node.id(), new NodeAddress("localhost", node.server().port()));
            }

            // Data plane: write through the partitioned client while all 3 nodes are healthy.
            try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
                for (int i = 0; i < 60; i++) {
                    client.put("mi-key-" + i, bytes("v" + i));
                }
            }

            // Membership plane: every node must see the other two as ALIVE once real
            // heartbeats have had time to establish that (well past suspectTimeout).
            Thread.sleep(suspectTimeout.toMillis() * 2);
            for (ClusterNode self : nodes) {
                for (ClusterNode peer : nodes) {
                    if (!self.id().equals(peer.id())) {
                        assertEquals(NodeState.ALIVE, self.detector().stateOf(peer.id()).orElseThrow(),
                                self.id() + " must see " + peer.id() + " as ALIVE before anything has failed");
                    }
                }
            }

            // Kill node C entirely: both its data-serving TCP server and its heartbeat UDP service.
            ClusterNode nodeC = nodes.stream().filter(n -> n.id().equals(c)).findFirst().orElseThrow();
            nodeC.close();

            // Membership plane: the two survivors must independently detect C as DEAD.
            ClusterNode nodeA = nodes.stream().filter(n -> n.id().equals(a)).findFirst().orElseThrow();
            ClusterNode nodeB = nodes.stream().filter(n -> n.id().equals(b)).findFirst().orElseThrow();
            waitUntil(Duration.ofSeconds(5), () -> nodeA.detector().stateOf(c).orElseThrow() == NodeState.DEAD);
            waitUntil(Duration.ofSeconds(5), () -> nodeB.detector().stateOf(c).orElseThrow() == NodeState.DEAD);
            // A and B must still see each other as alive — this is C's failure, not a network-wide one.
            assertEquals(NodeState.ALIVE, nodeA.detector().stateOf(b).orElseThrow());
            assertEquals(NodeState.ALIVE, nodeB.detector().stateOf(a).orElseThrow());

            // Data plane: keys owned by the surviving nodes are still fully readable —
            // this phase does not attempt failover for C's own shard (that's Phase 9/14),
            // so only C's own keys become unreachable; verify precisely that split.
            try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
                for (int i = 0; i < 60; i++) {
                    String key = "mi-key-" + i;
                    if (!ring.ownerOf(key).equals(c)) {
                        var result = client.get(key);
                        assertEquals(true, result.isPresent(), key + " belongs to a surviving node and must still be readable");
                        assertArrayEquals(bytes("v" + i), result.get());
                    }
                }
            }
        } finally {
            for (ClusterNode node : nodes) {
                try {
                    node.close();
                } catch (IOException ignored) {
                    // already closed (node C) or closing during cleanup; either way, nothing more to do
                }
            }
        }
    }
}
