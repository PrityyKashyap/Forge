package com.forge.tests;

import com.forge.client.ForgeClient;
import com.forge.client.ForgeServerException;
import com.forge.cluster.ClusterTopology;
import com.forge.cluster.ConsistentHashRing;
import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.PartitionRebalancer;
import com.forge.cluster.PartitionedForgeClient;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 7: real multi-node partitioning. Every node in these tests is a
 * genuinely separate {@link ForgeServer} + {@link ConcurrentLsmKeyValueStore}
 * pair, each with its own TCP port and its own on-disk data directory — this
 * is not partitioning simulated inside one process or one store; a key
 * physically lives in exactly one node's directory, and this is verified
 * directly against each node's own store, not just inferred from routing
 * appearing to work.
 */
class PartitioningIntegrationTest {

    /** One real node: its identity, its own store, its own server. */
    private record TestNode(NodeId id, ConcurrentLsmKeyValueStore store, ForgeServer server) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            server.close();
            store.close();
        }
    }

    private final List<TestNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() throws IOException {
        for (TestNode node : nodes) {
            node.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Starts one real node per id, each with an ownership predicate derived
     * from {@code ring} (computed before any server starts, since ownership
     * depends only on the ring, not on ports — this sidesteps the
     * chicken-and-egg problem of needing addresses to build a topology that
     * a node's own predicate needs before it has a port yet).
     */
    private TestNode startNode(NodeId id, ConsistentHashRing ring, Path dataDir) throws IOException {
        ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
        ForgeServer server;
        try {
            server = new ForgeServer(store, key -> ring.ownerOf(key).equals(id), 0);
        } catch (IOException | RuntimeException e) {
            store.close();
            throw e;
        }
        TestNode node = new TestNode(id, store, server);
        nodes.add(node);
        return node;
    }

    private static ClusterTopology topologyOf(List<TestNode> cluster) {
        ClusterTopology topology = ClusterTopology.empty();
        for (TestNode node : cluster) {
            topology = topology.withNode(node.id(), new NodeAddress("localhost", node.server().port()));
        }
        return topology;
    }

    @Test
    @Timeout(60)
    void keysRouteToTheirOwningNodeAndPhysicallyLiveOnlyThere(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("node-a");
        NodeId b = new NodeId("node-b");
        NodeId c = new NodeId("node-c");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(a, b, c));

        TestNode nodeA = startNode(a, ring, baseDir.resolve("a"));
        TestNode nodeB = startNode(b, ring, baseDir.resolve("b"));
        TestNode nodeC = startNode(c, ring, baseDir.resolve("c"));
        List<TestNode> cluster = List.of(nodeA, nodeB, nodeC);
        ClusterTopology topology = topologyOf(cluster);

        int keyCount = 300;
        try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
            for (int i = 0; i < keyCount; i++) {
                client.put("key-" + i, bytes("value-" + i));
            }
            // Routing round-trips correctly through the partitioned client.
            for (int i = 0; i < keyCount; i++) {
                Optional<byte[]> result = client.get("key-" + i);
                assertTrue(result.isPresent(), "key-" + i + " must be readable through the partitioned client");
                assertArrayEquals(bytes("value-" + i), result.get());
            }
        }

        // Now verify PHYSICAL placement directly against each node's own store,
        // bypassing routing entirely — proves genuine sharding, not just that
        // routing "worked" because one store secretly had everything.
        int foundExactlyOnce = 0;
        for (int i = 0; i < keyCount; i++) {
            String key = "key-" + i;
            NodeId expectedOwner = topology.ownerOf(key);
            int presentCount = 0;
            for (TestNode node : cluster) {
                boolean present = node.store().get(key).isPresent();
                if (present) {
                    presentCount++;
                    assertEquals(expectedOwner, node.id(),
                            key + " was found on " + node.id() + " but the topology says " + expectedOwner + " owns it");
                }
            }
            assertEquals(1, presentCount, key + " must live on exactly one node's disk");
            foundExactlyOnce++;
        }
        assertEquals(keyCount, foundExactlyOnce);

        // Every node actually got at least some keys (sanity check on distribution).
        for (TestNode node : cluster) {
            assertFalse(node.store().keys().isEmpty(), node.id() + " received no keys at all — check the hash distribution");
        }
    }

    @Test
    @Timeout(30)
    void aNodeRejectsAKeyItDoesNotOwnWithoutTouchingItsStore(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("node-a");
        NodeId b = new NodeId("node-b");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(a, b));
        TestNode nodeA = startNode(a, ring, baseDir.resolve("a"));
        startNode(b, ring, baseDir.resolve("b"));

        // Find a key node-a does NOT own, then ask node-a for it directly (bypassing routing).
        String foreignKey = null;
        for (int i = 0; i < 1000; i++) {
            String candidate = "probe-" + i;
            if (!ring.ownerOf(candidate).equals(a)) {
                foreignKey = candidate;
                break;
            }
        }
        assertTrue(foreignKey != null, "expected to find at least one key node-a doesn't own out of 1000 candidates");
        String key = foreignKey;

        try (ForgeClient direct = ForgeClient.connect("localhost", nodeA.server().port())) {
            ForgeServerException e = assertThrows(ForgeServerException.class, () -> direct.get(key));
            assertEquals(ProtocolConstants.ERROR_NOT_OWNER, e.errorCode());
        }
        assertTrue(nodeA.store().get(foreignKey).isEmpty(), "a rejected request must never reach the local store");
    }

    @Test
    @Timeout(60)
    void addingANodeAndRebalancingMovesOnlyTheAffectedKeys(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("node-a");
        NodeId b = new NodeId("node-b");
        ConsistentHashRing oldRing = ConsistentHashRing.of(Set.of(a, b));

        TestNode nodeA = startNode(a, oldRing, baseDir.resolve("a"));
        TestNode nodeB = startNode(b, oldRing, baseDir.resolve("b"));
        ClusterTopology oldTopology = topologyOf(List.of(nodeA, nodeB));

        int keyCount = 400;
        try (PartitionedForgeClient client = new PartitionedForgeClient(oldTopology)) {
            for (int i = 0; i < keyCount; i++) {
                client.put("rk-" + i, bytes("v" + i));
            }
        }

        // A new node joins. Its ownership predicate must be rebuilt around the NEW
        // ring (a running node's predicate isn't automatically live-updated in this
        // phase — see PROGRESS.md); rebuild node-a/node-b's predicates too by
        // restarting their servers against the new ring, atop their EXISTING stores
        // (data survives; only the serving process's ownership view changes).
        NodeId c = new NodeId("node-c");
        ConsistentHashRing newRing = oldRing.withNode(c);

        nodeA.server().close();
        nodeB.server().close();
        ForgeServer refreshedA = new ForgeServer(nodeA.store(), key -> newRing.ownerOf(key).equals(a), 0);
        ForgeServer refreshedB = new ForgeServer(nodeB.store(), key -> newRing.ownerOf(key).equals(b), 0);
        TestNode nodeC = startNode(c, newRing, baseDir.resolve("c"));

        TestNode refreshedNodeA = new TestNode(a, nodeA.store(), refreshedA);
        TestNode refreshedNodeB = new TestNode(b, nodeB.store(), refreshedB);
        nodes.remove(nodeA);
        nodes.remove(nodeB);
        nodes.add(refreshedNodeA);
        nodes.add(refreshedNodeB);

        ClusterTopology newTopology = topologyOf(List.of(refreshedNodeA, refreshedNodeB, nodeC));

        // Which keys SHOULD move, computed independently of the rebalancer, to check its work against.
        Set<String> expectedToMoveFromA = new HashSet<>();
        Set<String> expectedToMoveFromB = new HashSet<>();
        for (int i = 0; i < keyCount; i++) {
            String key = "rk-" + i;
            NodeId oldOwner = oldTopology.ownerOf(key);
            NodeId newOwner = newTopology.ownerOf(key);
            if (!oldOwner.equals(newOwner)) {
                if (oldOwner.equals(a)) {
                    expectedToMoveFromA.add(key);
                } else {
                    expectedToMoveFromB.add(key);
                }
            }
        }
        assertTrue(!expectedToMoveFromA.isEmpty() || !expectedToMoveFromB.isEmpty(),
                "adding a node should have moved at least some keys in a 400-key dataset");

        try (PartitionedForgeClient destinationRouter = new PartitionedForgeClient(newTopology)) {
            PartitionRebalancer.MigrationResult resultA = PartitionRebalancer.migrate(
                    a, refreshedNodeA.store(), newTopology, destinationRouter);
            PartitionRebalancer.MigrationResult resultB = PartitionRebalancer.migrate(
                    b, refreshedNodeB.store(), newTopology, destinationRouter);

            assertEquals(expectedToMoveFromA, resultA.keysMoved());
            assertEquals(expectedToMoveFromB, resultB.keysMoved());
        }

        // After rebalancing: every key is readable through the NEW topology,
        // physically lives only where the NEW topology says, and the moved
        // keys are actually gone from their old node.
        try (PartitionedForgeClient client = new PartitionedForgeClient(newTopology)) {
            for (int i = 0; i < keyCount; i++) {
                String key = "rk-" + i;
                Optional<byte[]> result = client.get(key);
                assertTrue(result.isPresent(), key + " must survive rebalancing");
                assertArrayEquals(bytes("v" + i), result.get());
            }
        }
        for (String key : expectedToMoveFromA) {
            assertTrue(refreshedNodeA.store().get(key).isEmpty(), key + " should have been removed from node-a after moving");
        }
        for (String key : expectedToMoveFromB) {
            assertTrue(refreshedNodeB.store().get(key).isEmpty(), key + " should have been removed from node-b after moving");
        }
    }

    @Test
    @Timeout(30)
    void concurrentRoutingThroughOnePartitionedClientIsSafe(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("node-a");
        NodeId b = new NodeId("node-b");
        NodeId c = new NodeId("node-c");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(a, b, c));
        TestNode nodeA = startNode(a, ring, baseDir.resolve("a"));
        TestNode nodeB = startNode(b, ring, baseDir.resolve("b"));
        TestNode nodeC = startNode(c, ring, baseDir.resolve("c"));
        ClusterTopology topology = topologyOf(List.of(nodeA, nodeB, nodeC));

        int threadCount = 16;
        int opsPerThread = 50;
        try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            CountDownLatch ready = new CountDownLatch(threadCount);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                int threadId = t;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "concurrent-" + threadId + "-" + i;
                        client.put(key, bytes("v" + threadId + "-" + i));
                        Optional<byte[]> result = client.get(key);
                        assertTrue(result.isPresent());
                        assertArrayEquals(bytes("v" + threadId + "-" + i), result.get());
                    }
                    return null;
                }));
            }
            ready.await();
            go.countDown();
            for (Future<Void> f : futures) {
                f.get(20, TimeUnit.SECONDS);
            }
            pool.shutdown();
        }
    }

    @Test
    void partitionedClientRejectsAnEmptyTopology() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionedForgeClient(ClusterTopology.empty()));
    }
}
