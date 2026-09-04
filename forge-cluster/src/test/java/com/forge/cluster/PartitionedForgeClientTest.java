package com.forge.cluster;

import com.forge.client.ForgeServerException;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.server.ForgeServer;
import com.forge.server.WriteAuthority;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionedForgeClientTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private record Node(NodeId id, ConcurrentLsmKeyValueStore store, ForgeServer server) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            server.close();
            store.close();
        }
    }

    private static Node start(NodeId id, ConsistentHashRing ring, Path dir) throws IOException {
        ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir);
        ForgeServer server = new ForgeServer(store, key -> ring.ownerOf(key).equals(id), 0);
        return new Node(id, store, server);
    }

    @Test
    void rejectsAnEmptyTopology() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionedForgeClient(ClusterTopology.empty()));
    }

    @Test
    @Timeout(30)
    void putGetDeleteRoundTripThroughTwoRealNodes(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("a");
        NodeId b = new NodeId("b");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(a, b));

        try (Node nodeA = start(a, ring, baseDir.resolve("a"));
             Node nodeB = start(b, ring, baseDir.resolve("b"))) {
            ClusterTopology topology = ClusterTopology.empty()
                    .withNode(a, new NodeAddress("localhost", nodeA.server().port()))
                    .withNode(b, new NodeAddress("localhost", nodeB.server().port()));

            try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
                for (int i = 0; i < 50; i++) {
                    client.put("k" + i, bytes("v" + i));
                }
                for (int i = 0; i < 50; i++) {
                    Optional<byte[]> result = client.get("k" + i);
                    assertTrue(result.isPresent());
                    assertArrayEquals(bytes("v" + i), result.get());
                }
                client.delete("k0");
                assertTrue(client.get("k0").isEmpty());
            }
        }
    }

    @Test
    @Timeout(30)
    void ownerOfMatchesTheTopologyWithoutMakingAnyNetworkCall(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("a");
        NodeId b = new NodeId("b");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(a, b));

        try (Node nodeA = start(a, ring, baseDir.resolve("a"));
             Node nodeB = start(b, ring, baseDir.resolve("b"))) {
            ClusterTopology topology = ClusterTopology.empty()
                    .withNode(a, new NodeAddress("localhost", nodeA.server().port()))
                    .withNode(b, new NodeAddress("localhost", nodeB.server().port()));

            try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
                for (int i = 0; i < 20; i++) {
                    assertEquals(ring.ownerOf("k" + i), client.ownerOf("k" + i));
                }
            }
        }
    }

    /**
     * Regression test for a real bug found during Phase 7's adversarial
     * review: an earlier version of {@link PartitionedForgeClient} let
     * multiple threads share one node's {@code ForgeClient} connection
     * concurrently with no synchronization, which is unsafe — {@code
     * ForgeClient} allows only one request in flight per connection.
     * Concurrent callers interleaved their bytes on the wire, corrupting
     * frames (surfaced as a nonsensical "invalid key length" protocol
     * error). This test hammers a single node from many threads through one
     * shared {@code PartitionedForgeClient} and asserts every read-back
     * value is exactly what that thread wrote — not corrupted, not another
     * thread's value.
     */
    @Test
    @Timeout(30)
    void manyThreadsRoutingToTheSameNodeThroughOneClientNeverCorruptTheWire(@TempDir Path baseDir) throws Exception {
        NodeId onlyNode = new NodeId("solo");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(onlyNode));

        try (Node node = start(onlyNode, ring, baseDir.resolve("solo"))) {
            ClusterTopology topology = ClusterTopology.empty()
                    .withNode(onlyNode, new NodeAddress("localhost", node.server().port()));

            int threadCount = 20;
            int opsPerThread = 40;
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
                            String key = "thread-" + threadId + "-key-" + i;
                            String value = "thread-" + threadId + "-value-" + i;
                            client.put(key, bytes(value));
                            Optional<byte[]> result = client.get(key);
                            assertTrue(result.isPresent(), key + " must be present immediately after its own PUT");
                            assertArrayEquals(bytes(value), result.get(),
                                    key + " must read back exactly what this thread wrote, not a corrupted or foreign value");
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
    }

    // =====================================================================
    // Phase 15: ERROR_NOT_LEADER redirect
    // =====================================================================

    private static final class FixedWriteAuthority implements WriteAuthority {
        private final boolean canWrite;
        private final String leaderHint;

        FixedWriteAuthority(boolean canWrite, String leaderHint) {
            this.canWrite = canWrite;
            this.leaderHint = leaderHint;
        }

        @Override
        public boolean canAcceptWrites() {
            return canWrite;
        }

        @Override
        public long currentTerm() {
            return 7;
        }

        @Override
        public Optional<String> currentLeaderHint() {
            return Optional.ofNullable(leaderHint);
        }
    }

    @Test
    @Timeout(30)
    void notLeaderResponseRedirectsRetryToTheHintedLeader(@TempDir Path baseDir) throws Exception {
        NodeId nodeX = new NodeId("x");
        NodeId nodeY = new NodeId("y");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(nodeX, nodeY));
        String key = "redirect-test-key";
        NodeId owner = ring.ownerOf(key);
        NodeId other = owner.equals(nodeX) ? nodeY : nodeX;

        // Both nodes serve the same single logical partition here (a replica set), matching
        // Phase 15's scope — see com.forge.cluster.leadership's package Javadoc.
        try (ConcurrentLsmKeyValueStore storeOwner = new ConcurrentLsmKeyValueStore(baseDir.resolve(owner.value()));
             ForgeServer serverOwner = new ForgeServer(storeOwner, k -> true,
                     new FixedWriteAuthority(false, other.value()), 0);
             ConcurrentLsmKeyValueStore storeOther = new ConcurrentLsmKeyValueStore(baseDir.resolve(other.value()));
             ForgeServer serverOther = new ForgeServer(storeOther, k -> true, WriteAuthority.NONE, 0)) {

            ClusterTopology topology = ClusterTopology.empty()
                    .withNode(owner, new NodeAddress("localhost", serverOwner.port()))
                    .withNode(other, new NodeAddress("localhost", serverOther.port()));
            assertEquals(owner, topology.ownerOf(key), "sanity check: the ring's owner must not have changed");

            try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
                client.put(key, bytes("v1"));

                assertArrayEquals(bytes("v1"), storeOther.get(key).orElseThrow(),
                        "the write must have actually landed on the redirected-to node");
                assertTrue(storeOwner.get(key).isEmpty(), "the rejecting node must never have received the write");
            }
        }
    }

    @Test
    @Timeout(30)
    void noRedirectTargetSurfacesTheOriginalNotLeaderError(@TempDir Path baseDir) throws Exception {
        NodeId solo = new NodeId("solo");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(solo));
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(baseDir.resolve("solo"));
             ForgeServer server = new ForgeServer(store, k -> true, new FixedWriteAuthority(false, null), 0)) {

            ClusterTopology topology = ClusterTopology.empty().withNode(solo, new NodeAddress("localhost", server.port()));
            try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
                ForgeServerException e = assertThrows(ForgeServerException.class, () -> client.put("k", bytes("v")));
                assertEquals(ProtocolConstants.ERROR_NOT_LEADER, e.errorCode());
            }
        }
    }

    @Test
    void parseLeaderHintExtractsTheDocumentedFormat() {
        assertEquals(Optional.of(new NodeId("node-b")),
                PartitionedForgeClient.parseLeaderHint("NOT_LEADER term=5 leader=node-b"));
        assertEquals(Optional.empty(), PartitionedForgeClient.parseLeaderHint("NOT_LEADER term=5 leader=none"));
        assertEquals(Optional.empty(), PartitionedForgeClient.parseLeaderHint("some unrelated message"));
        assertEquals(Optional.empty(), PartitionedForgeClient.parseLeaderHint(null));
    }
}
