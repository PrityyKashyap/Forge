package com.forge.cluster;

import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRebalancerTest {

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
    @Timeout(30)
    void migrateIsANoOpWhenNoKeysChangedOwnership(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("a");
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(a));

        try (Node nodeA = start(a, ring, baseDir.resolve("a"))) {
            nodeA.store().put("k1", bytes("v1"));
            nodeA.store().put("k2", bytes("v2"));

            ClusterTopology sameTopology = ClusterTopology.empty()
                    .withNode(a, new NodeAddress("localhost", nodeA.server().port()));

            try (PartitionedForgeClient router = new PartitionedForgeClient(sameTopology)) {
                PartitionRebalancer.MigrationResult result =
                        PartitionRebalancer.migrate(a, nodeA.store(), sameTopology, router);
                assertTrue(result.keysMoved().isEmpty());
            }
            assertEquals(Set.of("k1", "k2"), nodeA.store().keys());
        }
    }

    @Test
    @Timeout(30)
    void migratedKeysAreRemovedFromSourceAndReadableFromDestination(@TempDir Path baseDir) throws Exception {
        NodeId a = new NodeId("a");
        NodeId b = new NodeId("b");
        ConsistentHashRing oldRing = ConsistentHashRing.of(Set.of(a)); // only "a" exists initially

        try (Node nodeA = start(a, oldRing, baseDir.resolve("a"))) {
            for (int i = 0; i < 100; i++) {
                nodeA.store().put("k" + i, bytes("v" + i));
            }
            assertEquals(100, nodeA.store().keys().size());

            ConsistentHashRing newRing = oldRing.withNode(b);
            // node-a's server must reflect the NEW ring to correctly reject keys it no longer owns.
            nodeA.server().close();
            try (ForgeServer refreshedA = new ForgeServer(nodeA.store(), key -> newRing.ownerOf(key).equals(a), 0);
                 Node nodeB = start(b, newRing, baseDir.resolve("b"))) {

                ClusterTopology newTopology = ClusterTopology.empty()
                        .withNode(a, new NodeAddress("localhost", refreshedA.port()))
                        .withNode(b, new NodeAddress("localhost", nodeB.server().port()));

                try (PartitionedForgeClient router = new PartitionedForgeClient(newTopology)) {
                    PartitionRebalancer.MigrationResult result =
                            PartitionRebalancer.migrate(a, nodeA.store(), newTopology, router);

                    assertTrue(!result.keysMoved().isEmpty(), "adding a node to a 100-key single-node dataset should move some keys");

                    for (String movedKey : result.keysMoved()) {
                        assertTrue(nodeA.store().get(movedKey).isEmpty(), movedKey + " must be gone from the source after moving");
                        assertEquals(b, newTopology.ownerOf(movedKey));
                    }

                    // Every original key is still readable through the new topology, moved or not.
                    for (int i = 0; i < 100; i++) {
                        String key = "k" + i;
                        var result2 = router.get(key);
                        assertTrue(result2.isPresent(), key + " must survive rebalancing");
                        assertArrayEquals(bytes("v" + i), result2.get());
                    }
                }
            }
        }
    }
}
