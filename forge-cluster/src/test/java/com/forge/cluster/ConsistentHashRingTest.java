package com.forge.cluster;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashRingTest {

    private static final NodeId A = new NodeId("node-a");
    private static final NodeId B = new NodeId("node-b");
    private static final NodeId C = new NodeId("node-c");

    @Test
    void emptyRingRejectsRouting() {
        ConsistentHashRing ring = ConsistentHashRing.empty();
        assertThrows(IllegalStateException.class, () -> ring.ownerOf("any-key"));
    }

    @Test
    void sameKeySameTopologyAlwaysRoutesToTheSameNode() {
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(A, B, C));
        NodeId first = ring.ownerOf("stable-key");
        for (int i = 0; i < 100; i++) {
            assertEquals(first, ring.ownerOf("stable-key"), "routing must be a pure, deterministic function of the ring");
        }
    }

    @Test
    void ringIsRebuiltIdenticallyFromTheSameNodeSetEveryTime() {
        ConsistentHashRing ringOne = ConsistentHashRing.of(Set.of(A, B, C));
        ConsistentHashRing ringTwo = ConsistentHashRing.of(Set.of(C, A, B)); // different insertion order

        for (int i = 0; i < 50; i++) {
            String key = "key-" + i;
            assertEquals(ringOne.ownerOf(key), ringTwo.ownerOf(key),
                    "the owner of a key must not depend on the order nodes were added in");
        }
    }

    @Test
    void singleNodeOwnsEveryKey() {
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(A));
        for (int i = 0; i < 20; i++) {
            assertEquals(A, ring.ownerOf("key-" + i));
        }
    }

    @Test
    void keysDistributeAcrossAllNodesReasonably() {
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(A, B, C));
        Map<NodeId, Integer> counts = new HashMap<>();
        int totalKeys = 3000;
        for (int i = 0; i < totalKeys; i++) {
            counts.merge(ring.ownerOf("distribution-key-" + i), 1, Integer::sum);
        }

        assertEquals(Set.of(A, B, C), counts.keySet(), "every node must receive at least one key");
        // With 128 virtual nodes per physical node and 3 nodes, distribution should be
        // roughly even — allow a generous 2x-of-fair-share band to avoid a flaky test.
        int fairShare = totalKeys / 3;
        for (int count : counts.values()) {
            assertTrue(count > fairShare / 2 && count < fairShare * 2,
                    "distribution too skewed: counts=" + counts);
        }
    }

    @Test
    void addingANodeOnlyMovesSomeKeysNotAllOfThem() {
        ConsistentHashRing before = ConsistentHashRing.of(Set.of(A, B));
        ConsistentHashRing after = before.withNode(C);

        int totalKeys = 2000;
        int moved = 0;
        for (int i = 0; i < totalKeys; i++) {
            String key = "movement-key-" + i;
            if (!before.ownerOf(key).equals(after.ownerOf(key))) {
                moved++;
            }
        }

        // Consistent hashing's whole point: adding a 3rd node to a 2-node ring should move
        // roughly 1/3 of keys (to the new node), not anywhere near all of them.
        assertTrue(moved > 0, "adding a node must move at least some keys to it");
        assertTrue(moved < totalKeys * 3 / 4,
                "adding one node moved " + moved + "/" + totalKeys + " keys — consistent hashing should move far less than that");
    }

    @Test
    void removingANodeReassignsOnlyThatNodesKeys() {
        ConsistentHashRing before = ConsistentHashRing.of(Set.of(A, B, C));
        ConsistentHashRing after = before.withoutNode(C);

        int totalKeys = 2000;
        for (int i = 0; i < totalKeys; i++) {
            String key = "removal-key-" + i;
            NodeId ownerBefore = before.ownerOf(key);
            NodeId ownerAfter = after.ownerOf(key);
            if (!ownerBefore.equals(C)) {
                assertEquals(ownerBefore, ownerAfter,
                        "a key not owned by the removed node must keep its owner");
            } else {
                assertTrue(after.nodes().contains(ownerAfter), "a reassigned key's new owner must be a surviving node");
            }
        }
    }

    @Test
    void withNodeIsIdempotentForAnAlreadyPresentNode() {
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(A, B));
        ConsistentHashRing same = ring.withNode(A);
        assertEquals(ring.nodes(), same.nodes());
        for (int i = 0; i < 20; i++) {
            assertEquals(ring.ownerOf("k" + i), same.ownerOf("k" + i));
        }
    }

    @Test
    void withoutNodeIsANoOpForAnAbsentNode() {
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(A, B));
        ConsistentHashRing same = ring.withoutNode(C);
        assertEquals(ring.nodes(), same.nodes());
    }

    @Test
    void removingTheLastNodeProducesAnEmptyRing() {
        ConsistentHashRing ring = ConsistentHashRing.of(Set.of(A)).withoutNode(A);
        assertTrue(ring.isEmpty());
        assertThrows(IllegalStateException.class, () -> ring.ownerOf("any"));
    }

    @Test
    void moreVirtualNodesProducesFinerGrainedRingWithoutChangingCorrectness() {
        ConsistentHashRing coarse = ConsistentHashRing.of(Set.of(A, B, C), 4);
        ConsistentHashRing fine = ConsistentHashRing.of(Set.of(A, B, C), 256);

        // Both must still deterministically route every key to a real node.
        for (int i = 0; i < 100; i++) {
            String key = "vnode-key-" + i;
            assertTrue(coarse.nodes().contains(coarse.ownerOf(key)));
            assertTrue(fine.nodes().contains(fine.ownerOf(key)));
        }
    }

    @Test
    void rejectsNonPositiveVirtualNodeCount() {
        assertThrows(IllegalArgumentException.class, () -> ConsistentHashRing.empty(0));
        assertThrows(IllegalArgumentException.class, () -> ConsistentHashRing.empty(-1));
    }
}
