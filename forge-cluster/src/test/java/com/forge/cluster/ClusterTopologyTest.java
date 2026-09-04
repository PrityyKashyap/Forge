package com.forge.cluster;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterTopologyTest {

    private static final NodeId A = new NodeId("node-a");
    private static final NodeId B = new NodeId("node-b");
    private static final NodeAddress ADDR_A = new NodeAddress("localhost", 7001);
    private static final NodeAddress ADDR_B = new NodeAddress("localhost", 7002);

    @Test
    void emptyTopologyHasNoNodesAndRejectsRouting() {
        ClusterTopology topology = ClusterTopology.empty();
        assertTrue(topology.isEmpty());
        assertThrows(IllegalStateException.class, () -> topology.ownerOf("k"));
    }

    @Test
    void withNodeAddsBothRingMembershipAndAddress() {
        ClusterTopology topology = ClusterTopology.empty().withNode(A, ADDR_A);
        assertEquals(Set.of(A), topology.nodes());
        assertEquals(ADDR_A, topology.addressOf(A).orElseThrow());
        assertEquals(A, topology.ownerOf("any-key"));
    }

    @Test
    void withoutNodeRemovesBothRingMembershipAndAddress() {
        ClusterTopology topology = ClusterTopology.empty().withNode(A, ADDR_A).withNode(B, ADDR_B).withoutNode(A);
        assertEquals(Set.of(B), topology.nodes());
        assertTrue(topology.addressOf(A).isEmpty());
        assertEquals(B, topology.ownerOf("any-key"));
    }

    @Test
    void withNodeReplacesAnExistingNodesAddress() {
        NodeAddress movedAddress = new NodeAddress("localhost", 9999);
        ClusterTopology topology = ClusterTopology.empty().withNode(A, ADDR_A).withNode(A, movedAddress);
        assertEquals(movedAddress, topology.addressOf(A).orElseThrow());
        assertEquals(Set.of(A), topology.nodes());
    }

    @Test
    void originalTopologyIsNeverMutatedByWithNodeOrWithoutNode() {
        ClusterTopology original = ClusterTopology.empty().withNode(A, ADDR_A);
        ClusterTopology withB = original.withNode(B, ADDR_B);
        ClusterTopology withoutA = original.withoutNode(A);

        assertEquals(Set.of(A), original.nodes(), "the original topology must be unaffected by deriving a new one");
        assertEquals(Set.of(A, B), withB.nodes());
        assertTrue(withoutA.isEmpty());
    }

    @Test
    void addressOfAnUnknownNodeIsEmpty() {
        ClusterTopology topology = ClusterTopology.empty().withNode(A, ADDR_A);
        assertTrue(topology.addressOf(B).isEmpty());
    }
}
