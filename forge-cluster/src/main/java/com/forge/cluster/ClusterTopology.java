package com.forge.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A complete, immutable snapshot of "which node owns which keys, and where
 * is each node reachable": a {@link ConsistentHashRing} plus each node's
 * {@link NodeAddress}. This is the "partition map" ARCHITECTURE.md §3.6
 * says has to live somewhere every participant can see — for this phase,
 * every participant (client and, via the ownership check wired into
 * {@code ForgeServer}, every node) holds an explicit copy, rather than a
 * gossip-distributed one; see PROGRESS.md's Phase 7 section for why that's
 * an honest, deliberate scope boundary and not an oversight.
 *
 * <p>Per DESIGN.md's Phase 7 contract: in a converged (non-rebalancing)
 * state, {@link #ownerOf} maps every key to exactly one node, deterministically
 * and reproducibly — the same key under the same topology always returns the
 * same owner, from any caller, forever (it's a pure function of the ring).
 */
public final class ClusterTopology {

    private final ConsistentHashRing ring;
    private final Map<NodeId, NodeAddress> addresses;

    private ClusterTopology(ConsistentHashRing ring, Map<NodeId, NodeAddress> addresses) {
        this.ring = ring;
        this.addresses = addresses;
    }

    public static ClusterTopology empty() {
        return new ClusterTopology(ConsistentHashRing.empty(), Map.of());
    }

    public static ClusterTopology empty(int virtualNodesPerNode) {
        return new ClusterTopology(ConsistentHashRing.empty(virtualNodesPerNode), Map.of());
    }

    /** Returns a new topology with {@code node} added at {@code address}. Replaces the address if already present. */
    public ClusterTopology withNode(NodeId node, NodeAddress address) {
        Objects.requireNonNull(node, "node must not be null");
        Objects.requireNonNull(address, "address must not be null");
        Map<NodeId, NodeAddress> updated = new HashMap<>(addresses);
        updated.put(node, address);
        return new ClusterTopology(ring.withNode(node), Map.copyOf(updated));
    }

    /** Returns a new topology with {@code node} removed. A no-op if absent. */
    public ClusterTopology withoutNode(NodeId node) {
        Objects.requireNonNull(node, "node must not be null");
        if (!addresses.containsKey(node)) {
            return this;
        }
        Map<NodeId, NodeAddress> updated = new HashMap<>(addresses);
        updated.remove(node);
        return new ClusterTopology(ring.withoutNode(node), Map.copyOf(updated));
    }

    /** @throws IllegalStateException if the topology has no nodes */
    public NodeId ownerOf(String key) {
        return ring.ownerOf(key);
    }

    public Optional<NodeAddress> addressOf(NodeId node) {
        return Optional.ofNullable(addresses.get(node));
    }

    public Set<NodeId> nodes() {
        return ring.nodes();
    }

    public boolean isEmpty() {
        return ring.isEmpty();
    }

    public ConsistentHashRing ring() {
        return ring;
    }
}
