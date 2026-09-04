package com.forge.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A consistent-hashing ring with virtual nodes, per ARCHITECTURE.md §3.6:
 * minimizes key movement on membership change relative to plain
 * {@code hash(key) % N}, because only the keys between two adjacent ring
 * positions move when a node joins or leaves, not the whole keyspace.
 *
 * <p>Positions are 64-bit values derived from SHA-256 (the first 8 bytes,
 * big-endian) of a placement string — {@code "<nodeId>#<vnodeIndex>"} for a
 * virtual node, or the raw key for a lookup — not {@link Object#hashCode()},
 * which the JDK does not guarantee stability for across versions the way a
 * fixed cryptographic digest is. Every physical node gets the same number of
 * virtual nodes, which is what "configurable number of partitions" means
 * here: more virtual nodes per physical node means finer-grained,
 * better-distributed ring coverage, not a separate fixed-partition-count
 * scheme (ARCHITECTURE.md specifically chose ring-based consistent hashing
 * over a fixed-N-partitions model).
 *
 * <p>Immutable: {@link #withNode} / {@link #withoutNode} return a new ring
 * rather than mutating this one, so a {@link ClusterTopology} can hand out
 * stable snapshots.
 */
public final class ConsistentHashRing {

    /** Reasonable default: enough virtual nodes for good distribution without an excessive ring size. */
    public static final int DEFAULT_VIRTUAL_NODES_PER_NODE = 128;

    private final int virtualNodesPerNode;
    private final TreeMap<Long, NodeId> ring;
    private final Set<NodeId> nodes;

    private ConsistentHashRing(int virtualNodesPerNode, TreeMap<Long, NodeId> ring, Set<NodeId> nodes) {
        this.virtualNodesPerNode = virtualNodesPerNode;
        this.ring = ring;
        this.nodes = nodes;
    }

    public static ConsistentHashRing empty() {
        return empty(DEFAULT_VIRTUAL_NODES_PER_NODE);
    }

    public static ConsistentHashRing empty(int virtualNodesPerNode) {
        if (virtualNodesPerNode <= 0) {
            throw new IllegalArgumentException("virtualNodesPerNode must be positive");
        }
        return new ConsistentHashRing(virtualNodesPerNode, new TreeMap<>(), Set.of());
    }

    public static ConsistentHashRing of(Set<NodeId> initialNodes) {
        return of(initialNodes, DEFAULT_VIRTUAL_NODES_PER_NODE);
    }

    public static ConsistentHashRing of(Set<NodeId> initialNodes, int virtualNodesPerNode) {
        ConsistentHashRing result = empty(virtualNodesPerNode);
        for (NodeId node : initialNodes) {
            result = result.withNode(node);
        }
        return result;
    }

    /** Returns a new ring with {@code node}'s virtual nodes added. A no-op (returns {@code this}) if already present. */
    public ConsistentHashRing withNode(NodeId node) {
        Objects.requireNonNull(node, "node must not be null");
        if (nodes.contains(node)) {
            return this;
        }
        TreeMap<Long, NodeId> updated = new TreeMap<>(ring);
        for (int v = 0; v < virtualNodesPerNode; v++) {
            updated.put(hashOf(node.value() + "#" + v), node);
        }
        Set<NodeId> updatedNodes = new LinkedHashSet<>(nodes);
        updatedNodes.add(node);
        return new ConsistentHashRing(virtualNodesPerNode, updated, Set.copyOf(updatedNodes));
    }

    /** Returns a new ring with {@code node}'s virtual nodes removed. A no-op (returns {@code this}) if absent. */
    public ConsistentHashRing withoutNode(NodeId node) {
        Objects.requireNonNull(node, "node must not be null");
        if (!nodes.contains(node)) {
            return this;
        }
        TreeMap<Long, NodeId> updated = new TreeMap<>(ring);
        for (int v = 0; v < virtualNodesPerNode; v++) {
            updated.remove(hashOf(node.value() + "#" + v));
        }
        Set<NodeId> updatedNodes = new LinkedHashSet<>(nodes);
        updatedNodes.remove(node);
        return new ConsistentHashRing(virtualNodesPerNode, updated, Set.copyOf(updatedNodes));
    }

    /**
     * The node responsible for {@code key}: the first virtual node at or
     * after {@code key}'s ring position, wrapping around to the ring's
     * first entry if {@code key} hashes past every virtual node.
     *
     * @throws IllegalStateException if the ring has no nodes at all
     */
    public NodeId ownerOf(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (ring.isEmpty()) {
            throw new IllegalStateException("cannot route a key: the ring has no nodes");
        }
        long position = hashOf(key);
        Map.Entry<Long, NodeId> entry = ring.ceilingEntry(position);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    public Set<NodeId> nodes() {
        return nodes;
    }

    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public int virtualNodesPerNode() {
        return virtualNodesPerNode;
    }

    private static long hashOf(String input) {
        byte[] digest = sha256(input);
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << 8) | (digest[i] & 0xFF);
        }
        return value;
    }

    private static byte[] sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(input.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a mandatory JDK algorithm (Java Cryptography Architecture Standard
            // Algorithm Name spec) — this can only happen on a broken JVM installation.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
