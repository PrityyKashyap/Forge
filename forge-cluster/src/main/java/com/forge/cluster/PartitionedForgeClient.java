package com.forge.cluster;

import com.forge.client.ForgeClient;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cluster-aware client: given a fixed {@link ClusterTopology}, routes each
 * GET/PUT/DELETE to whichever node's {@link ForgeClient} connection owns
 * that key, per ARCHITECTURE.md §3.5 ("once partitioning exists, [the
 * client] knows the partition map so it can route a request to the right
 * node without guessing").
 *
 * <p>Connections are opened lazily, one per node, the first time that node
 * is actually addressed — not one per node up front, so a topology listing
 * nodes this client never happens to route to never pays a connection cost.
 * Connection setup for a given node happens at most once even if several
 * threads race to be the first to address it.
 *
 * <h2>Concurrency: one request in flight per node at a time</h2>
 * {@link ForgeClient} is explicitly not thread-safe — one request in flight
 * per connection, by design (see its own class Javadoc). Since this class
 * holds exactly one {@code ForgeClient} per node and multiple threads can
 * legitimately route to the same node concurrently, every use of a node's
 * connection is serialized here with a lock on that connection object. This
 * was not a hypothetical concern: an earlier version without it let two
 * threads' PUT/GET calls interleave their bytes on the same socket, and a
 * concurrency test written for exactly this scenario caught it immediately
 * (a corrupted frame, surfaced as a nonsensical "invalid key length" error)
 * — see PROGRESS.md's Phase 7 section. The tradeoff is real and worth
 * naming: concurrent callers targeting the <em>same</em> node queue up
 * behind each other rather than running in parallel; callers targeting
 * <em>different</em> nodes are unaffected, since each node has its own lock.
 * A connection pool per node would remove that queuing at the cost of real
 * added complexity — not built here because nothing in this phase's own
 * requirements demonstrated a need for it.
 *
 * <p>Does not itself detect or react to a stale topology — if the cluster's
 * real ownership has since changed (a node was added/removed after this
 * client was built) and a request lands on a node that no longer owns the
 * key, that node's {@code ForgeServer} (if constructed with an ownership
 * predicate) rejects it with {@code ERROR_NOT_OWNER}, surfaced here as an
 * ordinary {@code ForgeServerException} — there is no automatic re-routing
 * or topology refresh in this phase. See PROGRESS.md's Phase 7 section.
 */
public final class PartitionedForgeClient implements Closeable {

    private final ClusterTopology topology;
    private final Map<NodeId, ForgeClient> connections = new ConcurrentHashMap<>();

    public PartitionedForgeClient(ClusterTopology topology) {
        this.topology = Objects.requireNonNull(topology, "topology must not be null");
        if (topology.isEmpty()) {
            throw new IllegalArgumentException("topology must have at least one node");
        }
    }

    /** Which node this client would route {@code key} to, without actually sending anything. */
    public NodeId ownerOf(String key) {
        return topology.ownerOf(key);
    }

    public void put(String key, byte[] value) throws IOException {
        ForgeClient connection = connectionFor(key);
        synchronized (connection) {
            connection.put(key, value);
        }
    }

    public Optional<byte[]> get(String key) throws IOException {
        ForgeClient connection = connectionFor(key);
        synchronized (connection) {
            return connection.get(key);
        }
    }

    public void delete(String key) throws IOException {
        ForgeClient connection = connectionFor(key);
        synchronized (connection) {
            connection.delete(key);
        }
    }

    private ForgeClient connectionFor(String key) throws IOException {
        NodeId owner = topology.ownerOf(key);
        try {
            return connections.computeIfAbsent(owner, node -> {
                NodeAddress address = topology.addressOf(node)
                        .orElseThrow(() -> new IllegalStateException("topology has no address for owning node " + node));
                try {
                    return ForgeClient.connect(address.host(), address.port());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (ForgeClient client : connections.values()) {
            try {
                client.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
