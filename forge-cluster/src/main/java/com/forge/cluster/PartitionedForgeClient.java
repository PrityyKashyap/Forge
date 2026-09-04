package com.forge.cluster;

import com.forge.client.ForgeClient;
import com.forge.client.ForgeServerException;
import com.forge.common.protocol.ProtocolConstants;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * or topology refresh for ownership changes. See PROGRESS.md's Phase 7
 * section.
 *
 * <h2>Phase 15: one automatic retry on {@code ERROR_NOT_LEADER}</h2>
 * A PUT/DELETE (never a GET — {@code ERROR_NOT_LEADER} is only ever
 * returned for a write, see {@code docs/CONSISTENCY.md} §5) rejected with
 * {@code ERROR_NOT_LEADER} carries a documented {@code "...leader=<id>"}
 * hint in its message (see {@code ConnectionHandler.notLeaderMessage}).
 * If that hint names a node this client's topology actually knows the
 * address of, and it isn't the same node that just rejected the request,
 * this client retries the <em>exact same request</em> against that node
 * exactly once (no chained redirects, to bound worst-case latency and
 * avoid a routing loop against a stale/oscillating hint). This is safe to
 * retry blindly: {@code ConnectionHandler} checks write authority
 * <em>before</em> touching the store, so a rejected write provably never
 * applied anywhere, and PUT/DELETE are themselves idempotent (a repeated
 * PUT of the same key/value or a repeated DELETE of the same key produces
 * the same end state) — see {@code docs/CONSISTENCY.md} §6 for the fuller
 * argument, including the (pre-existing, not Phase-15-specific) case of a
 * client-side timeout of unknown outcome. If no useful redirect target is
 * available, the original {@code ForgeServerException} is thrown unchanged.
 */
public final class PartitionedForgeClient implements Closeable {

    private static final Pattern NOT_LEADER_HINT = Pattern.compile("leader=(\\S+)");

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
        withLeaderRedirect(key, connection -> {
            connection.put(key, value);
            return null;
        });
    }

    public Optional<byte[]> get(String key) throws IOException {
        ForgeClient connection = connectionFor(topology.ownerOf(key));
        synchronized (connection) {
            return connection.get(key);
        }
    }

    public void delete(String key) throws IOException {
        withLeaderRedirect(key, connection -> {
            connection.delete(key);
            return null;
        });
    }

    private interface ClientOp<T> {
        T apply(ForgeClient connection) throws IOException;
    }

    private <T> T withLeaderRedirect(String key, ClientOp<T> op) throws IOException {
        NodeId owner = topology.ownerOf(key);
        ForgeClient connection = connectionFor(owner);
        try {
            synchronized (connection) {
                return op.apply(connection);
            }
        } catch (ForgeServerException e) {
            if (e.errorCode() != ProtocolConstants.ERROR_NOT_LEADER) {
                throw e;
            }
            Optional<NodeId> hinted = parseLeaderHint(e.getMessage());
            if (hinted.isEmpty() || hinted.get().equals(owner) || topology.addressOf(hinted.get()).isEmpty()) {
                throw e; // no usable redirect target — surface the original rejection
            }
            ForgeClient redirected = connectionFor(hinted.get());
            synchronized (redirected) {
                return op.apply(redirected);
            }
        }
    }

    /** Parses the {@code "...leader=<id>"} hint from an {@code ERROR_NOT_LEADER} message — see {@code ConnectionHandler.notLeaderMessage}. */
    static Optional<NodeId> parseLeaderHint(String message) {
        if (message == null) {
            return Optional.empty();
        }
        Matcher matcher = NOT_LEADER_HINT.matcher(message);
        if (!matcher.find()) {
            return Optional.empty();
        }
        String id = matcher.group(1);
        if (id.equals("none")) {
            return Optional.empty();
        }
        return Optional.of(new NodeId(id));
    }

    private ForgeClient connectionFor(NodeId owner) throws IOException {
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
