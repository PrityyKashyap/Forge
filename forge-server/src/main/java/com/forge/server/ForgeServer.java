package com.forge.server;

import com.forge.common.protocol.ProtocolConstants;
import com.forge.storage.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * A FORGE TCP server for a single node: accepts connections and serves each
 * one on its own virtual thread, on top of a {@link KeyValueStore}.
 *
 * <p>Chosen connection model: one virtual thread per connection running
 * ordinary blocking I/O ({@link ConnectionHandler}), backed by
 * {@link Executors#newVirtualThreadPerTaskExecutor()}. This is a Java
 * 21-baseline-specific choice, not a general recommendation — it gets
 * thread-per-connection's simple, sequential-looking code (no callback or
 * event-loop machinery) without thread-per-connection's traditional cost,
 * because virtual threads are cheap enough that one per connection is fine
 * even at large connection counts. The caveat worth knowing: a virtual
 * thread that runs a JDK-monitor {@code synchronized} block which then
 * blocks (as {@code WriteAheadLog}'s {@code synchronized} methods do while
 * inside {@code force()}) pins its carrier platform thread for that
 * duration, rather than yielding it the way a virtual-thread-friendly lock
 * would. This is a known, accepted tradeoff for this phase, not an oversight.
 *
 * <p>The constructor binds the listening socket and starts serving
 * immediately, mirroring {@link ServerSocket}'s own eager-bind constructor;
 * there is no separate {@code start()}. Instances are meant to be used with
 * try-with-resources or an explicit {@link #close()}.
 *
 * <h2>Shutdown is best-effort against brand-new connections</h2>
 * {@link #close()} closes every socket it knows about and stops the accept
 * loop, but a connection whose TCP handshake completed in the OS backlog
 * queue microseconds before {@code close()} ran can still be handed back by
 * {@code accept()} afterward; the accept loop notices {@link #closed} and
 * discards that straggler immediately rather than serving or leaking it.
 * There's no way to close that residual window entirely at this layer — the
 * OS accept queue is outside this class's control — so this is a deliberate,
 * accepted limitation of graceful shutdown, not an oversight.
 */
public final class ForgeServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ForgeServer.class);

    private final KeyValueStore store;
    private final Predicate<String> ownershipPredicate;
    private final int maxFrameLength;
    private final int maxKeyLength;
    private final ServerSocket serverSocket;
    private final ExecutorService connectionExecutor;
    private final Thread acceptThread;

    /** Sockets for connections currently in flight, so {@link #close()} can force them shut. */
    private final Set<Socket> activeConnections = ConcurrentHashMap.newKeySet();

    private volatile boolean closed = false;

    public ForgeServer(KeyValueStore store, int port) throws IOException {
        this(store, port, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH, ProtocolConstants.DEFAULT_MAX_KEY_LENGTH);
    }

    public ForgeServer(KeyValueStore store, int port, int maxFrameLength, int maxKeyLength) throws IOException {
        this(store, key -> true, port, maxFrameLength, maxKeyLength);
    }

    /**
     * Phase 7 addition: {@code ownershipPredicate} lets a node reject
     * (with {@code ERROR_NOT_OWNER}, never touching {@code store}) any
     * request for a key it doesn't currently own under its partition map —
     * DESIGN.md's Phase 7 contract calls this "fail closed, not open."
     * Purely additive: every pre-Phase-7 constructor delegates here with
     * {@code key -> true}, so a caller that never heard of partitioning
     * gets byte-for-byte the same behavior as before this parameter existed.
     */
    public ForgeServer(KeyValueStore store, Predicate<String> ownershipPredicate, int port) throws IOException {
        this(store, ownershipPredicate, port, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH, ProtocolConstants.DEFAULT_MAX_KEY_LENGTH);
    }

    public ForgeServer(KeyValueStore store, Predicate<String> ownershipPredicate, int port, int maxFrameLength,
            int maxKeyLength) throws IOException {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.ownershipPredicate = Objects.requireNonNull(ownershipPredicate, "ownershipPredicate must not be null");
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be positive");
        }
        if (maxKeyLength <= 0) {
            throw new IllegalArgumentException("maxKeyLength must be positive");
        }
        this.maxFrameLength = maxFrameLength;
        this.maxKeyLength = maxKeyLength;

        this.serverSocket = new ServerSocket(port);
        this.connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.acceptThread = Thread.ofPlatform().name("forge-server-acceptor").start(this::acceptLoop);
    }

    /** The port actually bound — useful when constructed with port 0 (an ephemeral port), as tests do. */
    public int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (closed) {
                    return; // expected: close() closed serverSocket to unblock this accept()
                }
                log.warn("accept() failed; continuing to serve other connections", e);
                continue;
            }
            if (closed) {
                // close() may have already run its cleanup loop over activeConnections
                // by the time this connection, accepted just before closed flipped
                // true, gets here; registering it now would leak it, since neither
                // that loop nor connectionExecutor (already shut down) will touch it.
                closeQuietly(socket);
                return;
            }
            activeConnections.add(socket);
            connectionExecutor.execute(() -> {
                try {
                    new ConnectionHandler(socket, store, ownershipPredicate, maxFrameLength, maxKeyLength).run();
                } finally {
                    activeConnections.remove(socket);
                    closeQuietly(socket);
                }
            });
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        serverSocket.close();

        for (Socket socket : activeConnections) {
            closeQuietly(socket);
        }

        try {
            acceptThread.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        connectionExecutor.shutdown();
        try {
            if (!connectionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("connection executor did not terminate within the shutdown grace period");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("error closing client socket during shutdown", e);
        }
    }
}
