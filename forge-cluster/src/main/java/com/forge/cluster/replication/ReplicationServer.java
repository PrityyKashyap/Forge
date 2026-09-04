package com.forge.cluster.replication;

import com.forge.cluster.NodeId;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import com.forge.storage.wal.WalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Runs on a replication leader: accepts follower connections, streams this
 * node's {@link ConcurrentLsmKeyValueStore} write stream to each of them
 * (catch-up from wherever the follower says it's gotten to, then live
 * tailing), and tracks each follower's acknowledged position for lag
 * reporting.
 *
 * <h2>Why a separate protocol from the client wire protocol</h2>
 * Same reasoning as Phase 8's heartbeats: {@code forge-common}'s
 * {@code Request}/{@code Response} are sealed, exhaustively-switched
 * client/data-plane types; replication is leader-to-follower log shipping,
 * a different kind of traffic with different semantics (a continuous
 * stream, not one request/one response), and forcing it through that
 * protocol would mean touching every exhaustive switch over it for a
 * message shape that doesn't belong there conceptually.
 *
 * <h2>Catch-up-then-live-tail handoff, and why it can't lose or duplicate records</h2>
 * For each new follower connection: (1) a listener is registered on the
 * store <em>first</em>, queuing every new record from this instant on; only
 * <em>then</em> (2) is the WAL's current content read and everything at or
 * past the follower's requested start sent. Registering the listener before
 * reading the backlog guarantees nothing written in between is missed — it
 * lands in the backlog scan, the live queue, or (under a race) both. The
 * "both" case is handled explicitly: the live-queue drain loop tracks the
 * highest sequence number actually sent and skips anything at or below it,
 * so the same record is never sent twice. See {@code ReplicationServerTest}
 * for a test that writes concurrently with a follower actively catching up.
 *
 * <h2>Scope boundary</h2>
 * Catch-up only covers whatever is currently in the leader's WAL —
 * {@link ConcurrentLsmKeyValueStore#currentWalRecords()}, which never
 * includes anything already flushed to an SSTable. A follower behind
 * further than that needs a full bootstrap, which is Phase 10's job, not
 * this class's.
 */
public final class ReplicationServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicationServer.class);

    private final ConcurrentLsmKeyValueStore store;
    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private final Thread acceptThread;
    private final Set<Socket> activeConnections = ConcurrentHashMap.newKeySet();
    private final Map<NodeId, ReplicaState> replicaStates = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public ReplicationServer(ConcurrentLsmKeyValueStore store, int port) throws IOException {
        this.store = store;
        this.serverSocket = new ServerSocket(port);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.acceptThread = Thread.ofPlatform().name("replication-server-acceptor").start(this::acceptLoop);
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    /** A snapshot of every follower this leader currently knows about and how far behind it is. */
    public Map<NodeId, ReplicaState> replicaStates() {
        return Map.copyOf(replicaStates);
    }

    private void acceptLoop() {
        while (!closed) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (closed) {
                    return;
                }
                log.warn("accept() failed on replication server; continuing", e);
                continue;
            }
            if (closed) {
                closeQuietly(socket);
                return;
            }
            activeConnections.add(socket);
            executor.execute(() -> {
                try {
                    serveFollower(socket);
                } finally {
                    activeConnections.remove(socket);
                    closeQuietly(socket);
                }
            });
        }
    }

    private void serveFollower(Socket socket) {
        NodeId followerId;
        long startSeq;
        DataInputStream in;
        DataOutputStream out;
        try {
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            int idLength = in.readInt();
            if (idLength < 0 || idLength > 1024) {
                throw new IOException("invalid follower id length " + idLength);
            }
            byte[] idBytes = new byte[idLength];
            in.readFully(idBytes);
            followerId = new NodeId(new String(idBytes, StandardCharsets.UTF_8));
            startSeq = in.readLong();
        } catch (IOException e) {
            log.warn("failed to complete replication handshake with a connecting follower", e);
            return;
        }

        replicaStates.put(followerId, new ReplicaState(followerId, startSeq, Instant.now()));
        log.info("follower {} connected, requesting catch-up from sequence {}", followerId, startSeq);

        Thread ackReader = Thread.ofPlatform().name("replication-ack-reader-" + followerId)
                .start(() -> readAcks(followerId, in));

        BlockingQueue<WalRecord> liveQueue = new LinkedBlockingQueue<>();
        Consumer<WalRecord> listener = liveQueue::add;
        store.addReplicationListener(listener); // (1) register before reading the backlog -- see class Javadoc
        try {
            long lastSent = streamBacklog(out, startSeq); // (2)
            streamLiveTail(socket, out, liveQueue, lastSent);
        } catch (IOException e) {
            log.debug("replication connection to follower {} ended", followerId, e);
        } finally {
            store.removeReplicationListener(listener);
            // ackReader is unblocked by this connection's socket being closed by the
            // caller's finally block, which happens right after this method returns —
            // Thread.interrupt() does not unblock a plain Socket's blocking read.
        }
    }

    private long streamBacklog(DataOutputStream out, long startSeq) throws IOException {
        long lastSent = startSeq - 1;
        for (WalRecord record : store.currentWalRecords()) {
            if (record.sequenceNumber() >= startSeq) {
                ReplicationWireFormat.writeRecord(out, record);
                lastSent = record.sequenceNumber();
            }
        }
        out.flush();
        return lastSent;
    }

    private void streamLiveTail(Socket socket, DataOutputStream out, BlockingQueue<WalRecord> liveQueue, long lastSentInitial)
            throws IOException {
        long lastSent = lastSentInitial;
        while (!closed && !socket.isClosed()) {
            WalRecord record;
            try {
                record = liveQueue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (record == null) {
                continue; // just a periodic wake to re-check closed/socket state
            }
            if (record.sequenceNumber() > lastSent) {
                ReplicationWireFormat.writeRecord(out, record);
                out.flush();
                lastSent = record.sequenceNumber();
            }
            // else: already covered by the backlog scan (the race described in the class Javadoc) -- skip
        }
    }

    private void readAcks(NodeId followerId, DataInputStream in) {
        try {
            while (true) {
                long acknowledged = in.readLong();
                replicaStates.put(followerId, new ReplicaState(followerId, acknowledged, Instant.now()));
            }
        } catch (IOException e) {
            log.debug("ack stream from follower {} ended", followerId, e);
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

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("replication server connection executor did not terminate within the shutdown grace period");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("error closing follower socket during shutdown", e);
        }
    }
}
