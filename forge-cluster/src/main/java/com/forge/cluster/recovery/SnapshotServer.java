package com.forge.cluster.recovery;

import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs on a node willing to bootstrap other nodes: on each connection, sends
 * a full point-in-time(-ish) snapshot of its {@link ConcurrentLsmKeyValueStore}
 * — Phase 10's "snapshot transfer" for a node too far behind for Phase 9's
 * WAL-based {@code ReplicationFollower} catch-up alone.
 *
 * <h2>Consistency of the snapshot, stated precisely</h2>
 * The watermark is captured first ({@code lastAppliedSequenceNumber() - 1}),
 * then {@code keys()} and each key's value are read via the store's already
 * thread-safe, already-reviewed methods — concurrent writes are not
 * blocked. This means a key written or deleted <em>during</em> the transfer
 * can, in rare cases, be included in the snapshot even though its sequence
 * number is past the captured watermark, or be skipped if it was deleted
 * between the key list being read and its value being fetched. Neither
 * case corrupts anything: {@code SnapshotClient}'s caller always follows a
 * snapshot with a {@code ReplicationFollower} starting exactly at
 * {@code watermark + 1}, which replays every operation from that point
 * forward — so a value the snapshot happened to catch early is simply
 * re-applied (harmless; same value) and anything the snapshot missed
 * because it was deleted is corrected as soon as that delete's own record
 * streams past. The destination's final state converges to correct either
 * way; see {@code SnapshotTransferIntegrationTest} for a test that bootstraps
 * while writes are concurrently happening, verifying exactly that convergence.
 */
public final class SnapshotServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SnapshotServer.class);
    /** Package-visible: {@code SnapshotClient} reads for exactly this value to know the stream is complete. */
    static final int END_OF_SNAPSHOT_MARKER = -1;

    private final ConcurrentLsmKeyValueStore store;
    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private final Thread acceptThread;
    private final Set<Socket> activeConnections = ConcurrentHashMap.newKeySet();
    private volatile boolean closed = false;

    public SnapshotServer(ConcurrentLsmKeyValueStore store, int port) throws IOException {
        this.store = store;
        this.serverSocket = new ServerSocket(port);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.acceptThread = Thread.ofPlatform().name("snapshot-server-acceptor").start(this::acceptLoop);
    }

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
                    return;
                }
                log.warn("accept() failed on snapshot server; continuing", e);
                continue;
            }
            if (closed) {
                closeQuietly(socket);
                return;
            }
            activeConnections.add(socket);
            executor.execute(() -> {
                try {
                    sendSnapshot(socket);
                } finally {
                    activeConnections.remove(socket);
                    closeQuietly(socket);
                }
            });
        }
    }

    private void sendSnapshot(Socket socket) {
        try {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            long watermark = store.lastAppliedSequenceNumber() - 1;
            out.writeLong(watermark);

            Set<String> keys = store.keys();
            int sent = 0;
            for (String key : keys) {
                Optional<byte[]> value = store.get(key);
                if (value.isEmpty()) {
                    continue; // deleted concurrently between the key list and this read -- see class Javadoc
                }
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeInt(value.get().length);
                out.write(value.get());
                sent++;
            }
            out.writeInt(END_OF_SNAPSHOT_MARKER);
            out.flush();
            log.info("sent a {}-key snapshot at watermark {}", sent, watermark);
        } catch (IOException e) {
            log.warn("failed to send snapshot to a connecting client", e);
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
                log.warn("snapshot server connection executor did not terminate within the shutdown grace period");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("error closing snapshot client socket during shutdown", e);
        }
    }
}
