package com.forge.cluster.replication;

import com.forge.cluster.NodeId;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import com.forge.storage.wal.ReplicationOutcome;
import com.forge.storage.wal.WalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs on a replication follower: connects to one leader's
 * {@link ReplicationServer}, requests catch-up from exactly where this
 * node's own store has gotten to, applies every record it receives (in
 * order, via {@link ConcurrentLsmKeyValueStore#applyReplicated}), and
 * periodically acks its progress back so the leader can track lag.
 *
 * <h2>Durability guarantee this actually provides, stated precisely</h2>
 * This is <b>asynchronous replication</b> (DESIGN.md §2's default): the
 * leader acks a client's write as soon as its own WAL fsync completes,
 * before any follower has necessarily seen it. A write acknowledged to a
 * client can therefore be lost if the leader crashes before shipping it to
 * any follower — this class does not, and cannot by itself, change that.
 * Synchronous replication (leader waits for ≥1 follower ack before
 * acknowledging the client) is not implemented — doing so would require
 * changing {@code ConcurrentLsmKeyValueStore.put}/{@code delete}'s own ack
 * timing, a Phase-4-reviewed code path this phase does not touch. Recorded
 * here as a scope boundary, not glossed over.
 *
 * <h2>What happens on a detected gap</h2>
 * {@link ReplicationOutcome#GAP_DETECTED} should never actually happen given
 * {@link ReplicationServer}'s catch-up design (it always starts exactly at
 * this follower's requested sequence number) — if it does anyway (e.g. a
 * bug, or a future protocol change), this follower treats it as a fatal
 * error for the connection: it stops applying and closes, rather than ever
 * silently accepting a gap. A caller wanting resilience would reconnect
 * (creating a fresh {@code ReplicationFollower}, which re-requests catch-up
 * from this store's current, still-correct {@code lastAppliedSequenceNumber()}).
 */
public final class ReplicationFollower implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicationFollower.class);

    private final NodeId selfId;
    private final ConcurrentLsmKeyValueStore store;
    private final Socket socket;
    private final DataOutputStream ackOut;
    private final Thread applyThread;
    private final ScheduledExecutorService ackScheduler;
    private volatile boolean closed = false;
    private volatile boolean gapDetected = false;

    public ReplicationFollower(NodeId selfId, String leaderHost, int leaderPort, ConcurrentLsmKeyValueStore store,
            Duration ackInterval) throws IOException {
        this.selfId = Objects.requireNonNull(selfId, "selfId must not be null");
        this.store = Objects.requireNonNull(store, "store must not be null");
        Objects.requireNonNull(ackInterval, "ackInterval must not be null");
        if (ackInterval.isNegative() || ackInterval.isZero()) {
            throw new IllegalArgumentException("ackInterval must be positive");
        }

        this.socket = new Socket(leaderHost, leaderPort);
        DataInputStream recordIn;
        try {
            this.ackOut = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            recordIn = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            byte[] idBytes = selfId.value().getBytes(StandardCharsets.UTF_8);
            ackOut.writeInt(idBytes.length);
            ackOut.write(idBytes);
            ackOut.writeLong(store.lastAppliedSequenceNumber());
            ackOut.flush();
        } catch (IOException e) {
            closeQuietly(socket);
            throw e;
        }

        this.applyThread = Thread.ofPlatform().name("replication-follower-apply-" + selfId)
                .start(() -> applyLoop(recordIn));
        this.ackScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().name("replication-follower-ack-" + selfId).unstarted(r));
        long intervalMillis = ackInterval.toMillis();
        ackScheduler.scheduleAtFixedRate(this::sendAck, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /** True if this follower stopped applying because the leader's stream would have created a sequence gap. */
    public boolean gapDetected() {
        return gapDetected;
    }

    private void applyLoop(DataInputStream in) {
        try {
            while (!closed) {
                WalRecord record = ReplicationWireFormat.readRecord(in);
                ReplicationOutcome outcome = store.applyReplicated(record);
                if (outcome == ReplicationOutcome.GAP_DETECTED) {
                    log.error("{}: replication stream produced a sequence gap at {} (expected {}); "
                                    + "this should be unreachable given ReplicationServer's catch-up design — "
                                    + "closing the connection rather than risk a silent inconsistency",
                            selfId, record.sequenceNumber(), store.lastAppliedSequenceNumber());
                    gapDetected = true;
                    return;
                }
            }
        } catch (EOFException e) {
            log.debug("{}: replication stream from leader ended", selfId);
        } catch (IOException e) {
            if (!closed) {
                log.warn("{}: replication stream from leader failed", selfId, e);
            }
        }
    }

    private void sendAck() {
        try {
            ackOut.writeLong(store.lastAppliedSequenceNumber());
            ackOut.flush();
        } catch (IOException e) {
            log.debug("{}: failed to send replication ack", selfId, e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeQuietly(socket);
        ackScheduler.shutdownNow();
        try {
            applyThread.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            if (!ackScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("{}: ack scheduler did not terminate within the shutdown grace period", selfId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("error closing replication socket during shutdown", e);
        }
    }
}
