package com.forge.cluster.membership;

import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends and receives real UDP heartbeats between nodes, driving a
 * {@link FailureDetector} — this is the actual multi-node networking this
 * phase's "do not merely simulate membership inside one process" requirement
 * calls for, real datagrams over real sockets between real node processes.
 *
 * <h2>Why UDP, not the existing TCP wire protocol</h2>
 * {@code forge-common}'s {@code Request}/{@code Response} protocol is a
 * sealed, exhaustively-switched client/data-plane protocol (GET/PUT/DELETE);
 * adding a heartbeat variant to it would mean touching every exhaustive
 * switch over those sealed types across {@code FrameCodec},
 * {@code ConnectionHandler}, and {@code ForgeClient} for a message shape
 * that doesn't belong to that protocol conceptually. A heartbeat is
 * fire-and-forget, control-plane, and tolerates loss by design — UDP's
 * actual delivery semantics, not something layered on top of TCP's
 * connection and ordering guarantees that heartbeats don't need. A plain
 * {@link DatagramSocket} is standard JDK, no new dependency.
 *
 * <h2>Wire format</h2>
 * One datagram is one heartbeat: the sender's {@link NodeId} value, UTF-8,
 * with no framing needed — UDP already delivers whole messages, unlike the
 * TCP byte stream {@code FrameCodec} has to frame itself.
 *
 * <h2>What drives {@link FailureDetector#tick()}</h2>
 * This class also periodically calls {@code tick()} on the same schedule
 * heartbeats are sent — in production, something has to call it, and this
 * is that something. {@code FailureDetectorTest} covers the detector's
 * actual state-machine logic with a fake clock; the tests here only need to
 * prove datagrams really flow end to end between real sockets.
 */
public final class HeartbeatService implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);
    private static final int MAX_PACKET_BYTES = 1024;

    private final NodeId selfId;
    private final DatagramSocket socket;
    private final FailureDetector detector;
    private final Map<NodeId, NodeAddress> peers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final Thread receiverThread;
    private volatile boolean closed = false;

    public HeartbeatService(NodeId selfId, int port, FailureDetector detector, Duration heartbeatInterval)
            throws IOException {
        this.selfId = Objects.requireNonNull(selfId, "selfId must not be null");
        this.detector = Objects.requireNonNull(detector, "detector must not be null");
        Objects.requireNonNull(heartbeatInterval, "heartbeatInterval must not be null");
        if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }

        this.socket = new DatagramSocket(port);
        this.receiverThread = Thread.ofPlatform().name("heartbeat-receiver-" + selfId).start(this::receiveLoop);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().name("heartbeat-sender-" + selfId).unstarted(r));
        long intervalMillis = heartbeatInterval.toMillis();
        scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, intervalMillis, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(detector::tick, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public int port() {
        return socket.getLocalPort();
    }

    /** Starts sending heartbeats to (and expecting them from) {@code peer}, and admits it to the detector. */
    public void addPeer(NodeId peer, NodeAddress address) {
        peers.put(peer, address);
        detector.join(peer);
    }

    /** Stops sending heartbeats to {@code peer} and removes it from the detector (a voluntary departure). */
    public void removePeer(NodeId peer) {
        peers.remove(peer);
        detector.leave(peer);
    }

    private void sendHeartbeats() {
        byte[] payload = selfId.value().getBytes(StandardCharsets.UTF_8);
        for (NodeAddress address : peers.values()) {
            try {
                InetAddress inetAddress = InetAddress.getByName(address.host());
                socket.send(new DatagramPacket(payload, payload.length, inetAddress, address.port()));
            } catch (IOException e) {
                // Heartbeats are inherently fire-and-forget and expected to be lost sometimes
                // (DESIGN.md's Phase 8 failure assumption) — a single failed send is not fatal
                // and not even unusual; the failure detector's timeout, not this catch block,
                // is what's responsible for noticing a genuinely unreachable peer.
                log.debug("failed to send heartbeat from {} to {}", selfId, address, e);
            }
        }
    }

    private void receiveLoop() {
        byte[] buffer = new byte[MAX_PACKET_BYTES];
        while (!closed) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (SocketException e) {
                if (closed) {
                    return; // expected: close() closed the socket to unblock this receive()
                }
                log.warn("heartbeat socket error on {}; continuing to listen", selfId, e);
                continue;
            } catch (IOException e) {
                log.warn("failed to receive a heartbeat packet on {}", selfId, e);
                continue;
            }
            String senderValue = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
            if (senderValue.isEmpty()) {
                continue; // malformed/empty packet; ignore rather than fail the receiver loop
            }
            detector.recordHeartbeat(new NodeId(senderValue));
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        socket.close();
        scheduler.shutdownNow();
        try {
            receiverThread.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("heartbeat scheduler for {} did not terminate within the shutdown grace period", selfId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
