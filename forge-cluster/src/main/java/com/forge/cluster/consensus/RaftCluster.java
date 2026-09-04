package com.forge.cluster.consensus;

import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The real, networked driver around a {@link RaftNode} — owns the node, a
 * {@link RaftRpcServer} to receive RPCs, and a scheduler that periodically
 * calls {@link RaftNode#tick()} and actually transmits whatever
 * {@link RaftAction}s come back, feeding responses back into the node.
 * Same split {@code HeartbeatService} already established for
 * {@code FailureDetector}: all of the hard state-machine logic lives in a
 * pure, synchronously-testable class; this class only ever adds real
 * sockets and a clock-driven schedule on top.
 *
 * <p>Every RPC dispatch runs on its own virtual thread (an unresponsive or
 * partitioned peer must never stall the tick loop or another peer's RPC),
 * and a failed/timed-out send is simply logged and dropped — by design,
 * matching real Raft: a lost RequestVote just isn't counted, and a lost
 * AppendEntries is naturally retried on the next heartbeat tick with
 * whatever {@code nextIndex} currently holds, so no separate retry logic is
 * needed here.
 */
public final class RaftCluster implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftCluster.class);

    private final RaftNode node;
    private final RaftRpcServer rpcServer;
    private final Map<NodeId, NodeAddress> peerAddresses;
    private final Duration rpcTimeout;
    private final Duration leaseDuration;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService dispatchExecutor;

    public RaftCluster(NodeId selfId, Map<NodeId, NodeAddress> peerAddresses, int port, Clock clock,
            Duration electionTimeoutMin, Duration electionTimeoutMax, Duration heartbeatInterval,
            Duration tickInterval, Duration rpcTimeout, Random random) throws IOException {
        // A lease no shorter than the minimum election timeout: long enough that
        // ordinary heartbeat jitter never trips it, short enough that a genuinely
        // partitioned-away leader self-fences well before some other node could
        // legitimately have won a new election (which itself needs at least
        // electionTimeoutMin to elapse first) — see RaftNode.hasRecentQuorumContact.
        this(selfId, peerAddresses, port, clock, electionTimeoutMin, electionTimeoutMax, heartbeatInterval,
                tickInterval, rpcTimeout, electionTimeoutMin, random);
    }

    public RaftCluster(NodeId selfId, Map<NodeId, NodeAddress> peerAddresses, int port, Clock clock,
            Duration electionTimeoutMin, Duration electionTimeoutMax, Duration heartbeatInterval,
            Duration tickInterval, Duration rpcTimeout, Duration leaseDuration, Random random) throws IOException {
        this.peerAddresses = Map.copyOf(Objects.requireNonNull(peerAddresses, "peerAddresses must not be null"));
        this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout must not be null");
        this.leaseDuration = Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
        this.node = new RaftNode(selfId, this.peerAddresses.keySet(), clock, electionTimeoutMin, electionTimeoutMax,
                heartbeatInterval, random);
        this.rpcServer = new RaftRpcServer(node, port);
        this.dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().name("raft-ticker-" + selfId).unstarted(r));
        scheduler.scheduleAtFixedRate(this::doTick, 0, tickInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public int port() {
        return rpcServer.port();
    }

    public RaftRole role() {
        return node.role();
    }

    public boolean isConfirmedLeader() {
        return node.isConfirmedLeader();
    }

    /**
     * The Phase 15 fencing gate — see {@link RaftNode#canServeAuthoritatively}.
     * {@code PartitionLeadership} calls this, not {@link #isConfirmedLeader()},
     * to decide whether a write may proceed.
     */
    public boolean canServeAuthoritatively() {
        return node.canServeAuthoritatively(leaseDuration);
    }

    public java.util.Optional<NodeId> currentLeader() {
        return node.currentLeader();
    }

    public long currentTerm() {
        return node.currentTerm();
    }

    /** Exposed for tests that need to drive/inspect the underlying state machine directly. */
    RaftNode node() {
        return node;
    }

    private void doTick() {
        List<RaftAction> actions;
        try {
            actions = node.tick();
        } catch (RuntimeException e) {
            log.error("Raft tick failed for {}", node.selfId(), e);
            return;
        }
        for (RaftAction action : actions) {
            dispatchExecutor.execute(() -> dispatch(action));
        }
    }

    private void dispatch(RaftAction action) {
        NodeAddress address = peerAddresses.get(action.to());
        if (address == null) {
            log.warn("no known address for Raft peer {}; dropping outgoing RPC", action.to());
            return;
        }
        switch (action) {
            case RaftAction.SendRequestVote a -> dispatchRequestVote(address, a);
            case RaftAction.SendAppendEntries a -> dispatchAppendEntries(address, a);
        }
    }

    private void dispatchRequestVote(NodeAddress address, RaftAction.SendRequestVote action) {
        try (Socket socket = connect(address)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            out.writeByte(RaftWireFormat.MSG_REQUEST_VOTE);
            RaftWireFormat.writeRequestVoteRequest(out, action.request());
            out.flush();

            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            RequestVoteResponse response = RaftWireFormat.readRequestVoteResponse(in);
            List<RaftAction> followUp = node.handleRequestVoteResponse(action.to(), response);
            for (RaftAction next : followUp) {
                dispatchExecutor.execute(() -> dispatch(next));
            }
        } catch (IOException e) {
            log.debug("RequestVote to {} failed (peer down, partitioned, or timed out) — dropping, no retry needed",
                    action.to(), e);
        }
    }

    private void dispatchAppendEntries(NodeAddress address, RaftAction.SendAppendEntries action) {
        try (Socket socket = connect(address)) {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            out.writeByte(RaftWireFormat.MSG_APPEND_ENTRIES);
            RaftWireFormat.writeAppendEntriesRequest(out, action.request());
            out.flush();

            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            AppendEntriesResponse response = RaftWireFormat.readAppendEntriesResponse(in);
            node.handleAppendEntriesResponse(action.to(), action.request(), response);
        } catch (IOException e) {
            log.debug("AppendEntries to {} failed (peer down, partitioned, or timed out) — dropping, retried next heartbeat",
                    action.to(), e);
        }
    }

    private Socket connect(NodeAddress address) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(address.host(), address.port()), (int) rpcTimeout.toMillis());
        socket.setSoTimeout((int) rpcTimeout.toMillis());
        return socket;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        dispatchExecutor.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            if (!dispatchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Raft dispatch executor for {} did not terminate within the shutdown grace period", node.selfId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        rpcServer.close();
    }
}
