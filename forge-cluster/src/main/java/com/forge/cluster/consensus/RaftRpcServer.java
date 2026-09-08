package com.forge.cluster.consensus;

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
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Listens for real RequestVote/AppendEntries RPC connections and dispatches
 * each one straight into a {@link RaftNode} — the inbound half of Raft's
 * networking, real sockets between real node processes, same acceptor/
 * virtual-thread-per-connection shape as {@code SnapshotServer}.
 */
public final class RaftRpcServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftRpcServer.class);

    private final RaftNode node;
    private final ServerSocket serverSocket;
    private final ExecutorService executor;
    private final Thread acceptThread;
    private final Set<Socket> activeConnections = ConcurrentHashMap.newKeySet();
    private volatile boolean closed = false;

    public RaftRpcServer(RaftNode node, int port) throws IOException {
        this.node = node;
        this.serverSocket = new ServerSocket(port);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.acceptThread = Thread.ofPlatform().name("raft-rpc-server-" + node.selfId()).start(this::acceptLoop);
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
                log.warn("accept() failed on Raft RPC server for {}; continuing", node.selfId(), e);
                continue;
            }
            if (closed) {
                closeQuietly(socket);
                return;
            }
            activeConnections.add(socket);
            executor.execute(() -> {
                try {
                    handleConnection(socket);
                } finally {
                    activeConnections.remove(socket);
                    closeQuietly(socket);
                }
            });
        }
    }

    private void handleConnection(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            byte messageType = in.readByte();
            switch (messageType) {
                case RaftWireFormat.MSG_REQUEST_VOTE -> {
                    RequestVoteRequest request = RaftWireFormat.readRequestVoteRequest(in);
                    RequestVoteResponse response = handleRequestVote(request);
                    if (response == null) {
                        return; // failed to durably persist the vote — see handleRequestVote; no response sent
                    }
                    RaftWireFormat.writeRequestVoteResponse(out, response);
                }
                case RaftWireFormat.MSG_APPEND_ENTRIES -> {
                    AppendEntriesRequest request = RaftWireFormat.readAppendEntriesRequest(in);
                    AppendEntriesResponse response = handleAppendEntries(request);
                    if (response == null) {
                        return; // failed to durably persist the term/vote change — see handleAppendEntries
                    }
                    RaftWireFormat.writeAppendEntriesResponse(out, response);
                }
                default -> log.warn("Raft RPC server for {} received an unrecognized message type {}",
                        node.selfId(), messageType);
            }
            out.flush();
        } catch (IOException e) {
            log.debug("Raft RPC connection to {} failed or was closed by the peer", node.selfId(), e);
        }
    }

    /** @return the response, or {@code null} if a durable-persistence failure means no response should be sent at all. */
    private RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        try {
            return node.handleRequestVote(request);
        } catch (IOException e) {
            log.warn("failed to durably persist Raft state while handling a RequestVote from {}; "
                    + "not responding — the candidate will simply see this as a dropped RPC and retry", node.selfId(), e);
            return null;
        }
    }

    /** @return the response, or {@code null} if a durable-persistence failure means no response should be sent at all. */
    private AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        try {
            return node.handleAppendEntries(request);
        } catch (IOException e) {
            log.warn("failed to durably persist Raft state while handling an AppendEntries from {}; "
                    + "not responding — the leader will simply see this as a dropped RPC and retry", node.selfId(), e);
            return null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            serverSocket.close();
        } catch (IOException e) {
            log.debug("error closing Raft RPC server socket for {}", node.selfId(), e);
        }
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
                log.warn("Raft RPC server connection executor for {} did not terminate within the shutdown grace period",
                        node.selfId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("error closing Raft RPC connection during shutdown", e);
        }
    }
}
