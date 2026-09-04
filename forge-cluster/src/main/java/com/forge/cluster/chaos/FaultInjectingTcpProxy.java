package com.forge.cluster.chaos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A deterministic, explicitly-controlled TCP fault injector for chaos
 * testing: sits between a client and a real server, forwarding bytes in
 * both directions, and can be told — programmatically, never randomly — to
 * delay, drop, duplicate, or sever traffic. Per this phase's requirement:
 * "this should NOT be random chaos for the sake of looking impressive...
 * faults should be controllable and reproducible."
 *
 * <h2>What each fault actually does, precisely</h2>
 * <ul>
 *   <li>{@link #setDelay}: sleeps that long before forwarding each chunk
 *       read from either direction. A real, measurable network delay, not a
 *       simulated timestamp.</li>
 *   <li>{@link #dropNext}: silently discards the next {@code n} chunks
 *       forwarded (summed across both directions) — genuine packet loss,
 *       not a delayed-then-delivered message.</li>
 *   <li>{@link #setDuplicateAll}: forwards each chunk twice. Applied at the
 *       byte-chunk level read from the underlying socket, which for the
 *       small messages this project's protocols use typically corresponds
 *       to one complete frame, but is <em>not guaranteed</em> to align to a
 *       frame boundary for a larger message — a duplicated partial frame is
 *       exactly the kind of malformed input {@code FrameCodec}/{@code
 *       ReplicationWireFormat} are already tested against, so this remains
 *       a meaningful fault either way.</li>
 *   <li>{@link #partition()}: immediately closes every currently-proxied
 *       connection and refuses new ones until {@link #heal()} — modeling a
 *       network partition as it actually manifests to a real TCP
 *       application: the connection dies, future connection attempts fail,
 *       nothing queues up to be delivered late.</li>
 * </ul>
 *
 * <p>Reordering is deliberately not implemented at this proxy's byte-stream
 * level — TCP has no notion of "messages" to reorder without frame
 * awareness, and a naive byte-chunk permutation would mostly just produce
 * corrupted frames rather than a meaningful reordering of whole messages.
 * Phase 9's own {@code WriteAheadLogTest} (see {@code appendReplicatedRefusesToCreateAGap})
 * already directly tests FORGE's actual response to out-of-order delivery
 * at the layer where "order" is a meaningful, well-defined concept — this
 * phase's chaos scenario for reordering builds on that rather than
 * re-deriving it with a cruder mechanism.
 */
public final class FaultInjectingTcpProxy implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FaultInjectingTcpProxy.class);
    private static final int BUFFER_SIZE = 4096;

    private final String targetHost;
    private final int targetPort;
    private final ServerSocket listenSocket;
    private final ExecutorService executor;
    private final Thread acceptThread;
    private final Set<Socket> activeSockets = ConcurrentHashMap.newKeySet();

    private volatile Duration delay = Duration.ZERO;
    private final AtomicInteger dropCount = new AtomicInteger(0);
    private volatile boolean duplicateAll = false;
    private volatile boolean partitioned = false;
    private volatile boolean closed = false;

    public FaultInjectingTcpProxy(String targetHost, int targetPort, int listenPort) throws IOException {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.listenSocket = new ServerSocket(listenPort);
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.acceptThread = Thread.ofPlatform().name("chaos-proxy-acceptor-" + listenPort).start(this::acceptLoop);
    }

    public int port() {
        return listenSocket.getLocalPort();
    }

    public void setDelay(Duration delay) {
        this.delay = delay;
    }

    /** The next {@code n} chunks forwarded (either direction, summed) are silently discarded instead. */
    public void dropNext(int n) {
        dropCount.set(n);
    }

    public void setDuplicateAll(boolean duplicateAll) {
        this.duplicateAll = duplicateAll;
    }

    /** Severs every currently-proxied connection now and refuses new ones until {@link #heal()}. */
    public void partition() {
        partitioned = true;
        for (Socket socket : activeSockets) {
            closeQuietly(socket);
        }
    }

    public void heal() {
        partitioned = false;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    private void acceptLoop() {
        while (!closed) {
            Socket client;
            try {
                client = listenSocket.accept();
            } catch (IOException e) {
                if (closed) {
                    return;
                }
                log.warn("accept() failed on chaos proxy; continuing", e);
                continue;
            }
            if (closed) {
                closeQuietly(client);
                return;
            }
            if (partitioned) {
                closeQuietly(client); // refuse new connections while partitioned
                continue;
            }
            executor.execute(() -> handleConnection(client));
        }
    }

    private void handleConnection(Socket client) {
        Socket target;
        try {
            target = new Socket(targetHost, targetPort);
        } catch (IOException e) {
            log.warn("chaos proxy could not reach target {}:{}", targetHost, targetPort, e);
            closeQuietly(client);
            return;
        }
        activeSockets.add(client);
        activeSockets.add(target);
        try {
            Thread forward = Thread.ofPlatform().start(() -> pump(client, target));
            Thread backward = Thread.ofPlatform().start(() -> pump(target, client));
            forward.join();
            backward.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            activeSockets.remove(client);
            activeSockets.remove(target);
            closeQuietly(client);
            closeQuietly(target);
        }
    }

    private void pump(Socket from, Socket to) {
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            byte[] buffer = new byte[BUFFER_SIZE];
            while (!closed) {
                int n = in.read(buffer);
                if (n < 0) {
                    return; // clean EOF; let the caller close both sockets
                }
                if (partitioned) {
                    continue; // drop silently; the connection is about to be closed anyway
                }
                if (dropCount.getAndUpdate(c -> c > 0 ? c - 1 : 0) > 0) {
                    continue; // this chunk is the fault: genuinely never forwarded
                }
                Duration currentDelay = delay;
                if (!currentDelay.isZero()) {
                    Thread.sleep(currentDelay.toMillis());
                }
                out.write(buffer, 0, n);
                out.flush();
                if (duplicateAll) {
                    out.write(buffer, 0, n);
                    out.flush();
                }
            }
        } catch (IOException | InterruptedException e) {
            // connection ended (or was severed by partition()) -- nothing more to do here
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        listenSocket.close();
        for (Socket socket : activeSockets) {
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
                log.warn("chaos proxy connection executor did not terminate within the shutdown grace period");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("error closing chaos proxy socket", e);
        }
    }
}
