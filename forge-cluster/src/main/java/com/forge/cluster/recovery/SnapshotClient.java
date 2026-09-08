package com.forge.cluster.recovery;

import com.forge.storage.ConcurrentLsmKeyValueStore;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Fetches one full snapshot from a {@link SnapshotServer} and bulk-loads it
 * into a local (must-be-empty) {@link ConcurrentLsmKeyValueStore} — Phase
 * 10's bootstrap path. A one-shot operation, not a long-lived service like
 * {@link SnapshotServer}/{@code ReplicationFollower}: connect, transfer,
 * apply, done.
 *
 * <h2>Why an interrupted transfer can never leave a partial snapshot</h2>
 * The entire stream is read into an in-memory map <em>first</em>; {@link
 * ConcurrentLsmKeyValueStore#loadSnapshot} is only ever called with that
 * complete map, after the end-of-stream marker has actually been seen. If
 * the connection drops at any point before that — including immediately
 * after the watermark, or partway through any single entry — an
 * {@link IOException} propagates out of {@link #fetchAndLoad} and {@code
 * loadSnapshot} is never called at all, leaving the destination store
 * exactly as empty as it started. A caller can simply retry the whole
 * transfer from scratch; nothing needs to be undone first.
 *
 * <h2>Post-Phase-15 audit fix: a bounded read timeout</h2>
 * Originally this connected with no {@link java.net.Socket#setSoTimeout}
 * at all, so a peer that accepted the connection but then sent nothing —
 * wrong port, wrong protocol, or genuinely stuck — hung {@link #fetchAndLoad}
 * forever. This was disclosed as a known limitation in PROGRESS.md's Phase
 * 15 section and is exactly the bug class that produced a real 4+-minute
 * test hang during Phase 15 development (there, a test pointed this class
 * at a {@code ReplicationServer} port instead of a {@code SnapshotServer}
 * port — a test bug, but the identical mistake against a genuinely wrong or
 * frozen peer in production would hang identically). {@code
 * DEFAULT_TIMEOUT} now bounds both the connect and every subsequent read;
 * a hung/wrong-protocol peer now fails fast with a clear
 * {@link java.net.SocketTimeoutException} instead of blocking indefinitely.
 */
public final class SnapshotClient {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private SnapshotClient() {
    }

    /** @throws IOException on any transfer failure; the destination store is left untouched (see class Javadoc) */
    public static void fetchAndLoad(String host, int port, ConcurrentLsmKeyValueStore destination) throws IOException {
        fetchAndLoad(host, port, destination, DEFAULT_TIMEOUT);
    }

    /**
     * As {@link #fetchAndLoad(String, int, ConcurrentLsmKeyValueStore)}, with
     * an explicit bound on both the initial connect and every subsequent
     * read — a peer that never sends the expected bytes fails with a
     * {@link java.net.SocketTimeoutException} after {@code timeout} rather
     * than hanging the caller forever.
     */
    public static void fetchAndLoad(String host, int port, ConcurrentLsmKeyValueStore destination, Duration timeout)
            throws IOException {
        Objects.requireNonNull(host, "host must not be null");
        Objects.requireNonNull(destination, "destination must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        int timeoutMillis = (int) Math.min(timeout.toMillis(), Integer.MAX_VALUE);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            long watermark = in.readLong();
            Map<String, byte[]> keyValues = new HashMap<>();
            while (true) {
                int keyLength = in.readInt();
                if (keyLength == SnapshotServer.END_OF_SNAPSHOT_MARKER) {
                    break;
                }
                if (keyLength < 0 || keyLength > 64 * 1024 * 1024) {
                    throw new IOException("corrupt snapshot stream: invalid key length " + keyLength);
                }
                byte[] keyBytes = new byte[keyLength];
                in.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valueLength = in.readInt();
                if (valueLength < 0 || valueLength > 64 * 1024 * 1024) {
                    throw new IOException("corrupt snapshot stream: invalid value length " + valueLength);
                }
                byte[] value = new byte[valueLength];
                in.readFully(value);

                keyValues.put(key, value);
            }

            // Only reached once the full stream, including the terminator, has been read.
            destination.loadSnapshot(keyValues, watermark);
        }
    }
}
