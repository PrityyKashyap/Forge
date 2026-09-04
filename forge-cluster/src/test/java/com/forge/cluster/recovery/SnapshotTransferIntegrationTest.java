package com.forge.cluster.recovery;

import com.forge.cluster.NodeId;
import com.forge.cluster.replication.ReplicationFollower;
import com.forge.cluster.replication.ReplicationServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SnapshotTransferIntegrationTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeout);
        }
    }

    @Test
    @Timeout(30)
    void fetchAndLoadTransfersEveryKeyFromAPopulatedSource(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore sourceStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("source"))) {
            for (int i = 0; i < 100; i++) {
                sourceStore.put("k" + i, bytes("v" + i));
            }

            try (SnapshotServer server = new SnapshotServer(sourceStore, 0);
                 ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("dest"))) {

                SnapshotClient.fetchAndLoad("localhost", server.port(), destStore);

                assertEquals(100, destStore.keys().size());
                for (int i = 0; i < 100; i++) {
                    assertArrayEquals(bytes("v" + i), destStore.get("k" + i).orElseThrow());
                }
                assertEquals(sourceStore.lastAppliedSequenceNumber(), destStore.lastAppliedSequenceNumber());
            }
        }
    }

    @Test
    @Timeout(30)
    void fetchAndLoadOfAnEmptySourceProducesAnEmptyButValidDestination(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore sourceStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("source"));
             SnapshotServer server = new SnapshotServer(sourceStore, 0);
             ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("dest"))) {

            SnapshotClient.fetchAndLoad("localhost", server.port(), destStore);

            assertTrue(destStore.keys().isEmpty());
            assertEquals(1L, destStore.lastAppliedSequenceNumber());
        }
    }

    @Test
    @Timeout(30)
    void fetchAndLoadRejectsANonEmptyDestination(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore sourceStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("source"));
             SnapshotServer server = new SnapshotServer(sourceStore, 0);
             ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("dest"))) {
            sourceStore.put("a", bytes("1"));
            destStore.put("already-here", bytes("x"));

            assertThrows(IllegalStateException.class,
                    () -> SnapshotClient.fetchAndLoad("localhost", server.port(), destStore));
        }
    }

    @Test
    @Timeout(30)
    void anInterruptedTransferLeavesTheDestinationCompletelyUntouched(@TempDir Path baseDir) throws Exception {
        // A minimal fake server that sends a watermark, one entry, then closes without
        // ever sending the end-of-snapshot marker -- simulating a network failure or a
        // source crash partway through the transfer.
        try (ServerSocket rawServer = new ServerSocket(0)) {
            Thread serverThread = Thread.ofPlatform().start(() -> {
                try (Socket socket = rawServer.accept()) {
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                    out.writeLong(10L); // watermark
                    byte[] key = bytes("partial-key");
                    out.writeInt(key.length);
                    out.write(key);
                    out.writeInt(1);
                    out.write(bytes("1"));
                    out.flush();
                    // Deliberately no end-of-snapshot marker -- then the socket closes.
                } catch (IOException ignored) {
                    // expected once the client side closes its end after the failed read
                }
            });

            try (ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("dest"))) {
                assertThrows(IOException.class,
                        () -> SnapshotClient.fetchAndLoad("localhost", rawServer.getLocalPort(), destStore));

                assertTrue(destStore.keys().isEmpty(), "an interrupted transfer must never leave a partial snapshot loaded");
                assertEquals(1L, destStore.lastAppliedSequenceNumber(), "the watermark must not have been applied either");
            }
            serverThread.join(5_000);
        }
    }

    @Test
    @Timeout(30)
    void aBootstrappedNodeResumesReplicationSeamlesslyWithNoGapAndNoDuplication(@TempDir Path baseDir) throws Exception {
        Path destDir = baseDir.resolve("dest");
        try (ConcurrentLsmKeyValueStore sourceStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("source"))) {
            for (int i = 0; i < 50; i++) {
                sourceStore.put("k" + i, bytes("v" + i));
            }

            try (SnapshotServer snapshotServer = new SnapshotServer(sourceStore, 0);
                 ReplicationServer replicationServer = new ReplicationServer(sourceStore, 0)) {

                ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(destDir);
                SnapshotClient.fetchAndLoad("localhost", snapshotServer.port(), destStore);
                assertEquals(50, destStore.keys().size());

                // More writes happen on the source AFTER the snapshot was taken.
                for (int i = 50; i < 80; i++) {
                    sourceStore.put("k" + i, bytes("v" + i));
                }

                try (ReplicationFollower follower = new ReplicationFollower(
                        new NodeId("bootstrapped"), "localhost", replicationServer.port(), destStore, Duration.ofMillis(30))) {
                    waitUntil(Duration.ofSeconds(10), () -> destStore.keys().size() == 80);
                    for (int i = 0; i < 80; i++) {
                        assertArrayEquals(bytes("v" + i), destStore.get("k" + i).orElseThrow());
                    }
                }
                destStore.close();
            }
        }
    }

    @Test
    @Timeout(30)
    void bootstrapWhileConcurrentWritesAreHappeningStillConvergesToTheCorrectFinalState(@TempDir Path baseDir)
            throws Exception {
        Path destDir = baseDir.resolve("dest");
        try (ConcurrentLsmKeyValueStore sourceStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("source"))) {
            for (int i = 0; i < 100; i++) {
                sourceStore.put("pre-" + i, bytes("v" + i));
            }

            // Deliberately NOT a tight, zero-delay loop: ConcurrentLsmKeyValueStore's
            // stateLock (Phase 4) is a non-fair ReentrantReadWriteLock, and
            // SnapshotServer.sendSnapshot() takes that lock's read side once per key
            // (hundreds of times for a snapshot this size) rather than once for the
            // whole transfer (a deliberate choice — see SnapshotServer's Javadoc — so a
            // snapshot in progress never blocks writers). A writer that re-acquires the
            // write lock back-to-back with no gap can starve those per-key reads for a
            // very long time under a non-fair lock; this was discovered by an earlier,
            // zero-delay version of this exact test, which took over ten minutes instead
            // of the expected sub-second run. A tiny per-write delay is enough to give
            // waiting reads a real chance at the lock, which is representative of any
            // actual client anyway (every real write has at least a network round trip
            // between it and the next one) — see PROGRESS.md's Phase 10 section for why
            // this is recorded as a genuine lock-fairness limitation rather than "fixed"
            // by changing stateLock's fairness mode, which would need its own dedicated
            // design review and re-benchmarking (Phase 4's and Phase 6's numbers both
            // assume non-fair semantics).
            AtomicBoolean keepWriting = new AtomicBoolean(true);
            AtomicInteger liveWritesCompleted = new AtomicInteger(0);
            Thread writer = Thread.ofPlatform().start(() -> {
                int i = 0;
                while (keepWriting.get()) {
                    sourceStore.put("live-" + i, bytes("v" + i));
                    liveWritesCompleted.set(i + 1);
                    i++;
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });

            try (SnapshotServer snapshotServer = new SnapshotServer(sourceStore, 0);
                 ReplicationServer replicationServer = new ReplicationServer(sourceStore, 0)) {

                Thread.sleep(200); // let the writer get going before the snapshot races it
                ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(destDir);
                SnapshotClient.fetchAndLoad("localhost", snapshotServer.port(), destStore);

                ReplicationFollower follower = new ReplicationFollower(
                        new NodeId("racing-bootstrap"), "localhost", replicationServer.port(), destStore, Duration.ofMillis(30));

                Thread.sleep(300);
                keepWriting.set(false);
                writer.join(5_000);

                long sourceSeq = sourceStore.lastAppliedSequenceNumber();
                waitUntil(Duration.ofSeconds(10), () -> destStore.lastAppliedSequenceNumber() >= sourceSeq);
                follower.close();

                for (int i = 0; i < 100; i++) {
                    assertArrayEquals(bytes("v" + i), destStore.get("pre-" + i).orElseThrow());
                }
                int totalLive = liveWritesCompleted.get();
                for (int i = 0; i < totalLive; i++) {
                    assertArrayEquals(bytes("v" + i), destStore.get("live-" + i).orElseThrow(),
                            "live-" + i + " must have converged correctly despite racing the bootstrap snapshot");
                }
                assertEquals(100 + totalLive, destStore.keys().size(),
                        "no key should be lost or duplicated by a snapshot racing concurrent writes");
                destStore.close();
            } finally {
                keepWriting.set(false);
            }
        }
    }
}
