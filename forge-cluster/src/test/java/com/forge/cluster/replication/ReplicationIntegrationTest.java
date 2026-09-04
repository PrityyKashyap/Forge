package com.forge.cluster.replication;

import com.forge.cluster.NodeId;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Real {@link ReplicationServer} + {@link ReplicationFollower} pairs, real
 * TCP sockets, real {@link ConcurrentLsmKeyValueStore} instances on real
 * temp directories — this is what "writes reach replicas, replica data
 * converges, a failed follower can catch up, duplicate replication does not
 * corrupt state, restart recovery works" mean demonstrated for real, not
 * simulated in one process's memory.
 */
class ReplicationIntegrationTest {

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
    void writesOnTheLeaderAreReplicatedToAConnectedFollower(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0);
             ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"))) {

            try (ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("f1"), "localhost", server.port(), followerStore, Duration.ofMillis(50))) {

                for (int i = 0; i < 100; i++) {
                    leaderStore.put("k" + i, bytes("v" + i));
                }

                waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 100);
                for (int i = 0; i < 100; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
                }
            }
        }
    }

    @Test
    @Timeout(30)
    void aFollowerCatchesUpOnHistoryWrittenBeforeItEverConnected(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0)) {

            for (int i = 0; i < 50; i++) {
                leaderStore.put("pre-" + i, bytes("v" + i));
            }

            try (ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
                 ReplicationFollower follower = new ReplicationFollower(
                         new NodeId("f2"), "localhost", server.port(), followerStore, Duration.ofMillis(50))) {

                waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 50);
                for (int i = 0; i < 50; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore.get("pre-" + i).orElseThrow());
                }
            }
        }
    }

    @Test
    @Timeout(30)
    void deletesReplicateCorrectly(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0);
             ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
             ReplicationFollower follower = new ReplicationFollower(
                     new NodeId("f3"), "localhost", server.port(), followerStore, Duration.ofMillis(50))) {

            leaderStore.put("a", bytes("1"));
            waitUntil(Duration.ofSeconds(10), () -> followerStore.get("a").isPresent());

            leaderStore.delete("a");
            waitUntil(Duration.ofSeconds(10), () -> followerStore.get("a").isEmpty());
        }
    }

    @Test
    @Timeout(30)
    void writesConcurrentWithAFollowerCatchingUpAreNeverLostOrDuplicated(@TempDir Path baseDir) throws Exception {
        Path followerDir = baseDir.resolve("follower");
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0)) {

            // A substantial backlog for a follower to be catching up on...
            for (int i = 0; i < 200; i++) {
                leaderStore.put("backlog-" + i, bytes("v" + i));
            }

            // ...while writes keep happening live, racing against that same catch-up.
            AtomicInteger liveWritesCompleted = new AtomicInteger(0);
            AtomicBoolean keepWriting = new AtomicBoolean(true);
            Thread writer = Thread.ofPlatform().start(() -> {
                int i = 0;
                while (keepWriting.get()) {
                    leaderStore.put("live-" + i, bytes("v" + i));
                    liveWritesCompleted.set(i + 1);
                    i++;
                }
            });

            ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(followerDir);
            ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("f4"), "localhost", server.port(), followerStore, Duration.ofMillis(30));

            Thread.sleep(500); // let the writer genuinely race with catch-up for a while
            keepWriting.set(false);
            writer.join(5_000);

            long leaderSeq = leaderStore.lastAppliedSequenceNumber();
            waitUntil(Duration.ofSeconds(10), () -> followerStore.lastAppliedSequenceNumber() >= leaderSeq);
            follower.close();
            followerStore.close();

            try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(followerDir)) {
                for (int i = 0; i < 200; i++) {
                    assertArrayEquals(bytes("v" + i), reopened.get("backlog-" + i).orElseThrow());
                }
                int totalLiveWritten = liveWritesCompleted.get();
                for (int i = 0; i < totalLiveWritten; i++) {
                    assertArrayEquals(bytes("v" + i), reopened.get("live-" + i).orElseThrow(),
                            "live-" + i + " must have replicated correctly, with no corruption from the catch-up race");
                }
                assertEquals(200 + totalLiveWritten, reopened.keys().size(),
                        "no key should have been lost or duplicated by the catch-up/live-tail handoff race");
            }
        }
    }

    @Test
    @Timeout(30)
    void aDisconnectedFollowerDoesNotPreventTheLeaderFromContinuingToServeWrites(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0)) {

            ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
            ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("f5"), "localhost", server.port(), followerStore, Duration.ofMillis(50));

            leaderStore.put("a", bytes("1"));
            waitUntil(Duration.ofSeconds(10), () -> followerStore.get("a").isPresent());

            follower.close();
            followerStore.close();

            // The leader must keep working normally even with no follower attached at all.
            assertDoesNotThrow(() -> {
                for (int i = 0; i < 50; i++) {
                    leaderStore.put("after-disconnect-" + i, bytes("v" + i));
                }
            });
            assertEquals(51, leaderStore.keys().size());
        }
    }

    @Test
    @Timeout(30)
    void aFollowerCanReconnectAfterRestartAndFinishCatchingUpWithoutDuplicatingAnything(@TempDir Path baseDir)
            throws Exception {
        Path followerDir = baseDir.resolve("follower");
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0)) {

            for (int i = 0; i < 30; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            // First connection: catches up on some of it, then "crashes" (closed abruptly).
            ConcurrentLsmKeyValueStore followerStore1 = new ConcurrentLsmKeyValueStore(followerDir);
            ReplicationFollower follower1 = new ReplicationFollower(
                    new NodeId("f6"), "localhost", server.port(), followerStore1, Duration.ofMillis(50));
            waitUntil(Duration.ofSeconds(10), () -> followerStore1.keys().size() == 30);
            follower1.close();
            followerStore1.close();

            // More writes happen on the leader while the follower is "down."
            for (int i = 30; i < 60; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            // "Restart": reopen the SAME on-disk directory (recovers what was already
            // replicated via the durable WAL) and reconnect.
            try (ConcurrentLsmKeyValueStore followerStore2 = new ConcurrentLsmKeyValueStore(followerDir);
                 ReplicationFollower follower2 = new ReplicationFollower(
                         new NodeId("f6"), "localhost", server.port(), followerStore2, Duration.ofMillis(50))) {

                // Everything from before the "crash" survived recovery, with no need to re-fetch it.
                for (int i = 0; i < 30; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore2.get("k" + i).orElseThrow());
                }
                waitUntil(Duration.ofSeconds(10), () -> followerStore2.keys().size() == 60);
                for (int i = 30; i < 60; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore2.get("k" + i).orElseThrow());
                }
                assertEquals(60, followerStore2.keys().size(), "no key should have been duplicated or corrupted by the reconnect");
            }
        }
    }

    @Test
    @Timeout(30)
    void replicaLagIsTrackedAndConvergesToZeroOnceCaughtUp(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0);
             ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
             ReplicationFollower follower = new ReplicationFollower(
                     new NodeId("f7"), "localhost", server.port(), followerStore, Duration.ofMillis(30))) {

            for (int i = 0; i < 40; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            long leaderSeq = leaderStore.lastAppliedSequenceNumber();
            waitUntil(Duration.ofSeconds(10), () -> {
                ReplicaState state = server.replicaStates().get(new NodeId("f7"));
                return state != null && state.acknowledgedSequenceNumber() >= leaderSeq;
            });

            ReplicaState finalState = server.replicaStates().get(new NodeId("f7"));
            assertEquals(leaderSeq, finalState.acknowledgedSequenceNumber(),
                    "once fully caught up, the follower's acked sequence number must equal the leader's");
        }
    }

    @Test
    @Timeout(30)
    void aFollowerSurvivesTheLeaderDisappearingWithoutCorruptingItsOwnState(@TempDir Path baseDir) throws Exception {
        ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
        ReplicationServer server = new ReplicationServer(leaderStore, 0);
        ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
        ReplicationFollower follower = new ReplicationFollower(
                new NodeId("f8"), "localhost", server.port(), followerStore, Duration.ofMillis(30));

        try {
            leaderStore.put("a", bytes("1"));
            waitUntil(Duration.ofSeconds(10), () -> followerStore.get("a").isPresent());

            // The leader disappears entirely (server closed, store closed) — as if the process died.
            server.close();
            leaderStore.close();

            // The follower must not throw, hang, or corrupt its own state — its own
            // store remains fully usable on its own, exactly as it was when the leader vanished.
            Thread.sleep(200); // let the follower's apply thread notice the broken connection
            assertDoesNotThrow(() -> followerStore.put("still-works-locally", bytes("yes")));
            assertArrayEquals(bytes("1"), followerStore.get("a").orElseThrow());
            assertFalse(follower.gapDetected());
        } finally {
            follower.close();
            followerStore.close();
        }
    }

    @Test
    @Timeout(30)
    void writesSucceedLocallyOnTheLeaderEvenWithZeroFollowersConnected(@TempDir Path baseDir) throws Exception {
        // Documents the actual (async) durability guarantee: a leader never
        // blocks a local write waiting for any follower.
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer server = new ReplicationServer(leaderStore, 0)) {
            assertDoesNotThrow(() -> leaderStore.put("a", bytes("1")));
            assertArrayEquals(bytes("1"), leaderStore.get("a").orElseThrow());
            assertTrue(server.replicaStates().isEmpty());
        }
    }
}
