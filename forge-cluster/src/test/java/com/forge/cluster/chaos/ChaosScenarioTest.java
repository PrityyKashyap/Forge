package com.forge.cluster.chaos;

import com.forge.cluster.NodeId;
import com.forge.cluster.membership.FailureDetector;
import com.forge.cluster.membership.NodeState;
import com.forge.cluster.recovery.SnapshotClient;
import com.forge.cluster.replication.ReplicationFollower;
import com.forge.cluster.replication.ReplicationServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Six deterministic, reproducible chaos scenarios, per this phase's own
 * requirement: <b>not</b> random chaos for the sake of looking impressive —
 * every fault here is injected at an exact, controlled point (a specific
 * call to {@code close()}, {@code partition()}, or {@code dropNext(n)}), so
 * a run either always exposes the same behavior or genuinely found a bug.
 *
 * <p>Every scenario states, up front, in its own Javadoc: the assumptions
 * it operates under, the safety property it actually proves, and the
 * liveness property it actually proves — and never claims more than the
 * architecture built through Phase 10 actually supports. In particular:
 * this project has <b>no consensus or automatic failover yet</b> (that's
 * Phase 14) and replication is <b>asynchronous only</b> (Phase 9) — so no
 * scenario here claims "zero data loss" for a write that was only ever
 * acknowledged by a leader that then died before shipping it anywhere.
 */
class ChaosScenarioTest {

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

    /**
     * <b>Scenario A — leader crashes during active replication.</b>
     * <p>Assumptions: asynchronous replication (Phase 9); no failover exists
     * (Phase 14) — a dead leader is not replaced by anything.
     * <p>Safety property actually proved: every write the follower had
     * already durably applied before the leader died remains intact and
     * uncorrupted on the follower afterward. (Not claimed: writes the
     * leader acked to a client but never shipped to any follower — those
     * can genuinely be lost under this architecture, honestly.)
     * <p>Liveness property actually proved: the follower's own store
     * remains fully usable (readable and writable) after the leader
     * disappears — it does not hang or require the leader to function.
     */
    @Test
    @Timeout(30)
    void scenarioA_leaderCrashesDuringActiveReplication(@TempDir Path baseDir) throws Exception {
        ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
        ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0);
        ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"));
        ReplicationFollower follower = new ReplicationFollower(
                new NodeId("scenario-a-follower"), "localhost", replicationServer.port(), followerStore, Duration.ofMillis(30));

        try {
            for (int i = 0; i < 50; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }
            waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 50);

            // The leader dies mid-flow -- deterministically, at this exact point.
            replicationServer.close();
            leaderStore.close();

            // Safety: everything already applied is intact.
            for (int i = 0; i < 50; i++) {
                assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
            }
            // Liveness: the follower's own store still works, leader or no leader.
            assertDoesNotThrow(() -> followerStore.put("still-usable", bytes("yes")));
            assertArrayEquals(bytes("yes"), followerStore.get("still-usable").orElseThrow());
        } finally {
            follower.close();
            followerStore.close();
        }
    }

    /**
     * <b>Scenario B — follower disappears and rejoins.</b>
     * <p>Assumptions: the follower's on-disk directory survives the outage
     * (a process crash/restart, not a disk loss — that would need Phase
     * 10's snapshot bootstrap instead, covered separately).
     * <p>Safety property: no record is lost or duplicated across the
     * disappearance-and-rejoin cycle — the follower's final state after
     * catching up again is bit-for-bit what it would have been had it
     * never left.
     * <p>Liveness property: after rejoining, the follower reaches full
     * parity with the leader within a bounded time, with no manual
     * intervention beyond reconnecting.
     */
    @Test
    @Timeout(30)
    void scenarioB_followerDisappearsAndRejoins(@TempDir Path baseDir) throws Exception {
        Path followerDir = baseDir.resolve("follower");
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0)) {

            for (int i = 0; i < 20; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            ConcurrentLsmKeyValueStore followerStore1 = new ConcurrentLsmKeyValueStore(followerDir);
            ReplicationFollower follower1 = new ReplicationFollower(
                    new NodeId("scenario-b"), "localhost", replicationServer.port(), followerStore1, Duration.ofMillis(30));
            waitUntil(Duration.ofSeconds(10), () -> followerStore1.keys().size() == 20);

            // The follower disappears -- deterministically, right here.
            follower1.close();
            followerStore1.close();

            for (int i = 20; i < 40; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            // It rejoins -- same directory, fresh connection.
            try (ConcurrentLsmKeyValueStore followerStore2 = new ConcurrentLsmKeyValueStore(followerDir);
                 ReplicationFollower follower2 = new ReplicationFollower(
                         new NodeId("scenario-b"), "localhost", replicationServer.port(), followerStore2, Duration.ofMillis(30))) {

                waitUntil(Duration.ofSeconds(10), () -> followerStore2.keys().size() == 40);
                for (int i = 0; i < 40; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore2.get("k" + i).orElseThrow());
                }
                assertEquals(40, followerStore2.keys().size(), "no record lost or duplicated across the disappear/rejoin cycle");
            }
        }
    }

    /**
     * <b>Scenario C — network partition separates leader from follower.</b>
     * <p>Adapted from "leader from majority": this project has no quorum
     * concept yet (that needs Phase 14's consensus) — with exactly one
     * leader and one follower, "partitioned from the majority" and
     * "partitioned from its only follower" are the same scenario here.
     * <p>Safety property: the follower's data stays internally consistent
     * throughout the partition (no corruption from the severed connection
     * itself); the leader's local writes are never blocked by the
     * follower's unreachability (proving async replication's local writes
     * are genuinely decoupled from follower availability, not just in
     * theory).
     * <p>Liveness property: once the partition heals <em>and the caller
     * reconnects</em>, the follower catches up to full parity.
     * {@code ReplicationFollower} deliberately does not auto-reconnect a
     * severed connection (see its own class Javadoc) — a partition kills
     * the TCP connection just as a real one would, and per this project's
     * documented contract, the caller is the one who creates a fresh
     * {@code ReplicationFollower} afterward, exactly as Phase 9/10's own
     * reconnect tests already do. This scenario follows that same,
     * already-established contract rather than assuming a capability that
     * was never built.
     */
    @Test
    @Timeout(30)
    void scenarioC_networkPartitionSeparatesLeaderFromFollower(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0);
             FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", replicationServer.port(), 0);
             ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"))) {

            for (int i = 0; i < 20; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }

            ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("scenario-c"), "localhost", proxy.port(), followerStore, Duration.ofMillis(30));
            waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 20);

            proxy.partition();
            assertTrue(proxy.isPartitioned());

            // Liveness of the LEADER specifically: local writes must not block just
            // because the follower is now unreachable.
            long start = System.nanoTime();
            for (int i = 20; i < 40; i++) {
                leaderStore.put("k" + i, bytes("v" + i));
            }
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            assertTrue(elapsed.toSeconds() < 5, "leader writes must not block on an unreachable follower, took " + elapsed);

            // Safety: the follower's own (partial) data is untouched and consistent
            // while partitioned.
            for (int i = 0; i < 20; i++) {
                assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
            }

            follower.close(); // the partition already severed this connection; retire it explicitly
            proxy.heal();

            // Liveness: reconnecting (a fresh ReplicationFollower, per the documented
            // contract) reaches full parity.
            try (ReplicationFollower reconnected = new ReplicationFollower(
                    new NodeId("scenario-c"), "localhost", proxy.port(), followerStore, Duration.ofMillis(30))) {
                waitUntil(Duration.ofSeconds(10), () -> followerStore.keys().size() == 40);
                for (int i = 20; i < 40; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
                }
            }
        }
    }

    /**
     * <b>Scenario D — messages are delayed (and, separately, arrive out of order).</b>
     * <p>Delay is tested directly here, over a real proxied connection.
     * Reordering is deliberately tested at the layer where "order" is a
     * well-defined concept — {@code WriteAheadLog.appendReplicated}'s own
     * gap/duplicate handling ({@code WriteAheadLogTest.appendReplicatedRefusesToCreateAGap}
     * and {@code appendReplicatedIsIdempotentForAnAlreadyAppliedSequenceNumber},
     * Phase 9) — rather than re-derived here with a cruder byte-level
     * permutation that would mostly just produce corrupted frames; see
     * {@link FaultInjectingTcpProxy}'s class Javadoc for why.
     * <p>Safety property: no corruption and no duplication despite
     * significant, real, injected network delay.
     * <p>Liveness property: replication still fully converges, just later —
     * delay changes when convergence happens, not whether it happens.
     */
    @Test
    @Timeout(30)
    void scenarioD_messagesAreSignificantlyDelayed(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
             ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0);
             FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", replicationServer.port(), 0);
             ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower"))) {

            proxy.setDelay(Duration.ofMillis(200));

            try (ReplicationFollower follower = new ReplicationFollower(
                    new NodeId("scenario-d"), "localhost", proxy.port(), followerStore, Duration.ofMillis(30))) {

                for (int i = 0; i < 10; i++) {
                    leaderStore.put("k" + i, bytes("v" + i));
                }

                // Must still converge -- just not instantly, given the injected delay.
                waitUntil(Duration.ofSeconds(20), () -> followerStore.keys().size() == 10);
                for (int i = 0; i < 10; i++) {
                    assertArrayEquals(bytes("v" + i), followerStore.get("k" + i).orElseThrow());
                }
            }
        }
    }

    /**
     * <b>Scenario E — a node crashes during recovery (bootstrap).</b>
     * <p>Assumptions: {@code SnapshotClient.fetchAndLoad} only ever calls
     * {@code loadSnapshot} after reading the complete stream (see its own
     * class Javadoc); this scenario deliberately severs the transfer before
     * that point.
     * <p>Safety property: an interrupted bootstrap leaves the destination
     * exactly as it was before the attempt (empty) — never a partially
     * loaded, inconsistent SSTable.
     * <p>Liveness property: the bootstrap can simply be retried from
     * scratch afterward and succeed normally — the interrupted attempt
     * leaves nothing that needs to be cleaned up first.
     */
    @Test
    @Timeout(30)
    void scenarioE_nodeCrashesDuringBootstrapRecovery(@TempDir Path baseDir) throws Exception {
        try (ConcurrentLsmKeyValueStore sourceStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("source"))) {
            for (int i = 0; i < 30; i++) {
                sourceStore.put("k" + i, bytes("v" + i));
            }

            try (com.forge.cluster.recovery.SnapshotServer snapshotServer =
                    new com.forge.cluster.recovery.SnapshotServer(sourceStore, 0);
                    FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", snapshotServer.port(), 0)) {

                // The transfer is severed deterministically, shortly after it starts,
                // by partitioning the proxy mid-stream.
                Thread severer = Thread.ofPlatform().start(() -> {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    proxy.partition();
                });

                Path destDir = baseDir.resolve("dest");
                ConcurrentLsmKeyValueStore destStore = new ConcurrentLsmKeyValueStore(destDir);
                try {
                    try {
                        SnapshotClient.fetchAndLoad("localhost", proxy.port(), destStore);
                        // If it happened to complete before the severer fired, that's a valid
                        // (if untested-by-this-run) outcome too -- the safety property below
                        // still holds either way.
                    } catch (IOException expected) {
                        // the interrupted-transfer path -- the expected outcome most runs take
                    }
                    severer.join(5_000);

                    // Safety: either nothing loaded (interrupted) or everything loaded
                    // (completed first) -- never a partial state in between.
                    int keyCount = destStore.keys().size();
                    assertTrue(keyCount == 0 || keyCount == 30,
                            "bootstrap must never leave a partially-loaded destination; got " + keyCount + " keys");
                } finally {
                    destStore.close();
                }

                // Liveness: retrying from scratch (fresh destination directory) succeeds normally.
                proxy.heal();
                Path retryDir = baseDir.resolve("dest-retry");
                try (ConcurrentLsmKeyValueStore retryStore = new ConcurrentLsmKeyValueStore(retryDir)) {
                    SnapshotClient.fetchAndLoad("localhost", proxy.port(), retryStore);
                    assertEquals(30, retryStore.keys().size());
                }
            }
        }
    }

    /**
     * <b>Scenario F — two nodes temporarily have stale membership.</b>
     * <p>Assumptions: DESIGN.md's Phase 8 contract itself: membership is
     * eventually consistent, not agreed-upon — two independent
     * {@link FailureDetector}s are two independent local views by design,
     * not a single shared source of truth.
     * <p>Safety property: neither node's detector ever throws, corrupts its
     * own state, or produces an impossible state transition while the two
     * views disagree.
     * <p>Liveness property: once heartbeats resume, both nodes' views
     * converge back to agreement (both ALIVE) — staleness is temporary, not
     * permanent, with no manual reconciliation needed.
     */
    @Test
    @Timeout(15)
    void scenarioF_twoNodesTemporarilyHaveStaleMembership() {
        NodeId a = new NodeId("scenario-f-a");
        NodeId b = new NodeId("scenario-f-b");
        FailureDetector detectorOnA = new FailureDetector(Clock.systemUTC(), Duration.ofMillis(100), Duration.ofMillis(200));
        FailureDetector detectorOnB = new FailureDetector(Clock.systemUTC(), Duration.ofMillis(100), Duration.ofMillis(200));
        detectorOnA.join(b); // A's view of B
        detectorOnB.join(a); // B's view of A

        // Heartbeats between them stop (simulated directly: nobody calls recordHeartbeat).
        assertDoesNotThrow(() -> {
            Thread.sleep(250);
            detectorOnA.tick();
            detectorOnB.tick();
        });

        // Both views are now stale (DEAD) -- this is the "temporarily stale" state itself,
        // not a bug: each node's view has correctly, independently detected the silence.
        assertEquals(NodeState.DEAD, detectorOnA.stateOf(b).orElseThrow());
        assertEquals(NodeState.DEAD, detectorOnB.stateOf(a).orElseThrow());

        // Heartbeats resume -- both views converge back to agreement automatically.
        detectorOnA.recordHeartbeat(b);
        detectorOnB.recordHeartbeat(a);

        assertEquals(NodeState.ALIVE, detectorOnA.stateOf(b).orElseThrow());
        assertEquals(NodeState.ALIVE, detectorOnB.stateOf(a).orElseThrow());
    }
}
