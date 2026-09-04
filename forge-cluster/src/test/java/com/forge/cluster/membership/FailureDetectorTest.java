package com.forge.cluster.membership;

import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every timing-dependent scenario here drives a {@link MutableClock}
 * explicitly rather than sleeping a real thread — per this phase's explicit
 * instruction to prefer injectable/fake clocks over real sleeps for
 * correctness. The one test that uses real threads
 * ({@link #concurrentMembershipUpdatesAreSafe}) does so only to probe
 * thread-safety (no lost/corrupted updates under contention), not timing —
 * it advances no clock and asserts no wall-clock-dependent outcome.
 */
class FailureDetectorTest {

    private static final NodeId A = new NodeId("node-a");
    private static final NodeId B = new NodeId("node-b");
    private static final Duration SUSPECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEAD_TIMEOUT = Duration.ofSeconds(30);

    private static FailureDetector newDetector(MutableClock clock) {
        return new FailureDetector(clock, SUSPECT_TIMEOUT, DEAD_TIMEOUT);
    }

    @Test
    void rejectsADeadTimeoutShorterThanSuspectTimeout() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        assertThrows(IllegalArgumentException.class,
                () -> new FailureDetector(clock, Duration.ofSeconds(30), Duration.ofSeconds(10)));
    }

    @Test
    void rejectsANonPositiveSuspectTimeout() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        assertThrows(IllegalArgumentException.class,
                () -> new FailureDetector(clock, Duration.ZERO, Duration.ofSeconds(10)));
    }

    @Test
    void joinAdmitsANodeAsAliveImmediately() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);

        detector.join(A);

        assertEquals(NodeState.ALIVE, detector.stateOf(A).orElseThrow());
    }

    @Test
    void leaveRemovesTheNodeEntirelyRatherThanMarkingItDead() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        detector.leave(A);

        assertTrue(detector.stateOf(A).isEmpty());
        assertTrue(detector.snapshot().isEmpty());
    }

    @Test
    void aNodeReceivingRegularHeartbeatsStaysAliveIndefinitely() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        for (int i = 0; i < 20; i++) {
            clock.advance(Duration.ofSeconds(5)); // well under suspectTimeout each step
            detector.recordHeartbeat(A);
            assertEquals(List.of(), detector.tick(), "a healthy, regularly-heartbeating node must never transition");
        }
        assertEquals(NodeState.ALIVE, detector.stateOf(A).orElseThrow());
    }

    @Test
    void missingHeartbeatsPastSuspectTimeoutTransitionsToSuspectNotDead() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        clock.advance(SUSPECT_TIMEOUT); // exactly at the boundary
        List<MembershipChange> changes = detector.tick();

        assertEquals(List.of(new MembershipChange(A, NodeState.ALIVE, NodeState.SUSPECT)), changes);
        assertEquals(NodeState.SUSPECT, detector.stateOf(A).orElseThrow());
    }

    @Test
    void oneSecondBeforeDeadTimeoutTheNodeIsOnlySuspectNeverDead() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        clock.advance(DEAD_TIMEOUT.minusSeconds(1));
        detector.tick();

        assertEquals(NodeState.SUSPECT, detector.stateOf(A).orElseThrow(),
                "the correctness invariant is DEAD only after the full deadTimeout elapses, never a moment before");
    }

    @Test
    void ifNoTickHappensDuringTheSuspectWindowAJumpStraightToDeadIsReportedAsOneTransition() {
        // The detector only reports state as of whenever tick() is actually called — if no
        // one calls it during the window where the node would have been SUSPECT, that
        // intermediate state is simply never observed, and the one tick that does happen
        // (after the full deadTimeout) correctly reports ALIVE -> DEAD directly. This is a
        // property of ticking being caller-driven, not a violation of the DEAD invariant
        // above (which is about how much time must elapse, not about intermediate polling).
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        clock.advance(DEAD_TIMEOUT.plusSeconds(1));
        List<MembershipChange> changes = detector.tick();

        assertEquals(List.of(new MembershipChange(A, NodeState.ALIVE, NodeState.DEAD)), changes);
    }

    @Test
    void tickingWhileStillWithinSuspectWindowThenCrossingItProducesBothTransitionsInOrder() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        clock.advance(Duration.ofSeconds(5)); // under suspectTimeout
        assertEquals(List.of(), detector.tick());

        clock.advance(Duration.ofSeconds(6)); // now at 11s total, over suspectTimeout(10s), under deadTimeout(30s)
        assertEquals(List.of(new MembershipChange(A, NodeState.ALIVE, NodeState.SUSPECT)), detector.tick());

        clock.advance(Duration.ofSeconds(20)); // now at 31s total, over deadTimeout
        assertEquals(List.of(new MembershipChange(A, NodeState.SUSPECT, NodeState.DEAD)), detector.tick());
    }

    @Test
    void aDeadNodeIsAutomaticallyRevivedToAliveTheMomentAHeartbeatArrives() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);
        clock.advance(DEAD_TIMEOUT.plusSeconds(1));
        detector.tick();
        assertEquals(NodeState.DEAD, detector.stateOf(A).orElseThrow());

        detector.recordHeartbeat(A);

        assertEquals(NodeState.ALIVE, detector.stateOf(A).orElseThrow(),
                "a resumed heartbeat must revive a DEAD node with no separate un-mark-dead call, no restart");
    }

    @Test
    void aSuspectNodeIsAlsoRevivedByAHeartbeat() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);
        clock.advance(SUSPECT_TIMEOUT.plusSeconds(1));
        detector.tick();
        assertEquals(NodeState.SUSPECT, detector.stateOf(A).orElseThrow());

        detector.recordHeartbeat(A);

        assertEquals(NodeState.ALIVE, detector.stateOf(A).orElseThrow());
    }

    @Test
    void repeatedFlappingProducesTheExpectedAlternatingTransitionsEveryTime() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        for (int round = 0; round < 5; round++) {
            clock.advance(DEAD_TIMEOUT.plusSeconds(1));
            List<MembershipChange> died = detector.tick();
            assertEquals(NodeState.DEAD, died.get(died.size() - 1).current(), "round " + round + ": must reach DEAD");

            detector.recordHeartbeat(A);
            assertEquals(NodeState.ALIVE, detector.stateOf(A).orElseThrow(), "round " + round + ": must revive to ALIVE");
        }
    }

    @Test
    void delayedHeartbeatThatArrivesBeforeSuspectTimeoutPreventsTheTransition() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        clock.advance(Duration.ofSeconds(9)); // 1 second before suspectTimeout
        detector.recordHeartbeat(A); // a "delayed" heartbeat, but still in time
        assertEquals(List.of(), detector.tick());
        assertEquals(NodeState.ALIVE, detector.stateOf(A).orElseThrow());
    }

    @Test
    void duplicateHeartbeatsAreIdempotentAndNeverCauseAnErrorOrDoubleTransition() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);

        assertDoesNotThrow(() -> {
            detector.recordHeartbeat(A);
            detector.recordHeartbeat(A);
            detector.recordHeartbeat(A);
        });
        assertEquals(NodeState.ALIVE, detector.stateOf(A).orElseThrow());
        assertEquals(List.of(), detector.tick());
    }

    @Test
    void heartbeatForANeverJoinedNodeIsIgnoredNotAutoJoined() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);

        var result = detector.recordHeartbeat(A);

        assertTrue(result.isEmpty());
        assertTrue(detector.stateOf(A).isEmpty(), "an unjoined node's heartbeat must not silently create membership");
    }

    @Test
    void heartbeatForANodeThatHasLeftIsIgnoredNotRejoined() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);
        detector.leave(A);

        var result = detector.recordHeartbeat(A);

        assertTrue(result.isEmpty());
        assertTrue(detector.stateOf(A).isEmpty());
    }

    @Test
    void twoNodesAreTrackedIndependently() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        detector.join(A);
        detector.join(B);

        clock.advance(DEAD_TIMEOUT.plusSeconds(1));
        detector.recordHeartbeat(B); // B stays alive; A does not
        List<MembershipChange> changes = detector.tick();

        assertEquals(List.of(new MembershipChange(A, NodeState.ALIVE, NodeState.DEAD)), changes);
        assertEquals(NodeState.DEAD, detector.stateOf(A).orElseThrow());
        assertEquals(NodeState.ALIVE, detector.stateOf(B).orElseThrow());
    }

    @Test
    @Timeout(30)
    void concurrentMembershipUpdatesAreSafe() throws Exception {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        FailureDetector detector = newDetector(clock);
        int nodeCount = 20;
        NodeId[] nodeIds = new NodeId[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            nodeIds[i] = new NodeId("concurrent-node-" + i);
            detector.join(nodeIds[i]);
        }

        int threadCount = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger totalHeartbeats = new AtomicInteger();
        java.util.List<Future<Void>> futures = new java.util.ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            int threadId = t;
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int i = 0; i < 200; i++) {
                    NodeId target = nodeIds[(threadId + i) % nodeCount];
                    detector.recordHeartbeat(target);
                    detector.tick();
                    detector.stateOf(target);
                    detector.snapshot();
                    totalHeartbeats.incrementAndGet();
                }
                return null;
            }));
        }
        ready.await();
        go.countDown();
        for (Future<Void> f : futures) {
            f.get(20, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertEquals(threadCount * 200, totalHeartbeats.get());
        assertEquals(nodeCount, detector.snapshot().size(), "no member entries should be lost or duplicated under contention");
        for (NodeId id : nodeIds) {
            assertEquals(NodeState.ALIVE, detector.stateOf(id).orElseThrow(),
                    "every node received heartbeats throughout and no clock time passed — all must still be ALIVE");
        }
    }
}
