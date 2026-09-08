package com.forge.cluster.consensus;

import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Post-Phase-15 audit addition: proves {@link RaftCluster}'s
 * persistence-enabled constructor actually performs real, durable file I/O
 * during a genuine networked election (real sockets, real {@link
 * RaftRpcServer}, real {@link RaftCluster} dispatch loop) — not just that
 * {@link RaftNode} itself calls the listener correctly, which {@code
 * RaftPersistenceIntegrationTest} already proves in isolation. The specific
 * safety property (no double vote across a restart) is proven once, at the
 * {@code RaftNode} level, where it can be tested deterministically without
 * racing real election timers; this test's job is only to prove the
 * production wiring between {@code RaftCluster} and {@code
 * RaftPersistentState} is actually connected end to end.
 */
class RaftClusterPersistenceIntegrationTest {

    private static final Duration HEARTBEAT = Duration.ofMillis(40);
    private static final Duration TICK = Duration.ofMillis(15);
    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(2);

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

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    @Test
    @Timeout(30)
    void aRealElectionDurablyPersistsTermAndVotedForToDisk(@TempDir Path dir) throws Exception {
        NodeId a = new NodeId("pers-a");
        NodeId b = new NodeId("pers-b");
        Map<NodeId, NodeAddress> addresses = new HashMap<>();
        addresses.put(a, new NodeAddress("localhost", freePort()));
        addresses.put(b, new NodeAddress("localhost", freePort()));
        Path stateFileA = dir.resolve("a-raft-state");

        // A has a much shorter election timeout, so it deterministically wins the race.
        try (RaftCluster clusterA = new RaftCluster(a, withoutSelf(addresses, a), addresses.get(a).port(),
                        Clock.systemUTC(), Duration.ofMillis(40), Duration.ofMillis(60), HEARTBEAT, TICK,
                        RPC_TIMEOUT, Duration.ofMillis(40), new Random(1), stateFileA);
                RaftCluster clusterB = new RaftCluster(b, withoutSelf(addresses, b), addresses.get(b).port(),
                        Clock.systemUTC(), Duration.ofMillis(400), Duration.ofMillis(600), HEARTBEAT, TICK,
                        RPC_TIMEOUT, Duration.ofMillis(400), new Random(2))) {

            waitUntil(Duration.ofSeconds(10), clusterA::isConfirmedLeader);
            long termWon = clusterA.currentTerm();
            assertTrue(termWon >= 1);

            // The real networked election (tick() -> startElection() -> the persistence
            // listener -> RaftPersistentState.save()) must have durably written A's vote for
            // itself, on disk, independently of the in-memory RaftCluster/RaftNode objects.
            waitUntil(Duration.ofSeconds(5), () -> {
                try {
                    RaftPersistentState onDisk = RaftPersistentState.load(stateFileA);
                    return onDisk.currentTerm() == termWon && onDisk.votedFor().map(a::equals).orElse(false);
                } catch (IOException e) {
                    return false;
                }
            });
        }
    }

    /**
     * A deliberate 3-node (not 2-node) setup — the same reasoning {@code
     * ChaosScenarioTest}'s Scenario K documents: this project's log is
     * <em>not</em> persisted (see {@code RaftPersistentState}'s class
     * Javadoc for why that's a safe, disclosed scope boundary), so a
     * restarted node's log is always empty, which correctly (per Raft's
     * up-to-date-log safety rule) makes peers with a non-empty log refuse
     * to vote for it. In a 2-node cluster that is a genuine, permanent
     * liveness dead-end: the restarted node can never win (its log looks
     * stale), and the survivor alone can never reach a 2-node majority
     * either — neither side can ever become leader again. With 3 nodes,
     * the two survivors reach majority (2 of 3) <em>without</em> the
     * restarted node's vote at all, so the cluster keeps making progress;
     * the restarted node's own correct behavior is to rejoin as a
     * follower of whichever survivor legitimately won, not to become
     * leader itself. Proving persistence here means proving the term
     * survives the restart and the node peacefully rejoins — not that it
     * regains leadership, which would require the log to be persisted too
     * (a deliberately larger change this phase does not make).
     */
    @Test
    @Timeout(30)
    void aRestartedRaftClusterReloadsItsPersistedTermAndRejoinsAsAFollower(@TempDir Path dir) throws Exception {
        NodeId a = new NodeId("restart-a");
        NodeId b = new NodeId("restart-b");
        NodeId c = new NodeId("restart-c");
        int portA = freePort();
        int portB = freePort();
        int portC = freePort();
        Map<NodeId, NodeAddress> addresses = Map.of(
                a, new NodeAddress("localhost", portA),
                b, new NodeAddress("localhost", portB),
                c, new NodeAddress("localhost", portC));
        Path stateFileA = dir.resolve("a-raft-state");

        long termAtCrash;
        RaftCluster clusterB = new RaftCluster(b, withoutSelf(addresses, b), portB, Clock.systemUTC(),
                Duration.ofMillis(400), Duration.ofMillis(600), HEARTBEAT, TICK, RPC_TIMEOUT,
                Duration.ofMillis(400), new Random(2));
        RaftCluster clusterC = new RaftCluster(c, withoutSelf(addresses, c), portC, Clock.systemUTC(),
                Duration.ofMillis(400), Duration.ofMillis(600), HEARTBEAT, TICK, RPC_TIMEOUT,
                Duration.ofMillis(400), new Random(3));
        try {
            try (RaftCluster clusterA = new RaftCluster(a, withoutSelf(addresses, a), portA, Clock.systemUTC(),
                    Duration.ofMillis(40), Duration.ofMillis(60), HEARTBEAT, TICK, RPC_TIMEOUT,
                    Duration.ofMillis(40), new Random(1), stateFileA)) {
                waitUntil(Duration.ofSeconds(10), clusterA::isConfirmedLeader);
                termAtCrash = clusterA.currentTerm();
            } // "crash": clusterA closed with no graceful persistence beyond what elections already wrote

            // B and C reach majority (2 of 3) among themselves — no vote from A needed at all.
            waitUntil(Duration.ofSeconds(10), () -> clusterB.isConfirmedLeader() || clusterC.isConfirmedLeader());
            RaftCluster newLeader = clusterB.isConfirmedLeader() ? clusterB : clusterC;
            assertTrue(newLeader.currentTerm() > termAtCrash, "the survivors must elect at a strictly higher term");

            // Real restart: a brand-new RaftCluster (and brand-new RaftRpcServer on the same
            // port), reloading only what stateFileA durably holds.
            try (RaftCluster restartedA = new RaftCluster(a, withoutSelf(addresses, a), portA, Clock.systemUTC(),
                    Duration.ofMillis(40), Duration.ofMillis(60), HEARTBEAT, TICK, RPC_TIMEOUT,
                    Duration.ofMillis(40), new Random(4), stateFileA)) {
                assertTrue(restartedA.currentTerm() >= termAtCrash,
                        "a restarted node must reload its persisted term, never silently reset to 0");

                // It rejoins peacefully as a FOLLOWER of the already-legitimate new leader — it must
                // never itself claim leadership (its own empty, unpersisted log correctly can't win a
                // vote against peers with real log entries; see this test's own class-level rationale).
                waitUntil(Duration.ofSeconds(10), () -> restartedA.currentLeader().isPresent());
                assertEquals(newLeader.currentLeader().orElseThrow(), restartedA.currentLeader().orElseThrow(),
                        "the restarted node must agree on who the real current leader is");
            }
        } finally {
            clusterB.close();
            clusterC.close();
        }
    }

    private static Map<NodeId, NodeAddress> withoutSelf(Map<NodeId, NodeAddress> addresses, NodeId self) {
        Map<NodeId, NodeAddress> copy = new HashMap<>(addresses);
        copy.remove(self);
        return copy;
    }
}
