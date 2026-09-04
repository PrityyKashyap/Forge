package com.forge.cluster.leadership;

import com.forge.client.ForgeClient;
import com.forge.client.ForgeServerException;
import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.chaos.FaultInjectingTcpProxy;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.cluster.consensus.RaftRole;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The Phase 15 mandatory test: proves the "difficult case" explicitly named
 * in the mandate — an old leader that is <b>alive</b> but <b>stale</b>,
 * never told about a newer term because it is genuinely, bidirectionally
 * network-partitioned away from the rest of the cluster (not killed, not
 * disconnected via {@code close()}). Uses {@link FaultInjectingTcpProxy}
 * (Phase 11) on every A&harr;{B,C} link so A's {@link RaftCluster} object
 * stays fully alive and ticking throughout, exactly as a real partitioned
 * process would.
 *
 * <h2>Why this needs four proxies</h2>
 * A real network partition is bidirectional and affects both peer links.
 * B and C are left directly connected to each other (unaffected), so they
 * retain a majority (2 of 3) and can elect a new leader without A —
 * exactly the "B+C form a majority" scenario the mandate also requires
 * coverage for.
 */
class StaleLeaderFencingIntegrationTest {

    // A's election timeout is deliberately much shorter than B/C's, so A always times out
    // and wins the very first election deterministically (B/C are still passive followers
    // with no timeout of their own yet fired, so they simply grant A's vote) -- this test is
    // about what happens AFTER A is legitimately leader, not about who wins a fair race.
    private static final Duration A_ELECTION_MIN = Duration.ofMillis(40);
    private static final Duration A_ELECTION_MAX = Duration.ofMillis(60);
    private static final Duration BC_ELECTION_MIN = Duration.ofMillis(400);
    private static final Duration BC_ELECTION_MAX = Duration.ofMillis(600);
    private static final Duration HEARTBEAT = Duration.ofMillis(25);
    private static final Duration TICK = Duration.ofMillis(10);
    private static final Duration RPC_TIMEOUT = Duration.ofMillis(500);
    private static final Duration LEASE = A_ELECTION_MIN;

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
    @Timeout(60)
    void aliveButStaleLeaderIsFencedAfterAGenuineNetworkPartitionAndStaysFencedAfterHealing(@TempDir Path baseDir)
            throws Exception {
        NodeId a = new NodeId("stale-a");
        NodeId b = new NodeId("stale-b");
        NodeId c = new NodeId("stale-c");

        int aRaftPort = freePort();
        int bRaftPort = freePort();
        int cRaftPort = freePort();

        // Proxies for every A<->{B,C} Raft link; B<->C talks directly (unaffected by the partition).
        try (FaultInjectingTcpProxy proxyAtoB = new FaultInjectingTcpProxy("localhost", bRaftPort, freePort());
             FaultInjectingTcpProxy proxyAtoC = new FaultInjectingTcpProxy("localhost", cRaftPort, freePort());
             FaultInjectingTcpProxy proxyBtoA = new FaultInjectingTcpProxy("localhost", aRaftPort, freePort());
             FaultInjectingTcpProxy proxyCtoA = new FaultInjectingTcpProxy("localhost", aRaftPort, freePort())) {

            Map<NodeId, NodeAddress> addressesForA = Map.of(
                    b, new NodeAddress("localhost", proxyAtoB.port()),
                    c, new NodeAddress("localhost", proxyAtoC.port()));
            Map<NodeId, NodeAddress> addressesForB = Map.of(
                    a, new NodeAddress("localhost", proxyBtoA.port()),
                    c, new NodeAddress("localhost", cRaftPort));
            Map<NodeId, NodeAddress> addressesForC = Map.of(
                    a, new NodeAddress("localhost", proxyCtoA.port()),
                    b, new NodeAddress("localhost", bRaftPort));

            try (RaftCluster raftA = new RaftCluster(a, addressesForA, aRaftPort, Clock.systemUTC(),
                            A_ELECTION_MIN, A_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(1));
                    RaftCluster raftB = new RaftCluster(b, addressesForB, bRaftPort, Clock.systemUTC(),
                            BC_ELECTION_MIN, BC_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(2));
                    RaftCluster raftC = new RaftCluster(c, addressesForC, cRaftPort, Clock.systemUTC(),
                            BC_ELECTION_MIN, BC_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(3));
                    ConcurrentLsmKeyValueStore storeA = new ConcurrentLsmKeyValueStore(baseDir.resolve("a"))) {

                PartitionLeadership leadershipA = new PartitionLeadership("p0", raftA, storeA);
                try (ForgeServer realServerA = new ForgeServer(storeA, key -> true, leadershipA, 0)) {

                    // Step 1: A is leader in term T. A's much shorter election timeout guarantees
                    // it times out and wins the very first election deterministically.
                    waitUntil(Duration.ofSeconds(10), () -> raftA.isConfirmedLeader());
                    long termT = raftA.currentTerm();
                    waitUntil(Duration.ofSeconds(2), leadershipA::canAcceptWrites);

                    try (ForgeClient clientToA = ForgeClient.connect("localhost", realServerA.port())) {
                        clientToA.put("k1", "v1".getBytes());
                    }
                    assertTrue(storeA.get("k1").isPresent(), "a normal write to the legitimate leader must succeed");

                    // Step 2: A becomes partitioned/unreachable (bidirectionally, genuinely — A's
                    // RaftCluster stays fully alive and ticking throughout).
                    proxyAtoB.partition();
                    proxyAtoC.partition();
                    proxyBtoA.partition();
                    proxyCtoA.partition();

                    // Step 3: B or C becomes leader in a strictly higher term, using their own
                    // still-intact majority (2 of 3).
                    List<RaftCluster> survivors = List.of(raftB, raftC);
                    waitUntil(Duration.ofSeconds(10),
                            () -> survivors.stream().anyMatch(RaftCluster::isConfirmedLeader));
                    RaftCluster newLeader = survivors.stream().filter(RaftCluster::isConfirmedLeader).findFirst()
                            .orElseThrow();
                    assertTrue(newLeader.currentTerm() > termT, "the new leader's term must be strictly higher");

                    // A, meanwhile, never heard a single message about this — but its lease
                    // expires anyway, purely from the passage of time (see RaftNode.hasRecentQuorumContact).
                    waitUntil(Duration.ofSeconds(5), () -> !raftA.canServeAuthoritatively());
                    assertTrue(raftA.isConfirmedLeader(),
                            "A's own isConfirmedLeader() alone never learns anything is wrong -- this is the gap the lease closes");

                    // Step 4 + 5: A later receives a client write and MUST reject it.
                    try (ForgeClient clientToA = ForgeClient.connect("localhost", realServerA.port())) {
                        ForgeServerException e = assertThrows(ForgeServerException.class,
                                () -> clientToA.put("k2", "v2".getBytes()));
                        assertEquals(ProtocolConstants.ERROR_NOT_LEADER, e.errorCode());
                    }
                    assertTrue(storeA.get("k2").isEmpty(), "a fenced write must never reach the stale leader's store");

                    // Step 6: B (or C) remains authoritative and can actually serve a write.
                    assertTrue(newLeader.canServeAuthoritatively(), "the legitimate new leader must remain authoritative");

                    // Step 7: A reconnects (the partition heals, in both directions, on every link).
                    proxyAtoB.heal();
                    proxyAtoC.heal();
                    proxyBtoA.heal();
                    proxyCtoA.heal();

                    // Step 8: A discovers the newer term (via a real AppendEntries/RequestVote it
                    // can now actually receive) and steps down to FOLLOWER.
                    waitUntil(Duration.ofSeconds(5), () -> raftA.currentTerm() >= newLeader.currentTerm());
                    waitUntil(Duration.ofSeconds(5), () -> raftA.role() == RaftRole.FOLLOWER);

                    // Step 9: A still cannot act as leader -- neither immediately after healing,
                    // nor (giving the cluster a moment to settle) shortly after.
                    assertFalse(raftA.canServeAuthoritatively());
                    Thread.sleep(HEARTBEAT.toMillis() * 3);
                    assertFalse(raftA.canServeAuthoritatively(), "A must not spontaneously re-claim leadership after healing");
                    try (ForgeClient clientToA = ForgeClient.connect("localhost", realServerA.port())) {
                        ForgeServerException e = assertThrows(ForgeServerException.class,
                                () -> clientToA.put("k3", "v3".getBytes()));
                        assertEquals(ProtocolConstants.ERROR_NOT_LEADER, e.errorCode());
                    }
                }
            }
        }
    }
}
