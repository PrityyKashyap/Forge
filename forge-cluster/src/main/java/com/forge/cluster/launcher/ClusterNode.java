package com.forge.cluster.launcher;

import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.cluster.leadership.PartitionLeadership;
import com.forge.cluster.leadership.ReplicationFollowerCoordinator;
import com.forge.cluster.recovery.SnapshotServer;
import com.forge.cluster.recovery.StaleReplicaRecovery;
import com.forge.cluster.replication.ReplicationServer;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * The actual wiring behind {@link ClusterNodeMain} — every component one
 * FORGE cluster node needs, started and connected exactly like {@code
 * FailoverReplicationIntegrationTest} already proves works, extracted into
 * its own {@link #start} so it's directly testable in-process (real ports,
 * real sockets, no subprocess or classpath gymnastics needed) rather than
 * only reachable by actually spawning a JVM.
 */
public final class ClusterNode implements Closeable {

    private static final Duration ELECTION_TIMEOUT_MIN = Duration.ofMillis(300);
    private static final Duration ELECTION_TIMEOUT_MAX = Duration.ofMillis(500);
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(75);
    private static final Duration TICK_INTERVAL = Duration.ofMillis(30);
    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration LEASE_DURATION = ELECTION_TIMEOUT_MIN;
    private static final Duration REPLICATION_ACK_INTERVAL = Duration.ofMillis(200);
    private static final Duration COORDINATOR_POLL_INTERVAL = Duration.ofMillis(200);
    private static final String PARTITION_ID = "p0";
    private static final long FLUSH_THRESHOLD_BYTES = 4L * 1024 * 1024; // matches ConcurrentLsmKeyValueStore's own default

    private final ConcurrentLsmKeyValueStore store;
    private final ReplicationServer replicationServer;
    private final SnapshotServer snapshotServer;
    private final RaftCluster raftCluster;
    private final ReplicationFollowerCoordinator coordinator;
    private final ForgeServer server;

    private ClusterNode(ConcurrentLsmKeyValueStore store, ReplicationServer replicationServer,
            SnapshotServer snapshotServer, RaftCluster raftCluster, ReplicationFollowerCoordinator coordinator,
            ForgeServer server) {
        this.store = store;
        this.replicationServer = replicationServer;
        this.snapshotServer = snapshotServer;
        this.raftCluster = raftCluster;
        this.coordinator = coordinator;
        this.server = server;
    }

    /**
     * Starts every component for {@code selfId} — a single-partition
     * (id {@code "p0"}) replica of the cluster described by {@code specs}
     * (every node in that list, including {@code selfId} itself). Raft
     * persistent state is written under {@code dataDirectory}.
     */
    public static ClusterNode start(List<NodeSpec> specs, NodeId selfId, Path dataDirectory) throws IOException {
        Objects.requireNonNull(specs, "specs must not be null");
        Objects.requireNonNull(selfId, "selfId must not be null");
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        NodeSpec self = specs.stream().filter(spec -> spec.id().equals(selfId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("node id '" + selfId + "' not found in cluster config"));

        Map<NodeId, NodeAddress> raftPeerAddresses = new HashMap<>();
        Map<NodeId, NodeAddress> replicationAddresses = new HashMap<>();
        for (NodeSpec spec : specs) {
            replicationAddresses.put(spec.id(), new NodeAddress(spec.host(), spec.replicationPort()));
            if (!spec.id().equals(selfId)) {
                raftPeerAddresses.put(spec.id(), new NodeAddress(spec.host(), spec.raftPort()));
            }
        }

        ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDirectory, FLUSH_THRESHOLD_BYTES);
        try {
            ReplicationServer replicationServer = new ReplicationServer(store, self.replicationPort());
            SnapshotServer snapshotServer = new SnapshotServer(store, self.snapshotPort());
            Path raftStateFile = dataDirectory.resolve("raft-state");
            RaftCluster raftCluster = new RaftCluster(selfId, raftPeerAddresses, self.raftPort(), Clock.systemUTC(),
                    ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL, TICK_INTERVAL, RPC_TIMEOUT,
                    LEASE_DURATION, new Random(), raftStateFile);
            ReplicationFollowerCoordinator coordinator = new ReplicationFollowerCoordinator(selfId, raftCluster,
                    store, replicationAddresses, REPLICATION_ACK_INTERVAL, COORDINATOR_POLL_INTERVAL);
            PartitionLeadership leadership = new PartitionLeadership(PARTITION_ID, raftCluster, store);
            ForgeServer server = new ForgeServer(store, key -> true, leadership, self.clientPort());
            return new ClusterNode(store, replicationServer, snapshotServer, raftCluster, coordinator, server);
        } catch (IOException | RuntimeException e) {
            store.close();
            throw e;
        }
    }

    /**
     * Offline resync tool, matching {@code forge-server}'s own {@code status}
     * subcommand precedent: opens {@code dataDirectory} directly (must NOT be
     * run against a directory a live {@link #start}ed node already has open —
     * two processes cannot safely share one WAL/SSTable directory), wipes it,
     * and reloads it completely from {@code sourceHost:sourceSnapshotPort}'s
     * live {@link SnapshotServer}. See {@link StaleReplicaRecovery}'s class
     * Javadoc for exactly when this is needed and what it guarantees.
     */
    public static void resync(Path dataDirectory, String sourceHost, int sourceSnapshotPort) throws IOException {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(sourceHost, "sourceHost must not be null");
        ConcurrentLsmKeyValueStore staleStore = new ConcurrentLsmKeyValueStore(dataDirectory, FLUSH_THRESHOLD_BYTES);
        ConcurrentLsmKeyValueStore resynced = StaleReplicaRecovery.resyncFromSnapshot(
                staleStore, dataDirectory, FLUSH_THRESHOLD_BYTES, sourceHost, sourceSnapshotPort);
        resynced.close();
    }

    public int clientPort() {
        return server.port();
    }

    public int raftPort() {
        return raftCluster.port();
    }

    public RaftCluster raftCluster() {
        return raftCluster;
    }

    public ConcurrentLsmKeyValueStore store() {
        return store;
    }

    @Override
    public void close() {
        closeQuietly(server);
        closeQuietly(coordinator);
        closeQuietly(raftCluster);
        closeQuietly(replicationServer);
        closeQuietly(snapshotServer);
        closeQuietly(store);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // best-effort shutdown; a component already closed or mid-failure must not block the others
        }
    }
}
