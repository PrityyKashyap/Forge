package com.forge.cluster.leadership;

import com.forge.cluster.NodeId;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.server.WriteAuthority;
import com.forge.storage.ConcurrentLsmKeyValueStore;

import java.util.Objects;
import java.util.Optional;

/**
 * Phase 15: the bridge between Raft's control plane and one partition's
 * data-plane write acceptance — the concrete {@link WriteAuthority} a
 * {@code ForgeServer} for a Raft-managed partition is constructed with.
 *
 * <h2>What "authoritative" means here, precisely</h2>
 * {@link #canAcceptWrites()} is backed by {@link RaftCluster#canServeAuthoritatively()}
 * — not {@link RaftCluster#isConfirmedLeader()} — because a node that won
 * an election and is now silently partitioned away from every peer would
 * stay {@code isConfirmedLeader()} forever with no one ever telling it
 * otherwise. {@code canServeAuthoritatively()}'s leader-lease check is what
 * actually bounds how long such a node can keep accepting writes after
 * losing real contact with a majority. See {@code RaftNode}'s Javadoc for
 * the exact mechanism and {@code docs/CONSISTENCY.md} §5 for the guarantee
 * this provides (and its precise, bounded limits — this is fencing with a
 * bounded staleness window, not an instantaneous, zero-latency guarantee).
 *
 * <h2>The sequence-band ratchet</h2>
 * The very first time {@link #canAcceptWrites()} observes a term it hasn't
 * seen before, it ratchets {@code store}'s sequence numbering into that
 * term's band ({@link SequenceEpochs}) <em>before</em> returning — done
 * inside one {@code synchronized} method so every concurrent caller
 * (one per client connection) is guaranteed the ratchet for term T has
 * already happened before any of them can observe "yes, accept this write
 * under term T." This is what makes the very first write after a failover
 * safe, not just eventually-consistent-with-a-race.
 */
public final class PartitionLeadership implements WriteAuthority {

    private final String partitionId;
    private final RaftCluster raftCluster;
    private final ConcurrentLsmKeyValueStore store;
    private long lastBumpedTerm = -1;

    public PartitionLeadership(String partitionId, RaftCluster raftCluster, ConcurrentLsmKeyValueStore store) {
        this.partitionId = Objects.requireNonNull(partitionId, "partitionId must not be null");
        this.raftCluster = Objects.requireNonNull(raftCluster, "raftCluster must not be null");
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    public String partitionId() {
        return partitionId;
    }

    @Override
    public synchronized boolean canAcceptWrites() {
        if (!raftCluster.canServeAuthoritatively()) {
            return false;
        }
        long term = raftCluster.currentTerm();
        if (term > lastBumpedTerm) {
            store.ensureNextSequenceNumberAtLeast(SequenceEpochs.bandStart(term));
            lastBumpedTerm = term;
        }
        return true;
    }

    @Override
    public long currentTerm() {
        return raftCluster.currentTerm();
    }

    @Override
    public Optional<String> currentLeaderHint() {
        return raftCluster.currentLeader().map(NodeId::value);
    }
}
