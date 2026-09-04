package com.forge.cluster.leadership;

import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.cluster.replication.ReplicationFollower;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Phase 15: watches {@link RaftCluster}'s current leader and keeps this
 * node's {@link ReplicationFollower} pointed at whoever it actually is —
 * the piece that makes a Raft leadership change actually move the data
 * plane, not just the control plane. Same "passive state, external
 * scheduler polls it" shape as {@code HeartbeatService}/{@code RaftCluster}
 * itself.
 *
 * <h2>What this automatically handles</h2>
 * <ul>
 *   <li>This node becomes (or stays) leader: any running {@code ReplicationFollower}
 *       is closed (a leader does not follow anyone). {@code ReplicationServer}
 *       lifecycle is <em>not</em> managed here — see {@code docs/ARCHITECTURE.md}
 *       §3.12 for why every node simply runs one continuously, gated
 *       implicitly by fencing rather than started/stopped on leadership change.</li>
 *   <li>A different node is (or becomes) the confirmed leader: this node's
 *       store is ratcheted into that leader's current term's sequence band
 *       ({@link SequenceEpochs}) and a fresh {@code ReplicationFollower} is
 *       connected to it — {@code ReplicationFollower} never auto-reconnects
 *       by design (Phase 9), so a fresh instance is constructed exactly as
 *       {@code ChaosScenarioTest}'s Scenario C already established is the
 *       correct pattern.</li>
 * </ul>
 *
 * <h2>What this deliberately does NOT automatically handle</h2>
 * The very first time this coordinator instance considers following anyone
 * (i.e. before it has ever successfully done so) — which in practice means
 * "just after this process started, possibly after a restart" — it checks
 * whether the store already has real local data (a nonzero
 * {@code lastAppliedSequenceNumber()}) whose implied term
 * ({@link SequenceEpochs#impliedTerm}) is behind the current leader's. If
 * so, that data's provenance is untrusted (most likely a former leader's
 * orphaned, never-replicated writes — see {@code StaleReplicaRecovery}'s
 * Javadoc for exactly why those can't be reconciled by simply tailing
 * forward) and incremental catch-up is refused. Once this coordinator has
 * successfully followed at least once, every later term change is treated
 * as an ordinary live failover instead — a node that only ever applied
 * exactly what a legitimate leader sent it has no divergence risk to guard
 * against, no matter how many elections happen while it keeps running.
 * This coordinator only ever <em>detects</em> the risky case
 * ({@link #needsFullResync()}) and logs a warning; it does not perform the
 * resync itself, since doing so would require replacing this node's
 * {@code ConcurrentLsmKeyValueStore} instance out from under whatever else
 * (a live {@code ForgeServer}) holds a reference to it — a bigger
 * structural change than this phase's scope. See {@code StaleReplicaRecovery},
 * which a caller invokes explicitly (see the Phase 15 rejoin tests for
 * exactly this sequence).
 */
public final class ReplicationFollowerCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReplicationFollowerCoordinator.class);

    private final NodeId selfId;
    private final RaftCluster raftCluster;
    private final ConcurrentLsmKeyValueStore store;
    private final Map<NodeId, NodeAddress> replicationAddresses;
    private final Duration ackInterval;
    private final ScheduledExecutorService scheduler;

    private volatile ReplicationFollower currentFollower;
    private volatile NodeId currentlyFollowing;
    private volatile long currentlyFollowingTerm = -1;
    private final AtomicBoolean needsFullResync = new AtomicBoolean(false);
    /**
     * True once this coordinator has, within its own process lifetime,
     * already established at least one real {@code ReplicationFollower}
     * connection — the discriminator between "I've been continuously,
     * correctly following the replication stream this whole time, so a
     * failover to a new leader is a perfectly normal event" and "I don't
     * yet know whether my existing local data (if any — most likely from
     * before a restart) is trustworthy." Never reset once true: it answers
     * "did *this instance* ever prove itself a legitimate follower," not
     * "am I currently following someone."
     */
    private volatile boolean hasEverFollowedSuccessfully = false;

    public ReplicationFollowerCoordinator(NodeId selfId, RaftCluster raftCluster, ConcurrentLsmKeyValueStore store,
            Map<NodeId, NodeAddress> replicationAddresses, Duration ackInterval, Duration pollInterval) {
        this.selfId = Objects.requireNonNull(selfId, "selfId must not be null");
        this.raftCluster = Objects.requireNonNull(raftCluster, "raftCluster must not be null");
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.replicationAddresses = Map.copyOf(Objects.requireNonNull(replicationAddresses, "replicationAddresses must not be null"));
        this.ackInterval = Objects.requireNonNull(ackInterval, "ackInterval must not be null");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().name("replication-follower-coordinator-" + selfId).unstarted(r));
        scheduler.scheduleAtFixedRate(this::tick, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * True once this coordinator has observed a leader whose term is ahead
     * of this node's own implied term — a full {@code StaleReplicaRecovery}
     * resync is needed before this node can safely resume as a follower.
     * See class Javadoc.
     */
    public boolean needsFullResync() {
        return needsFullResync.get();
    }

    /** Cleared by a caller once it has performed the resync (e.g. via {@code StaleReplicaRecovery}) — see the Phase 15 rejoin tests. */
    public void clearNeedsFullResyncFlag() {
        needsFullResync.set(false);
    }

    private synchronized void tick() {
        Optional<NodeId> leader = raftCluster.currentLeader();
        if (leader.isEmpty()) {
            return; // no known leader yet — nothing to redirect to
        }
        if (leader.get().equals(selfId)) {
            stopFollowing("this node is now the leader");
            return;
        }

        NodeId leaderId = leader.get();
        long leaderTerm = raftCluster.currentTerm();
        if (leaderId.equals(currentlyFollowing) && leaderTerm == currentlyFollowingTerm && currentFollower != null) {
            return; // already correctly following this exact leader for this exact term
        }

        // The resync question only makes sense the first time this coordinator (this process
        // lifetime) considers following anyone: once we've already proven ourselves a
        // continuously-correct follower (hasEverFollowedSuccessfully), every subsequent
        // failover to a new leader is a perfectly ordinary event — we only ever applied
        // exactly what a legitimate leader sent us, never originated anything of our own, so
        // there is no divergence risk to protect against, regardless of how many terms have
        // passed. The risk this check exists for is specifically: local data of unknown
        // provenance already on disk when THIS instance starts up (most likely surviving a
        // restart) that implies an older term than the cluster's current one.
        long lastApplied = store.lastAppliedSequenceNumber();
        long impliedTerm = SequenceEpochs.impliedTerm(lastApplied);
        if (!hasEverFollowedSuccessfully && lastApplied > 1 && impliedTerm < leaderTerm) {
            // Deliberately conservative — see class Javadoc's "does NOT automatically handle" section.
            if (needsFullResync.compareAndSet(false, true)) {
                log.warn("{}: leader {} is on term {} but my own data implies term {} — a full snapshot resync "
                                + "is needed before I can safely follow again; not attempting incremental catch-up",
                        selfId, leaderId, leaderTerm, impliedTerm);
            }
            return;
        }

        NodeAddress address = replicationAddresses.get(leaderId);
        if (address == null) {
            log.warn("{}: no known replication address for leader {}; cannot follow it", selfId, leaderId);
            return;
        }

        stopFollowing("switching to leader " + leaderId + " for term " + leaderTerm);
        store.ensureNextSequenceNumberAtLeast(SequenceEpochs.bandStart(leaderTerm));
        try {
            currentFollower = new ReplicationFollower(selfId, address.host(), address.port(), store, ackInterval);
            currentlyFollowing = leaderId;
            currentlyFollowingTerm = leaderTerm;
            hasEverFollowedSuccessfully = true;
            log.info("{}: now following leader {} (term {}) from sequence {}",
                    selfId, leaderId, leaderTerm, store.lastAppliedSequenceNumber());
        } catch (IOException e) {
            log.debug("{}: failed to connect to leader {} at {}; will retry next poll", selfId, leaderId, address, e);
            currentlyFollowing = null;
            currentlyFollowingTerm = -1;
        }
    }

    private void stopFollowing(String reason) {
        if (currentFollower != null) {
            log.info("{}: stopping current replication follower ({})", selfId, reason);
            currentFollower.close();
            currentFollower = null;
        }
        currentlyFollowing = null;
        currentlyFollowingTerm = -1;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("{}: replication-follower-coordinator scheduler did not terminate within the shutdown grace period", selfId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (this) {
            if (currentFollower != null) {
                currentFollower.close();
                currentFollower = null;
            }
        }
    }
}
