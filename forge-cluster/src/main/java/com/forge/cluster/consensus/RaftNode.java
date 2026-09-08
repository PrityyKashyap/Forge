package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * Phase 14: a real Raft consensus state machine — terms, leader election by
 * majority vote, and log replication/commit via AppendEntries — implementing
 * the core of the Raft paper (Ongaro &amp; Ousterhout, "In Search of an
 * Understandable Consensus Algorithm"), Figure 2's rules specifically.
 *
 * <h2>What this is for for FORGE, precisely — and what it is deliberately not</h2>
 * This is <b>not</b> a rename of Phase 9's leader-follower replication.
 * Phase 9's {@code ReplicationServer}/{@code ReplicationFollower} remain
 * exactly what they were: the data plane, streaming actual WAL records for
 * actual KV writes. Raft here is the <b>control plane</b>: it decides, via
 * real elections with real majority quorums, which node is currently the
 * leader for a partition, and its log carries nothing but a single no-op
 * "I am the leader for this term" marker per election (see {@link LogEntry}).
 * {@code RaftCluster} treats a <em>committed</em> no-op — meaning a majority
 * of the cluster has durably (for this phase's disclosed definition of
 * "durably," see below) acknowledged it — as the trigger to actually start
 * treating {@code selfId} as the data-plane leader. Real KV commands are
 * never proposed through this log; that would either duplicate Phase 9's
 * already-correct, already-tested replication or require ripping it out,
 * neither of which this phase's mandate calls for.
 *
 * <h2>Design, matching {@code FailureDetector}'s established shape</h2>
 * A <b>passive, clock-driven state machine with no threads and no I/O of
 * its own</b> — exactly {@code FailureDetector}'s split, for the same
 * reason: it makes every election, split-vote, and log-inconsistency
 * scenario fully testable with a fake {@link Clock} and a seeded
 * {@link Random}, no real sleeping, no real sockets. {@link #tick()} and
 * the {@code handle*} methods never send anything themselves; they return
 * {@link RaftAction}s describing what should be sent, which a driver (in
 * production, {@link RaftCluster}) actually transmits and feeds responses
 * back through {@link #handleRequestVoteResponse}/{@link #handleAppendEntriesResponse}.
 *
 * <h2>Persistent state (post-Phase-15 audit addition) — and what's still deliberately not persisted</h2>
 * The Raft paper requires {@code currentTerm}, {@code votedFor}, and the
 * log to be persisted to stable storage <em>before responding to RPCs</em>,
 * so that a crashed-and-restarted node can never violate the "at most one
 * leader per term" safety property by forgetting a vote it already cast.
 * {@code currentTerm}/{@code votedFor} now are: every mutation of either
 * field calls {@link RaftPersistenceListener#onPersistentStateChanged}
 * <em>before</em> the in-memory fields themselves change, and a caller
 * seeds a restarted node's initial values from {@link RaftPersistentState}
 * (see {@code RaftCluster}'s persistence-enabled constructor). If
 * persistence fails, the in-memory fields are left completely untouched and
 * the failure propagates like a dropped RPC — this node simply doesn't
 * grant the vote / doesn't advance its term this round, never fabricating
 * durability it doesn't have.
 * <p>The log is <b>deliberately still not persisted</b> — see {@link
 * RaftPersistentState}'s class Javadoc for exactly why that's safe here
 * (this log only ever carries disposable no-op leadership markers, never
 * real data) and not merely deferred out of laziness. A node that crashes
 * and restarts still rejoins with an empty log but now with its correct,
 * durable {@code currentTerm}/{@code votedFor} — the "at most one vote per
 * term, even across a crash" safety property is closed; as a
 * <em>follower</em>, the log's own loss on restart costs nothing beyond
 * what the existing {@code nextIndex} back-off already handles for any
 * lagging follower. As a would-be <em>candidate</em>, though, the empty
 * log correctly (per Raft's own up-to-date-log safety rule) can never win
 * a vote against a peer with a non-empty log — see {@link
 * RaftPersistentState}'s Javadoc for the real, disclosed liveness
 * consequence this has in a bare-minimum-quorum (2-node) cluster.
 *
 * <p>Thread-safe: every public method is {@code synchronized} — control-plane
 * traffic is low-volume, so one lock is simple and sufficient, same
 * reasoning {@code FailureDetector} already documents for itself.
 */
public final class RaftNode {

    private final NodeId selfId;
    private final Set<NodeId> peers;
    private final Clock clock;
    private final Duration electionTimeoutMin;
    private final Duration electionTimeoutMax;
    private final Duration heartbeatInterval;
    private final Random random;
    private final RaftPersistenceListener persistenceListener;

    // Persistent state per the paper (currentTerm/votedFor only — see class Javadoc for the log).
    private long currentTerm;
    private NodeId votedFor;
    private final List<LogEntry> log = new ArrayList<>();

    // Volatile state on all servers.
    private RaftRole role = RaftRole.FOLLOWER;
    private NodeId currentLeader = null;
    private long commitIndex = 0;
    private Instant lastElectionResetTime;
    private Duration currentElectionTimeout;

    // Volatile state while CANDIDATE.
    private Set<NodeId> votesReceivedThisElection = new HashSet<>();

    // Volatile state while LEADER (re-initialized on election), per the paper.
    private Map<NodeId, Long> nextIndex = Map.of();
    private Map<NodeId, Long> matchIndex = Map.of();
    private Instant lastHeartbeatSentTime;
    /** The log index of this term's own leader-announcement no-op — see {@link #isConfirmedLeader()}. */
    private long leaderNoOpIndex = -1;
    /**
     * Phase 15: when each peer last successfully acknowledged an
     * AppendEntries in the <em>current</em> term — the raw material for
     * {@link #hasRecentQuorumContact}, a leader-lease check distinct from
     * {@link #isConfirmedLeader()}. Reset empty on every election (a stale
     * ack from a previous term must never count towards this term's lease).
     */
    private Map<NodeId, Instant> lastAckTime = Map.of();

    public RaftNode(NodeId selfId, Set<NodeId> peers, Clock clock, Duration electionTimeoutMin,
            Duration electionTimeoutMax, Duration heartbeatInterval, Random random) {
        this(selfId, peers, clock, electionTimeoutMin, electionTimeoutMax, heartbeatInterval, random,
                RaftPersistenceListener.NONE, 0L, null);
    }

    /**
     * As the other constructor, plus persistence wiring: {@code
     * persistenceListener} is invoked synchronously on every future
     * {@code currentTerm}/{@code votedFor} change (see class Javadoc), and
     * {@code initialTerm}/{@code initialVotedFor} seed this node's starting
     * values — a caller restarting a real node passes what {@link
     * RaftPersistentState#load} returned; a caller with nothing durable yet
     * (or not wiring persistence at all, like every test) passes
     * {@code (RaftPersistenceListener.NONE, 0L, null)}, exactly the other
     * constructor's behavior.
     */
    public RaftNode(NodeId selfId, Set<NodeId> peers, Clock clock, Duration electionTimeoutMin,
            Duration electionTimeoutMax, Duration heartbeatInterval, Random random,
            RaftPersistenceListener persistenceListener, long initialTerm, NodeId initialVotedFor) {
        this.selfId = Objects.requireNonNull(selfId, "selfId must not be null");
        this.peers = Set.copyOf(Objects.requireNonNull(peers, "peers must not be null"));
        if (peers.contains(selfId)) {
            throw new IllegalArgumentException("peers must not include selfId");
        }
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.electionTimeoutMin = requirePositive(electionTimeoutMin, "electionTimeoutMin");
        this.electionTimeoutMax = requirePositive(electionTimeoutMax, "electionTimeoutMax");
        if (electionTimeoutMax.compareTo(electionTimeoutMin) < 0) {
            throw new IllegalArgumentException("electionTimeoutMax must be >= electionTimeoutMin");
        }
        this.heartbeatInterval = requirePositive(heartbeatInterval, "heartbeatInterval");
        this.random = Objects.requireNonNull(random, "random must not be null");
        this.persistenceListener = Objects.requireNonNull(persistenceListener, "persistenceListener must not be null");
        this.currentTerm = initialTerm;
        this.votedFor = initialVotedFor;

        Instant now = clock.instant();
        this.lastElectionResetTime = now;
        this.currentElectionTimeout = randomElectionTimeout();
        this.lastHeartbeatSentTime = now;
    }

    private static Duration requirePositive(Duration d, String name) {
        Objects.requireNonNull(d, name + " must not be null");
        if (d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return d;
    }

    // =====================================================================
    // Driven by the caller's clock — never sleeps, never blocks.
    // =====================================================================

    /**
     * Re-evaluates timers against {@link Clock#instant()} right now: as
     * {@link RaftRole#LEADER}, sends a heartbeat/AppendEntries to every peer
     * once {@code heartbeatInterval} has elapsed since the last one; as
     * {@link RaftRole#FOLLOWER}/{@link RaftRole#CANDIDATE}, starts a new
     * election once the current (randomized) election timeout has elapsed
     * with no qualifying reset (a granted vote, or a valid AppendEntries
     * from a current-term leader).
     */
    public synchronized List<RaftAction> tick() throws IOException {
        Instant now = clock.instant();
        if (role == RaftRole.LEADER) {
            if (Duration.between(lastHeartbeatSentTime, now).compareTo(heartbeatInterval) >= 0) {
                lastHeartbeatSentTime = now;
                return buildAppendEntriesForAllPeers();
            }
            return List.of();
        }
        if (Duration.between(lastElectionResetTime, now).compareTo(currentElectionTimeout) >= 0) {
            return startElection(now);
        }
        return List.of();
    }

    private List<RaftAction> startElection(Instant now) throws IOException {
        long newTerm = currentTerm + 1;
        persistenceListener.onPersistentStateChanged(newTerm, selfId);
        currentTerm = newTerm;
        role = RaftRole.CANDIDATE;
        votedFor = selfId;
        currentLeader = null;
        votesReceivedThisElection = new HashSet<>();
        votesReceivedThisElection.add(selfId);
        resetElectionTimer(now);

        if (peers.isEmpty()) {
            return becomeLeader(now);
        }
        RequestVoteRequest request = new RequestVoteRequest(currentTerm, selfId, lastLogIndex(), lastLogTerm());
        List<RaftAction> actions = new ArrayList<>(peers.size());
        for (NodeId peer : peers) {
            actions.add(new RaftAction.SendRequestVote(peer, request));
        }
        return actions;
    }

    private List<RaftAction> becomeLeader(Instant now) {
        role = RaftRole.LEADER;
        currentLeader = selfId;
        // The no-op must be appended BEFORE nextIndex/matchIndex are initialized: nextIndex's
        // standard optimistic default is "assume every peer already has everything through our
        // last log index" (lastLogIndex + 1, i.e. nothing pending) — computing it against the
        // pre-no-op log would instead default every peer to "needs the no-op," turning what
        // should be an optimistic assumption (corrected only on actual rejection) into the
        // pessimistic one on every single election.
        log.add(new LogEntry(currentTerm, LogEntry.NO_OP));
        leaderNoOpIndex = lastLogIndex();

        Map<NodeId, Long> newNextIndex = new HashMap<>();
        Map<NodeId, Long> newMatchIndex = new HashMap<>();
        for (NodeId peer : peers) {
            newNextIndex.put(peer, lastLogIndex() + 1);
            newMatchIndex.put(peer, 0L);
        }
        nextIndex = newNextIndex;
        matchIndex = newMatchIndex;
        lastAckTime = new HashMap<>();
        lastHeartbeatSentTime = now;
        recomputeCommitIndex();
        return buildAppendEntriesForAllPeers();
    }

    private void resetElectionTimer(Instant now) {
        lastElectionResetTime = now;
        currentElectionTimeout = randomElectionTimeout();
    }

    private Duration randomElectionTimeout() {
        long minMillis = electionTimeoutMin.toMillis();
        long maxMillis = electionTimeoutMax.toMillis();
        long spread = maxMillis - minMillis;
        long chosen = spread <= 0 ? minMillis : minMillis + (long) (random.nextDouble() * spread);
        return Duration.ofMillis(chosen);
    }

    private List<RaftAction> buildAppendEntriesForAllPeers() {
        List<RaftAction> actions = new ArrayList<>(peers.size());
        for (NodeId peer : peers) {
            actions.add(new RaftAction.SendAppendEntries(peer, buildAppendEntriesFor(peer)));
        }
        return actions;
    }

    private AppendEntriesRequest buildAppendEntriesFor(NodeId peer) {
        long ni = nextIndex.getOrDefault(peer, lastLogIndex() + 1);
        long prevLogIndex = ni - 1;
        long prevLogTerm = termAt(prevLogIndex);
        List<LogEntry> entries = ni <= lastLogIndex()
                ? List.copyOf(log.subList((int) (ni - 1), log.size()))
                : List.of();
        return new AppendEntriesRequest(currentTerm, selfId, prevLogIndex, prevLogTerm, entries, commitIndex);
    }

    // =====================================================================
    // Incoming RPCs
    // =====================================================================

    /** Handles an incoming RequestVote RPC — Figure 2's rules, exactly. */
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) throws IOException {
        Objects.requireNonNull(request, "request must not be null");
        if (request.term() > currentTerm) {
            stepDownToFollower(request.term());
        }
        if (request.term() < currentTerm) {
            return new RequestVoteResponse(currentTerm, false);
        }

        boolean canVote = votedFor == null || votedFor.equals(request.candidateId());
        boolean upToDate = isAtLeastAsUpToDate(request.lastLogIndex(), request.lastLogTerm());
        if (canVote && upToDate) {
            persistenceListener.onPersistentStateChanged(currentTerm, request.candidateId());
            votedFor = request.candidateId();
            resetElectionTimer(clock.instant());
            return new RequestVoteResponse(currentTerm, true);
        }
        return new RequestVoteResponse(currentTerm, false);
    }

    private boolean isAtLeastAsUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = lastLogTerm();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= lastLogIndex();
    }

    /** Handles an incoming AppendEntries RPC (heartbeat when {@code entries} is empty) — Figure 2's rules, exactly. */
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) throws IOException {
        Objects.requireNonNull(request, "request must not be null");
        if (request.term() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false, 0);
        }
        if (request.term() > currentTerm) {
            stepDownToFollower(request.term());
        } else if (role == RaftRole.CANDIDATE) {
            role = RaftRole.FOLLOWER; // someone else already won this term's election
        }
        currentLeader = request.leaderId();
        resetElectionTimer(clock.instant());

        if (request.prevLogIndex() > lastLogIndex() || termAt(request.prevLogIndex()) != request.prevLogTerm()) {
            return new AppendEntriesResponse(currentTerm, false, lastLogIndex());
        }

        long index = request.prevLogIndex();
        for (LogEntry entry : request.entries()) {
            index++;
            if (index <= lastLogIndex()) {
                if (termAt(index) != entry.term()) {
                    truncateLogFrom(index);
                    log.add(entry);
                }
            } else {
                log.add(entry);
            }
        }

        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), index);
        }
        return new AppendEntriesResponse(currentTerm, true, index);
    }

    private void stepDownToFollower(long newTerm) throws IOException {
        persistenceListener.onPersistentStateChanged(newTerm, null);
        currentTerm = newTerm;
        role = RaftRole.FOLLOWER;
        votedFor = null;
        currentLeader = null;
    }

    private void truncateLogFrom(long index) {
        log.subList((int) (index - 1), log.size()).clear();
    }

    // =====================================================================
    // Responses to our own outgoing RPCs
    // =====================================================================

    /** Handles a RequestVote response from {@code from}; may return a fresh round of heartbeats if this vote just won the election. */
    public synchronized List<RaftAction> handleRequestVoteResponse(NodeId from, RequestVoteResponse response)
            throws IOException {
        Objects.requireNonNull(from, "from must not be null");
        Objects.requireNonNull(response, "response must not be null");
        if (response.term() > currentTerm) {
            stepDownToFollower(response.term());
            return List.of();
        }
        if (role != RaftRole.CANDIDATE || response.term() != currentTerm || !response.voteGranted()) {
            return List.of();
        }
        votesReceivedThisElection.add(from);
        int majority = (peers.size() + 1) / 2 + 1;
        if (votesReceivedThisElection.size() >= majority) {
            return becomeLeader(clock.instant());
        }
        return List.of();
    }

    /**
     * Handles an AppendEntries response from {@code from}. {@code request}
     * is the exact request this responds to (the driver already has it,
     * having just sent it) — used to compute what index this response
     * actually claims, so a stale/delayed response from an outdated
     * request can't corrupt {@code nextIndex}/{@code matchIndex} bookkeeping.
     */
    public synchronized void handleAppendEntriesResponse(NodeId from, AppendEntriesRequest request,
            AppendEntriesResponse response) throws IOException {
        Objects.requireNonNull(from, "from must not be null");
        Objects.requireNonNull(request, "request must not be null");
        Objects.requireNonNull(response, "response must not be null");
        if (response.term() > currentTerm) {
            stepDownToFollower(response.term());
            return;
        }
        if (role != RaftRole.LEADER || response.term() != currentTerm) {
            return;
        }
        long sentUpTo = request.prevLogIndex() + request.entries().size();
        if (response.success()) {
            matchIndex.merge(from, sentUpTo, Math::max);
            nextIndex.put(from, sentUpTo + 1);
            recordAck(from);
            recomputeCommitIndex();
        } else {
            long current = nextIndex.getOrDefault(from, 1L);
            nextIndex.put(from, Math.max(1, current - 1));
        }
    }

    private void recordAck(NodeId from) {
        lastAckTime.put(from, clock.instant());
    }

    /**
     * Phase 15's leader-lease check: true if a majority of the cluster
     * (self, trivially, plus every peer whose most recent successful
     * AppendEntries ack in <em>this</em> term is no older than
     * {@code within}) has been in contact recently. This is a
     * <b>different, stronger</b> question than {@link #isConfirmedLeader()},
     * which only ever asks "did I win an election and get one no-op
     * acknowledged at some point" — a leader that won its election and was
     * then partitioned away from every peer stays {@link #isConfirmedLeader()}
     * forever (nothing ever tells it a higher term exists), but
     * {@code hasRecentQuorumContact} correctly goes false roughly
     * {@code within} after the partition starts, since acks simply stop
     * arriving. This is what actually closes the "old leader is alive but
     * stale, and never hears about the new term" gap: self-fencing via lease
     * expiry, not just reacting to an explicit higher-term message.
     *
     * <p>Purely a local, this-node's-own-clock computation (like every other
     * timeout in this codebase) — no cross-node clock synchronization is
     * assumed or required.
     */
    public synchronized boolean hasRecentQuorumContact(Duration within) {
        if (role != RaftRole.LEADER) {
            return false;
        }
        if (peers.isEmpty()) {
            return true; // a sole node is trivially always in contact with itself
        }
        Instant now = clock.instant();
        long recentPeers = peers.stream()
                .map(lastAckTime::get)
                .filter(Objects::nonNull)
                .filter(ackTime -> Duration.between(ackTime, now).compareTo(within) <= 0)
                .count();
        long contactedCount = 1 + recentPeers; // self always counts
        return contactedCount * 2 > peers.size() + 1;
    }

    /**
     * The Raft safety-critical commit rule (Figure 2, §5.4.2): a leader may
     * only advance {@code commitIndex} to an index whose entry was appended
     * <em>in its own current term</em> — never by directly counting
     * replicas of an older-term entry, even if a majority already has it.
     * Older entries become committed only as a side effect of a later,
     * current-term entry (like the leader's own election no-op) reaching
     * that same majority.
     */
    private void recomputeCommitIndex() {
        if (role != RaftRole.LEADER) {
            return;
        }
        for (long n = lastLogIndex(); n > commitIndex; n--) {
            if (termAt(n) != currentTerm) {
                continue;
            }
            long replicatedCount = 1; // this leader already has it
            for (NodeId peer : peers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    replicatedCount++;
                }
            }
            if (replicatedCount * 2 > peers.size() + 1) {
                commitIndex = n;
                return;
            }
        }
    }

    private long lastLogIndex() {
        return log.size();
    }

    private long lastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).term();
    }

    private long termAt(long index) {
        if (index <= 0) {
            return 0;
        }
        return log.get((int) (index - 1)).term();
    }

    // =====================================================================
    // Read-only inspection
    // =====================================================================

    public synchronized RaftRole role() {
        return role;
    }

    public synchronized boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    /**
     * True only once this node is {@link RaftRole#LEADER} <em>and</em> a
     * majority has actually acknowledged its own current-term leadership
     * no-op — the signal {@code RaftCluster} gates data-plane leadership on,
     * not {@link #isLeader()} alone (which flips true the instant a
     * majority of <em>votes</em> arrive, before any peer has actually
     * accepted a single AppendEntries from this node — a real, if narrow,
     * window in which treating {@code isLeader()} alone as authoritative
     * could let a node believe itself leader more confidently than its
     * peers currently agree).
     */
    public synchronized boolean isConfirmedLeader() {
        return role == RaftRole.LEADER && commitIndex >= leaderNoOpIndex;
    }

    /**
     * The actual fencing gate: {@link #isConfirmedLeader()} (won an election
     * and had the no-op acknowledged at some point) <em>and</em>
     * {@link #hasRecentQuorumContact} (still, right now, in contact with a
     * majority). Both conditions are read from the same synchronized method
     * so a caller gets one consistent snapshot rather than two calls that
     * could straddle a state change. This — not {@link #isConfirmedLeader()}
     * alone — is what {@code PartitionLeadership}/{@code WriteAuthority}
     * actually gate data-plane writes on.
     */
    public synchronized boolean canServeAuthoritatively(Duration leaseDuration) {
        return isConfirmedLeader() && hasRecentQuorumContact(leaseDuration);
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized Optional<NodeId> currentLeader() {
        return Optional.ofNullable(currentLeader);
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public synchronized long lastLogIndexForTest() {
        return lastLogIndex();
    }

    public synchronized List<LogEntry> logForTest() {
        return List.copyOf(log);
    }

    public NodeId selfId() {
        return selfId;
    }
}
