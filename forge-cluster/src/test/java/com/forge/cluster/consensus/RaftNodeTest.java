package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RaftNode} is a pure, clock-driven state machine with no I/O — every
 * scenario here drives it directly (no sockets, no real sleeping), the same
 * discipline {@code FailureDetectorTest} already established for Phase 8.
 * These are the ten-plus named adversarial scenarios the Phase 14 mandate
 * calls for: terms, elections, quorums, log replication, and the Figure 8
 * commit-safety rule.
 */
class RaftNodeTest {

    private static final Duration ELECTION_MIN = Duration.ofMillis(150);
    private static final Duration ELECTION_MAX = Duration.ofMillis(300);
    private static final Duration HEARTBEAT = Duration.ofMillis(50);
    private static final Duration PAST_ELECTION_TIMEOUT = ELECTION_MAX.plusMillis(1);
    private static final Duration PAST_HEARTBEAT_INTERVAL = HEARTBEAT.plusMillis(1);

    private static RaftNode newNode(NodeId self, Set<NodeId> peers, MutableClock clock) {
        return new RaftNode(self, peers, clock, ELECTION_MIN, ELECTION_MAX, HEARTBEAT, new Random(42));
    }

    private static RaftNode newNode(NodeId self, Set<NodeId> peers, MutableClock clock, long seed) {
        return new RaftNode(self, peers, clock, ELECTION_MIN, ELECTION_MAX, HEARTBEAT, new Random(seed));
    }

    private static RaftAction.SendAppendEntries findAppendEntriesTo(List<RaftAction> actions, NodeId target) {
        return actions.stream()
                .filter(a -> a.to().equals(target))
                .map(a -> (RaftAction.SendAppendEntries) a)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no SendAppendEntries action addressed to " + target));
    }

    // =====================================================================
    // Scenario 1: an election with a majority of votes wins outright
    // =====================================================================
    @Test
    void scenario1_electionWithMajorityVotesBecomesLeader() {
        NodeId self = new NodeId("n1");
        NodeId peer1 = new NodeId("n2");
        NodeId peer2 = new NodeId("n3");
        MutableClock clock = new MutableClock(Instant.EPOCH);
        RaftNode node = newNode(self, Set.of(peer1, peer2), clock);

        clock.advance(PAST_ELECTION_TIMEOUT);
        List<RaftAction> electionActions = node.tick();
        assertEquals(RaftRole.CANDIDATE, node.role());
        assertEquals(1, node.currentTerm());
        assertEquals(2, electionActions.size(), "a RequestVote must be sent to every peer");
        assertTrue(electionActions.stream().allMatch(a -> a instanceof RaftAction.SendRequestVote));

        // Majority of 3 is 2 (self + one peer) — a single granted vote is enough to win the election.
        List<RaftAction> afterVote = node.handleRequestVoteResponse(peer1, new RequestVoteResponse(1, true));
        assertTrue(node.isLeader());
        assertFalse(node.isConfirmedLeader(), "winning the vote is not the same as a peer having acked AppendEntries yet");
        assertEquals(2, afterVote.size(), "winning immediately sends AppendEntries/heartbeats to every peer");
        assertTrue(afterVote.stream().allMatch(a -> a instanceof RaftAction.SendAppendEntries));
        assertEquals(1, node.logForTest().size(), "the leader appends exactly one no-op on election");
        assertEquals(LogEntry.NO_OP, node.logForTest().get(0).command());

        // Once a peer actually acknowledges the no-op via AppendEntries, leadership is confirmed.
        RaftAction.SendAppendEntries heartbeat = (RaftAction.SendAppendEntries) afterVote.get(0);
        node.handleAppendEntriesResponse(heartbeat.to(), heartbeat.request(), new AppendEntriesResponse(1, true, 1));
        assertTrue(node.isConfirmedLeader(), "self + one ack now forms a majority of 3");
    }

    // =====================================================================
    // Scenario 2: a split vote (no majority) forces a new election at a higher term
    // =====================================================================
    @Test
    void scenario2_splitVoteTriggersNewElectionAtHigherTerm() {
        NodeId self = new NodeId("n1");
        Set<NodeId> peers = Set.of(new NodeId("n2"), new NodeId("n3"), new NodeId("n4"), new NodeId("n5"));
        MutableClock clock = new MutableClock(Instant.EPOCH);
        RaftNode node = newNode(self, peers, clock);

        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        assertEquals(1, node.currentTerm());

        // Only one of four peers grants — self + 1 = 2 votes, majority of 5 is 3. No win.
        NodeId onePeer = peers.iterator().next();
        List<RaftAction> result = node.handleRequestVoteResponse(onePeer, new RequestVoteResponse(1, true));
        assertTrue(result.isEmpty());
        assertEquals(RaftRole.CANDIDATE, node.role(), "insufficient votes must not win the election");

        // Timeout elapses again with no leader ever heard from — a fresh election, term must increase again.
        clock.advance(PAST_ELECTION_TIMEOUT);
        List<RaftAction> secondElection = node.tick();
        assertEquals(2, node.currentTerm(), "a repeated timeout starts a brand-new election at term+1");
        assertEquals(RaftRole.CANDIDATE, node.role());
        assertEquals(4, secondElection.size());
    }

    // =====================================================================
    // Scenario 3: a stale-term RequestVote is rejected and reports the true current term
    // =====================================================================
    @Test
    void scenario3_staleTermRequestVoteIsRejected() {
        NodeId self = new NodeId("n1");
        NodeId leader = new NodeId("leader");
        MutableClock clock = new MutableClock(Instant.EPOCH);
        RaftNode node = newNode(self, Set.of(leader), clock);

        node.handleAppendEntries(new AppendEntriesRequest(5, leader, 0, 0, List.of(), 0));
        assertEquals(5, node.currentTerm());

        RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(3, new NodeId("stale-candidate"), 0, 0));
        assertFalse(response.voteGranted());
        assertEquals(5, response.term(), "a rejected stale vote must still report the true current term");
        assertEquals(5, node.currentTerm(), "a stale RequestVote must not affect our term at all");
    }

    // =====================================================================
    // Scenario 4: at most one vote is granted per term
    // =====================================================================
    @Test
    void scenario4_atMostOneVotePerTerm() {
        NodeId self = new NodeId("n1");
        NodeId candidateA = new NodeId("a");
        NodeId candidateB = new NodeId("b");
        MutableClock clock = new MutableClock(Instant.EPOCH);
        RaftNode node = newNode(self, Set.of(candidateA, candidateB), clock);

        RequestVoteResponse toA = node.handleRequestVote(new RequestVoteRequest(1, candidateA, 0, 0));
        assertTrue(toA.voteGranted());

        RequestVoteResponse toB = node.handleRequestVote(new RequestVoteRequest(1, candidateB, 0, 0));
        assertFalse(toB.voteGranted(), "a second candidate in the same term must be denied");

        // A retried RPC from the SAME candidate we already voted for must still be granted (idempotent).
        RequestVoteResponse toAAgain = node.handleRequestVote(new RequestVoteRequest(1, candidateA, 0, 0));
        assertTrue(toAAgain.voteGranted());
    }

    // =====================================================================
    // Scenario 5: a higher term observed anywhere forces an immediate step-down
    // =====================================================================
    @Test
    void scenario5_higherTermAlwaysForcesStepDownToFollower() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId peer = new NodeId("n2");
        RaftNode leaderNode = newNode(self, Set.of(peer), clock);
        clock.advance(PAST_ELECTION_TIMEOUT);
        leaderNode.tick();
        leaderNode.handleRequestVoteResponse(peer, new RequestVoteResponse(1, true));
        assertTrue(leaderNode.isLeader());

        // An AppendEntries response carrying a higher term steps a LEADER down.
        AppendEntriesRequest sent = new AppendEntriesRequest(1, self, 1, 1, List.of(), 0);
        leaderNode.handleAppendEntriesResponse(peer, sent, new AppendEntriesResponse(9, false, 0));
        assertEquals(RaftRole.FOLLOWER, leaderNode.role());
        assertEquals(9, leaderNode.currentTerm());

        // A RequestVote response carrying a higher term steps a CANDIDATE down.
        RaftNode candidateNode = newNode(self, Set.of(peer), clock);
        clock.advance(PAST_ELECTION_TIMEOUT);
        candidateNode.tick();
        assertEquals(RaftRole.CANDIDATE, candidateNode.role());
        candidateNode.handleRequestVoteResponse(peer, new RequestVoteResponse(50, false));
        assertEquals(RaftRole.FOLLOWER, candidateNode.role());
        assertEquals(50, candidateNode.currentTerm());
    }

    // =====================================================================
    // Scenario 6: log-inconsistent AppendEntries is rejected; leader backs off nextIndex and retries
    // =====================================================================
    @Test
    void scenario6_logInconsistencyIsRejectedAndLeaderBacksOffNextIndex() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId follower = new NodeId("follower");
        NodeId oldLeader = new NodeId("old-leader");
        NodeId newLeader = new NodeId("new-leader");
        RaftNode followerNode = newNode(follower, Set.of(), clock);

        AppendEntriesResponse fromOld = followerNode.handleAppendEntries(
                new AppendEntriesRequest(1, oldLeader, 0, 0, List.of(new LogEntry(1, "x")), 0));
        assertTrue(fromOld.success());
        assertEquals(1, followerNode.logForTest().size());

        // A new leader (higher term) wrongly believes the follower already has a term-2 entry at index 1.
        AppendEntriesResponse mismatched = followerNode.handleAppendEntries(
                new AppendEntriesRequest(2, newLeader, 1, 2, List.of(new LogEntry(2, "y")), 0));
        assertFalse(mismatched.success(), "prevLogTerm mismatch (actual=1, claimed=2) must be rejected");
        assertEquals(2, followerNode.currentTerm(), "the follower still adopts the higher term even on rejection");
        assertEquals(1, followerNode.logForTest().size(), "a rejected AppendEntries must not touch the log");

        // Leader-side: a failure response must decrement nextIndex, observable in the next AppendEntries built for that peer.
        MutableClock leaderClock = new MutableClock(Instant.EPOCH);
        RaftNode leaderNode = newNode(new NodeId("leader"), Set.of(follower), leaderClock);
        leaderClock.advance(PAST_ELECTION_TIMEOUT);
        leaderNode.tick();
        leaderNode.handleRequestVoteResponse(follower, new RequestVoteResponse(1, true));
        assertTrue(leaderNode.isLeader());

        leaderClock.advance(PAST_HEARTBEAT_INTERVAL);
        List<RaftAction> firstHeartbeat = leaderNode.tick();
        AppendEntriesRequest firstSent = ((RaftAction.SendAppendEntries) firstHeartbeat.get(0)).request();
        assertEquals(1, firstSent.prevLogIndex(), "nextIndex starts optimistically at lastLogIndex+1");

        leaderNode.handleAppendEntriesResponse(follower, firstSent, new AppendEntriesResponse(1, false, 0));

        leaderClock.advance(PAST_HEARTBEAT_INTERVAL);
        List<RaftAction> secondHeartbeat = leaderNode.tick();
        AppendEntriesRequest secondSent = ((RaftAction.SendAppendEntries) secondHeartbeat.get(0)).request();
        assertEquals(0, secondSent.prevLogIndex(), "a rejection must decrement nextIndex so the next attempt reaches further back");
    }

    // =====================================================================
    // Scenario 7: Figure 8 safety — an entry from an older term is never committed by direct majority count alone
    // =====================================================================
    @Test
    void scenario7_oldTermEntryNeverCommittedByDirectMajorityCountAlone() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId peer1 = new NodeId("n2");
        NodeId peer2 = new NodeId("n3");
        RaftNode node = newNode(self, Set.of(peer1, peer2), clock);

        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        node.handleRequestVoteResponse(peer1, new RequestVoteResponse(1, true));
        assertTrue(node.isLeader());
        assertEquals(1, node.currentTerm());

        // Both peers fully ack the term-1 no-op (index 1) — a legitimate current-term commit.
        AppendEntriesRequest sentTerm1 = new AppendEntriesRequest(1, self, 0, 0, List.of(new LogEntry(1, LogEntry.NO_OP)), 0);
        node.handleAppendEntriesResponse(peer1, sentTerm1, new AppendEntriesResponse(1, true, 1));
        node.handleAppendEntriesResponse(peer2, sentTerm1, new AppendEntriesResponse(1, true, 1));
        assertEquals(1, node.commitIndex());

        // This node is forced to a much higher term by an external message (stale-network-heals scenario),
        // then wins a fresh election there, appending a second (term-6) no-op at index 2.
        node.handleAppendEntries(new AppendEntriesRequest(5, peer1, 1, 1, List.of(), 1));
        assertEquals(RaftRole.FOLLOWER, node.role());
        assertEquals(5, node.currentTerm());

        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        assertEquals(6, node.currentTerm());
        node.handleRequestVoteResponse(peer1, new RequestVoteResponse(6, true));
        assertTrue(node.isLeader());
        assertEquals(2, node.logForTest().size());
        assertEquals(1, node.commitIndex(), "becoming leader again must not retroactively advance commitIndex on its own");

        // peer1 acks index 1 only (the OLD term-1 entry) — even though this now forms a majority
        // (self + peer1) for index 1, it must NOT advance commitIndex, because index 1's term (1)
        // is not this leader's current term (6). This is the exact rule recomputeCommitIndex enforces.
        AppendEntriesRequest partialAck = new AppendEntriesRequest(6, self, 0, 1, List.of(new LogEntry(1, LogEntry.NO_OP)), 1);
        node.handleAppendEntriesResponse(peer1, partialAck, new AppendEntriesResponse(6, true, 1));
        assertEquals(1, node.commitIndex(),
                "an old-term entry must never be committed by direct majority count alone, even with a real majority");

        // Only once the leader's OWN current-term entry (index 2) reaches majority does commitIndex
        // advance — which also implicitly commits index 1 as a side effect, per Raft's log-matching property.
        AppendEntriesRequest fullAck = new AppendEntriesRequest(6, self, 1, 1, List.of(new LogEntry(6, LogEntry.NO_OP)), 1);
        node.handleAppendEntriesResponse(peer1, fullAck, new AppendEntriesResponse(6, true, 2));
        assertEquals(2, node.commitIndex(), "a current-term entry reaching majority commits it and everything before it");
    }

    // =====================================================================
    // Scenario 8: a candidate steps down when a legitimate same-term leader is discovered
    // =====================================================================
    @Test
    void scenario8_candidateStepsDownOnSameTermLeaderAppendEntries() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId otherLeader = new NodeId("n2");
        RaftNode node = newNode(self, Set.of(otherLeader), clock);

        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        assertEquals(RaftRole.CANDIDATE, node.role());
        long candidateTerm = node.currentTerm();

        // Another node already won this exact term's election and is sending heartbeats.
        AppendEntriesResponse response = node.handleAppendEntries(
                new AppendEntriesRequest(candidateTerm, otherLeader, 0, 0, List.of(), 0));
        assertTrue(response.success());
        assertEquals(RaftRole.FOLLOWER, node.role(), "discovering a legitimate same-term leader must end the candidacy");
        assertEquals(otherLeader, node.currentLeader().orElseThrow());
        assertEquals(candidateTerm, node.currentTerm(), "the term itself does not change, only the role");
    }

    // =====================================================================
    // Scenario 9: a vote is denied to a candidate whose log is less up-to-date
    // =====================================================================
    @Test
    void scenario9_voteDeniedToLessUpToDateCandidate() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId leader = new NodeId("leader");
        NodeId candidate = new NodeId("behind-candidate");
        RaftNode node = newNode(self, Set.of(leader, candidate), clock);

        // Give this node a two-entry log (via a real leader) so it is more up-to-date than a fresh candidate.
        node.handleAppendEntries(new AppendEntriesRequest(1, leader, 0, 0,
                List.of(new LogEntry(1, "a"), new LogEntry(1, "b")), 0));
        assertEquals(2, node.logForTest().size());

        // A candidate campaigning with an empty log (term matches, but candidate's log is strictly shorter).
        RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(1, candidate, 0, 0));
        assertFalse(response.voteGranted(), "a candidate with a shorter log at the same term must be denied");

        // A candidate campaigning with a lower last-log-term is denied even if its index looks longer.
        RequestVoteResponse response2 = node.handleRequestVote(new RequestVoteRequest(2, candidate, 100, 0));
        assertFalse(response2.voteGranted(), "a lower last-log-term always loses, regardless of index");
    }

    // =====================================================================
    // Scenario 10: a stale leader (still on an old term) is rejected once a higher term has been observed
    // =====================================================================
    @Test
    void scenario10_staleLeaderIsRejectedAfterNewTermElection() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId staleLeader = new NodeId("stale-leader");
        RaftNode node = newNode(self, Set.of(staleLeader), clock);

        AppendEntriesResponse first = node.handleAppendEntries(new AppendEntriesRequest(1, staleLeader, 0, 0, List.of(), 0));
        assertTrue(first.success());
        assertEquals(staleLeader, node.currentLeader().orElseThrow());

        // This node times out (the stale leader is now partitioned away / unreachable) and wins a new election.
        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        node.handleRequestVoteResponse(staleLeader, new RequestVoteResponse(2, true));
        assertTrue(node.isLeader());
        assertEquals(2, node.currentTerm());

        // The partition heals and the stale leader (still on term 1) sends a heartbeat.
        AppendEntriesResponse fromStale = node.handleAppendEntries(new AppendEntriesRequest(1, staleLeader, 0, 0, List.of(), 0));
        assertFalse(fromStale.success(), "a stale term-1 leader must be rejected once we're on term 2");
        assertEquals(2, fromStale.term());
        assertTrue(node.isLeader(), "the stale leader's heartbeat must not depose the legitimate current leader");
    }

    // =====================================================================
    // Bonus: a sole node (no peers) is its own trivial majority
    // =====================================================================
    @Test
    void bonus_soleNodeClusterBecomesLeaderAndCommitsImmediately() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        RaftNode node = newNode(new NodeId("n1"), Set.of(), clock);

        clock.advance(PAST_ELECTION_TIMEOUT);
        List<RaftAction> actions = node.tick();
        assertTrue(actions.isEmpty(), "a sole node has no peers to send RequestVote to");
        assertTrue(node.isLeader());
        assertTrue(node.isConfirmedLeader(), "a majority of one is trivially satisfied by self alone");
        assertEquals(1, node.commitIndex());
    }

    // =====================================================================
    // Bonus: election timeouts are actually randomized, not a disguised constant
    // =====================================================================
    @Test
    void bonus_electionTimeoutsAreRandomizedAcrossSuccessiveElections() {
        // A single node that never wins (peers never respond) keeps
        // re-running its own election indefinitely, drawing a fresh random
        // timeout each time. Measuring the elapsed time between successive
        // term increments, over many elections from one seeded Random,
        // proves real per-election variation without relying on
        // java.util.Random's first draw for many small, sequential seeds —
        // which is a known-correlated LCG artifact, not a meaningful test
        // of randomness (confirmed directly: seeds 0-29's first nextDouble()
        // all land within 0.730-0.733 of each other).
        NodeId self = new NodeId("n1");
        Set<NodeId> peers = Set.of(new NodeId("n2"), new NodeId("n3"));
        MutableClock clock = new MutableClock(Instant.EPOCH);
        RaftNode node = newNode(self, peers, clock);

        Duration step = Duration.ofMillis(5);
        Duration sinceLastElection = Duration.ZERO;
        Set<Duration> observedGaps = new HashSet<>();
        long lastTerm = node.currentTerm();

        for (int i = 0; i < 2000 && observedGaps.size() < 2; i++) {
            clock.advance(step);
            sinceLastElection = sinceLastElection.plus(step);
            node.tick();
            if (node.currentTerm() != lastTerm) {
                observedGaps.add(sinceLastElection);
                sinceLastElection = Duration.ZERO;
                lastTerm = node.currentTerm();
            }
        }
        assertTrue(observedGaps.size() >= 2,
                "successive elections from one node must use varying timeouts, not a fixed interval — observed: " + observedGaps);
    }

    // =====================================================================
    // Phase 15: the leader-lease fencing check (canServeAuthoritatively)
    // =====================================================================

    /**
     * The exact scenario the Phase 15 mandate calls "the difficult case":
     * a leader that is alive, was legitimately elected, and never receives
     * any message revealing a higher term (because it's genuinely isolated,
     * not because anyone told it so) must still stop considering itself
     * authoritative once it can no longer prove it has a majority. This is
     * precisely what {@code isConfirmedLeader()} alone cannot do — it never
     * looks at the actual passage of time as evidence of a problem.
     */
    @Test
    void phase15_isolatedLeaderLosesQuorumContactWithinLeaseEvenWithoutHearingAHigherTerm() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId peer1 = new NodeId("n2");
        NodeId peer2 = new NodeId("n3");
        Duration lease = ELECTION_MIN;
        RaftNode node = newNode(self, Set.of(peer1, peer2), clock);

        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        List<RaftAction> afterVote = node.handleRequestVoteResponse(peer1, new RequestVoteResponse(node.currentTerm(), true));
        RaftAction.SendAppendEntries hb = findAppendEntriesTo(afterVote, peer1);
        node.handleAppendEntriesResponse(peer1, hb.request(), new AppendEntriesResponse(node.currentTerm(), true, 1));
        assertTrue(node.isConfirmedLeader());
        assertTrue(node.canServeAuthoritatively(lease), "just acked — well within the lease");

        // The network partitions here: no further AppendEntries responses ever arrive,
        // and no one ever tells this node about a higher term (it's isolated, not deposed).
        clock.advance(lease.plusMillis(1));

        assertTrue(node.isConfirmedLeader(),
                "isConfirmedLeader() alone never looks at elapsed time — it stays true forever, which is exactly the gap");
        assertFalse(node.canServeAuthoritatively(lease),
                "canServeAuthoritatively must self-fence once the lease expires with no fresh quorum contact");
    }

    @Test
    void phase15_leaderWithOngoingAcksRetainsQuorumContact() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId peer1 = new NodeId("n2");
        NodeId peer2 = new NodeId("n3");
        Duration lease = ELECTION_MIN;
        RaftNode node = newNode(self, Set.of(peer1, peer2), clock);

        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        List<RaftAction> afterVote = node.handleRequestVoteResponse(peer1, new RequestVoteResponse(node.currentTerm(), true));
        RaftAction.SendAppendEntries hb = findAppendEntriesTo(afterVote, peer1);
        node.handleAppendEntriesResponse(peer1, hb.request(), new AppendEntriesResponse(node.currentTerm(), true, 1));

        // Keep acking from peer1 well inside every lease window, repeatedly, well past
        // what a single lease duration would allow if acks had stopped.
        for (int i = 0; i < 5; i++) {
            clock.advance(lease.minusMillis(10));
            assertTrue(node.canServeAuthoritatively(lease), "iteration " + i);
            AppendEntriesRequest sent = new AppendEntriesRequest(node.currentTerm(), self, 1, node.currentTerm(), List.of(), 1);
            node.handleAppendEntriesResponse(peer1, sent, new AppendEntriesResponse(node.currentTerm(), true, 1));
        }
        assertTrue(node.canServeAuthoritatively(lease));
    }

    @Test
    void phase15_soleNodeAlwaysHasQuorumContact() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        RaftNode node = newNode(new NodeId("n1"), Set.of(), clock);
        clock.advance(PAST_ELECTION_TIMEOUT);
        node.tick();
        assertTrue(node.isLeader());

        clock.advance(Duration.ofDays(1)); // arbitrarily far in the future
        assertTrue(node.canServeAuthoritatively(ELECTION_MIN), "a sole node's majority (of one) is always itself");
    }

    @Test
    void phase15_canServeAuthoritativelyIsFalseForNonLeaders() {
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        RaftNode follower = newNode(self, Set.of(new NodeId("n2")), clock);
        assertFalse(follower.canServeAuthoritatively(ELECTION_MIN), "a plain follower is never authoritative");

        clock.advance(PAST_ELECTION_TIMEOUT);
        follower.tick(); // becomes CANDIDATE
        assertFalse(follower.canServeAuthoritatively(ELECTION_MIN), "a candidate is never authoritative either");
    }

    @Test
    void confirmedLeaderIsFalseUntilANoOpActuallyCommits() {
        // A 5-node cluster is used deliberately: with 3, winning the minimum
        // quorum of votes (self + 1 peer) and that same peer's first ack
        // already forms a majority, collapsing "elected" and "confirmed"
        // into the same step. With 5, winning the election needs only 2
        // peer votes (majority 3), but confirming leadership needs 2 peer
        // acks — leaving a real, observable "leader but not yet confirmed" gap.
        MutableClock clock = new MutableClock(Instant.EPOCH);
        NodeId self = new NodeId("n1");
        NodeId p1 = new NodeId("p1"), p2 = new NodeId("p2"), p3 = new NodeId("p3"), p4 = new NodeId("p4");
        RaftNode wide = newNode(self, Set.of(p1, p2, p3, p4), clock);
        clock.advance(PAST_ELECTION_TIMEOUT);
        wide.tick();
        wide.handleRequestVoteResponse(p1, new RequestVoteResponse(wide.currentTerm(), true));
        wide.handleRequestVoteResponse(p2, new RequestVoteResponse(wide.currentTerm(), true));
        assertTrue(wide.isLeader(), "3 of 5 votes wins the election");
        assertFalse(wide.isConfirmedLeader(), "no peer has acknowledged AppendEntries yet — votes alone don't confirm");

        AppendEntriesRequest sent = new AppendEntriesRequest(wide.currentTerm(), self, 0, 0,
                List.of(new LogEntry(wide.currentTerm(), LogEntry.NO_OP)), 0);
        wide.handleAppendEntriesResponse(p1, sent, new AppendEntriesResponse(wide.currentTerm(), true, 1));
        assertFalse(wide.isConfirmedLeader(), "self + one ack is only 2 of 5 — majority of 5 is 3");

        wide.handleAppendEntriesResponse(p2, sent, new AppendEntriesResponse(wide.currentTerm(), true, 1));
        assertTrue(wide.isConfirmedLeader(), "self + two acks forms a majority of 5, confirming leadership");
    }
}
