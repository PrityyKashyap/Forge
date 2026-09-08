package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Post-Phase-15 audit addition: proves the exact safety property {@link
 * RaftPersistentState} exists to close — a node that crashes and restarts
 * mid-term must never grant a second, contradictory vote in a term it
 * already voted in. Mirrors this project's own established discipline (see
 * {@code StaleLeaderFencingIntegrationTest}): don't just add the mechanism,
 * prove the specific failure it prevents, including proving the failure
 * actually happens WITHOUT the mechanism (the "vulnerable baseline").
 *
 * <p>A "restart" here means exactly what a real one produces: a brand-new
 * {@link RaftNode} instance (all in-memory volatile state gone — role,
 * votesReceivedThisElection, the log, everything) seeded only from whatever
 * {@link RaftPersistentState#load} returns from the same file the previous
 * instance wrote to.
 */
class RaftPersistenceIntegrationTest {

    private static final Duration ELECTION_MIN = Duration.ofMillis(150);
    private static final Duration ELECTION_MAX = Duration.ofMillis(300);
    private static final Duration HEARTBEAT = Duration.ofMillis(50);

    @Test
    void aRestartedNodeRemembersItsVoteAndRefusesASecondCandidateInTheSameTerm(@TempDir Path dir) throws Exception {
        Path stateFile = dir.resolve("raft-state");
        NodeId self = new NodeId("n1");
        NodeId candidateA = new NodeId("a");
        NodeId candidateB = new NodeId("b");
        Clock clock = Clock.systemUTC(); // real clock: only currentTerm()/votedFor() are exercised, no timing

        // --- Before the crash: node grants candidate A a vote at term 5, durably. ---
        RaftPersistentState state1 = RaftPersistentState.load(stateFile);
        RaftNode node1 = new RaftNode(self, Set.of(candidateA, candidateB), clock, ELECTION_MIN, ELECTION_MAX,
                HEARTBEAT, new Random(1), state1::save, state1.currentTerm(), state1.votedFor().orElse(null));
        RequestVoteResponse toA = node1.handleRequestVote(new RequestVoteRequest(5, candidateA, 0, 0));
        assertTrue(toA.voteGranted(), "the vote to A must be granted (nothing voted for yet)");

        // --- "Crash": node1 is discarded entirely (simulating an unclean process exit — no
        // graceful shutdown, no extra bookkeeping) and a genuinely fresh RaftNode restarts,
        // reloading only what was durably persisted to stateFile. ---
        RaftPersistentState state2 = RaftPersistentState.load(stateFile);
        assertEquals(5L, state2.currentTerm(), "the term must survive the restart");
        assertEquals(candidateA, state2.votedFor().orElseThrow(), "the vote must survive the restart");
        RaftNode node2 = new RaftNode(self, Set.of(candidateA, candidateB), clock, ELECTION_MIN, ELECTION_MAX,
                HEARTBEAT, new Random(1), state2::save, state2.currentTerm(), state2.votedFor().orElse(null));

        // --- The restarted node must refuse a second, contradictory vote in the SAME term. ---
        RequestVoteResponse toB = node2.handleRequestVote(new RequestVoteRequest(5, candidateB, 0, 0));
        assertFalse(toB.voteGranted(),
                "a restarted node must remember it already voted for A in term 5 and refuse B — this is the exact "
                        + "safety property RaftPersistentState exists to close");

        // A retried request from the SAME candidate it already (durably) voted for is still granted (idempotent),
        // exactly like the non-restart case in RaftNodeTest.scenario4_atMostOneVotePerTerm.
        RequestVoteResponse toAAgainAfterRestart = node2.handleRequestVote(new RequestVoteRequest(5, candidateA, 0, 0));
        assertTrue(toAAgainAfterRestart.voteGranted());
    }

    @Test
    void withoutPersistenceARestartedNodeWouldIncorrectlyDoubleVote_theVulnerableBaseline(@TempDir Path dir) throws Exception {
        // This test deliberately reproduces the PRE-fix behavior (RaftPersistenceListener.NONE,
        // the default used by every other RaftNodeTest case) to prove this isn't a test that
        // would have passed anyway — the vulnerability is real without the persistence wiring.
        NodeId self = new NodeId("n1");
        NodeId candidateA = new NodeId("a");
        NodeId candidateB = new NodeId("b");
        Clock clock = Clock.systemUTC();

        RaftNode beforeCrash = new RaftNode(self, Set.of(candidateA, candidateB), clock, ELECTION_MIN, ELECTION_MAX,
                HEARTBEAT, new Random(1));
        RequestVoteResponse toA = beforeCrash.handleRequestVote(new RequestVoteRequest(5, candidateA, 0, 0));
        assertTrue(toA.voteGranted());

        // "Crash": a fresh RaftNode with NO persistence wiring restarts at term 0, votedFor=null —
        // exactly RaftNode's pre-Phase-15-audit behavior on every real restart.
        RaftNode afterCrash = new RaftNode(self, Set.of(candidateA, candidateB), clock, ELECTION_MIN, ELECTION_MAX,
                HEARTBEAT, new Random(1));
        RequestVoteResponse toB = afterCrash.handleRequestVote(new RequestVoteRequest(5, candidateB, 0, 0));
        assertTrue(toB.voteGranted(),
                "documenting the vulnerability this phase closes: with no persistence, a restarted node has "
                        + "forgotten its vote entirely and incorrectly grants a second, contradictory one");
    }

    @Test
    void aRestartedNodeRejectsAStaleRequestVoteBelowItsPersistedTerm(@TempDir Path dir) throws Exception {
        Path stateFile = dir.resolve("raft-state");
        NodeId self = new NodeId("n1");
        NodeId leader = new NodeId("leader");
        Clock clock = Clock.systemUTC();

        RaftPersistentState state1 = RaftPersistentState.load(stateFile);
        RaftNode node1 = new RaftNode(self, Set.of(leader), clock, ELECTION_MIN, ELECTION_MAX, HEARTBEAT,
                new Random(1), state1::save, state1.currentTerm(), state1.votedFor().orElse(null));
        node1.handleAppendEntries(new AppendEntriesRequest(9, leader, 0, 0, java.util.List.of(), 0));
        assertEquals(9L, node1.currentTerm());

        RaftPersistentState state2 = RaftPersistentState.load(stateFile);
        RaftNode restarted = new RaftNode(self, Set.of(leader), clock, ELECTION_MIN, ELECTION_MAX, HEARTBEAT,
                new Random(1), state2::save, state2.currentTerm(), state2.votedFor().orElse(null));
        assertEquals(9L, restarted.currentTerm(), "the term must survive the restart even with no vote ever granted");

        RequestVoteResponse stale = restarted.handleRequestVote(new RequestVoteRequest(3, new NodeId("stale"), 0, 0));
        assertFalse(stale.voteGranted(), "the restarted node must still reject a term far below its persisted term");
        assertEquals(9L, stale.term());
    }
}
