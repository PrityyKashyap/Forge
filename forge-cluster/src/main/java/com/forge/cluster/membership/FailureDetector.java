package com.forge.cluster.membership;

import com.forge.cluster.NodeId;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A fixed-timeout heartbeat failure detector, per DESIGN.md's Phase 8
 * contract and ARCHITECTURE.md §3.7.
 *
 * <h2>Design</h2>
 * Deliberately a <b>passive, clock-driven state machine with no threads of
 * its own</b> — {@link #recordHeartbeat} feeds it evidence of liveness, and
 * {@link #tick()} re-evaluates every tracked node against the current time,
 * returning whatever transitions just happened. Nothing about this class
 * schedules anything or sleeps; a caller (in production, {@code
 * HeartbeatService}) decides when to call {@code tick()}, typically on a
 * fixed schedule. This split is what makes the detector's actual state-machine
 * correctness fully testable with a fake {@link Clock} and zero real
 * sleeping — see {@code FailureDetectorTest}.
 *
 * <h2>Failure model, stated precisely</h2>
 * <ul>
 *   <li><b>Failure assumptions</b>: heartbeats can be delayed or dropped
 *       independent of whether the sender is actually alive — a slow network
 *       and a dead process look identical from here. Clocks across nodes are
 *       not assumed synchronized; every timestamp compared is local to this
 *       detector's own {@link Clock}, never a peer's.</li>
 *   <li><b>Correctness invariant</b>: a node reaches {@link NodeState#DEAD}
 *       only after {@code deadTimeout} has elapsed with no heartbeat — never
 *       on a single missed beat. Modeling "N consecutive missed heartbeats"
 *       as "elapsed time ≥ N × heartbeatInterval" (i.e. picking {@code
 *       suspectTimeout}/{@code deadTimeout} as multiples of however often
 *       heartbeats are actually sent) is equivalent to counting misses but
 *       more robust to jitter — one heartbeat arriving a little early or
 *       late doesn't reset a miss counter incorrectly.</li>
 *   <li><b>Consistency guarantee</b>: eventually consistent, not
 *       instantaneous or agreed-upon — this is one detector's local view;
 *       another node's detector, or the same node ticked at a different
 *       moment, can legitimately disagree for a while.</li>
 *   <li><b>Recovery behavior</b>: a {@link NodeState#DEAD} (or {@link
 *       NodeState#SUSPECT}) node is returned to {@link NodeState#ALIVE}
 *       automatically the moment a heartbeat is recorded for it — no
 *       separate "un-mark-dead" operation, no restart required.</li>
 * </ul>
 *
 * <h2>Honest limitation: false positives</h2>
 * A sufficiently slow-but-alive node (GC pause, network congestion,
 * overloaded host) is indistinguishable from a dead one once it misses
 * heartbeats for {@code deadTimeout} — this detector <b>will</b> mark it
 * dead. That's the fundamental, unavoidable tradeoff named in
 * ARCHITECTURE.md §3.7: shorter timeouts detect real failures faster at the
 * cost of more false positives; longer timeouts do the reverse. Nothing here
 * (phi-accrual, adaptive timeouts) tries to soften that tradeoff — it's a
 * fixed-timeout detector by deliberate choice, per DESIGN.md §10.
 *
 * <h2>Join is required before heartbeats count</h2>
 * {@link #recordHeartbeat} for a node that was never {@link #join}ed is
 * ignored (returns {@link Optional#empty()}) rather than silently
 * auto-joining it — {@code join} is the one authoritative way a node enters
 * this detector's membership view, so an arbitrary heartbeat can't make
 * something a "member" that was never explicitly admitted.
 *
 * <p>Thread-safe: every public method is {@code synchronized}. Membership
 * changes are low-volume control-plane traffic, so a single lock is simple
 * and sufficient — no lock striping attempted or needed.
 */
public final class FailureDetector {

    private final Clock clock;
    private final Duration suspectTimeout;
    private final Duration deadTimeout;
    private final Map<NodeId, MembershipEntry> members = new LinkedHashMap<>();

    public FailureDetector(Clock clock, Duration suspectTimeout, Duration deadTimeout) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.suspectTimeout = Objects.requireNonNull(suspectTimeout, "suspectTimeout must not be null");
        this.deadTimeout = Objects.requireNonNull(deadTimeout, "deadTimeout must not be null");
        if (suspectTimeout.isNegative() || suspectTimeout.isZero()) {
            throw new IllegalArgumentException("suspectTimeout must be positive");
        }
        if (deadTimeout.compareTo(suspectTimeout) < 0) {
            throw new IllegalArgumentException("deadTimeout must be >= suspectTimeout");
        }
    }

    /** Admits {@code node} as {@link NodeState#ALIVE} as of now. Resets an already-tracked node back to ALIVE. */
    public synchronized void join(NodeId node) {
        Objects.requireNonNull(node, "node must not be null");
        members.put(node, new MembershipEntry(node, NodeState.ALIVE, clock.instant()));
    }

    /** Voluntary departure: {@code node} is removed from tracking entirely, not marked DEAD. */
    public synchronized void leave(NodeId node) {
        Objects.requireNonNull(node, "node must not be null");
        members.remove(node);
    }

    /**
     * Records evidence that {@code node} is alive right now. A no-op,
     * returning {@link Optional#empty()}, if {@code node} was never
     * {@link #join}ed. Otherwise returns the state {@code node} is in
     * immediately after this call — always {@link NodeState#ALIVE},
     * regardless of what it was before (this is the "recovery" path).
     */
    public synchronized Optional<NodeState> recordHeartbeat(NodeId node) {
        Objects.requireNonNull(node, "node must not be null");
        if (!members.containsKey(node)) {
            return Optional.empty();
        }
        members.put(node, new MembershipEntry(node, NodeState.ALIVE, clock.instant()));
        return Optional.of(NodeState.ALIVE);
    }

    /**
     * Re-evaluates every tracked node's state against {@link Clock#instant()}
     * right now, applying {@link #suspectTimeout}/{@link #deadTimeout} to
     * time elapsed since each node's last recorded heartbeat.
     *
     * @return every transition that just happened, in no particular order;
     *         empty if nothing changed
     */
    public synchronized List<MembershipChange> tick() {
        Instant now = clock.instant();
        List<MembershipChange> changes = new ArrayList<>();
        for (Map.Entry<NodeId, MembershipEntry> e : members.entrySet()) {
            MembershipEntry entry = e.getValue();
            Duration sinceLastHeartbeat = Duration.between(entry.lastHeartbeat(), now);
            NodeState newState;
            if (sinceLastHeartbeat.compareTo(deadTimeout) >= 0) {
                newState = NodeState.DEAD;
            } else if (sinceLastHeartbeat.compareTo(suspectTimeout) >= 0) {
                newState = NodeState.SUSPECT;
            } else {
                newState = NodeState.ALIVE;
            }
            if (newState != entry.state()) {
                changes.add(new MembershipChange(entry.nodeId(), entry.state(), newState));
                e.setValue(new MembershipEntry(entry.nodeId(), newState, entry.lastHeartbeat()));
            }
        }
        return changes;
    }

    public synchronized Optional<NodeState> stateOf(NodeId node) {
        MembershipEntry entry = members.get(node);
        return entry == null ? Optional.empty() : Optional.of(entry.state());
    }

    /** A stable-ordered snapshot of every currently-tracked node's full entry. */
    public synchronized List<MembershipEntry> snapshot() {
        return List.copyOf(members.values());
    }
}
