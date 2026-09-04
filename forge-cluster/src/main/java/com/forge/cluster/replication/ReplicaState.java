package com.forge.cluster.replication;

import com.forge.cluster.NodeId;

import java.time.Instant;
import java.util.Objects;

/**
 * What a leader's {@link ReplicationServer} knows about one follower:
 * {@code acknowledgedSequenceNumber} is that follower's own
 * {@code lastAppliedSequenceNumber()} as of its most recent ack — "one past
 * the last record it has durably applied," the same convention used
 * everywhere else in this project. Replication lag against a leader whose
 * own {@code lastAppliedSequenceNumber()} is {@code L} is simply
 * {@code L - acknowledgedSequenceNumber}.
 */
public record ReplicaState(NodeId followerId, long acknowledgedSequenceNumber, Instant lastAckTime) {

    public ReplicaState {
        Objects.requireNonNull(followerId, "followerId must not be null");
        Objects.requireNonNull(lastAckTime, "lastAckTime must not be null");
    }
}
