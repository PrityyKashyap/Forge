package com.forge.cluster.membership;

import com.forge.cluster.NodeId;

import java.util.Objects;

/** One state transition a {@link FailureDetector#tick()} call observed. */
public record MembershipChange(NodeId nodeId, NodeState previous, NodeState current) {

    public MembershipChange {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(previous, "previous must not be null");
        Objects.requireNonNull(current, "current must not be null");
    }
}
