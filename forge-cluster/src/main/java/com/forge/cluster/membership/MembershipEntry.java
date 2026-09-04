package com.forge.cluster.membership;

import com.forge.cluster.NodeId;

import java.time.Instant;
import java.util.Objects;

/** One node's current membership view: its state and when it was last heard from. */
public record MembershipEntry(NodeId nodeId, NodeState state, Instant lastHeartbeat) {

    public MembershipEntry {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(lastHeartbeat, "lastHeartbeat must not be null");
    }
}
