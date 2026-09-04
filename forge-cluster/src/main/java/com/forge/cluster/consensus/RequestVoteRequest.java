package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;

import java.util.Objects;

/** The RequestVote RPC's arguments, per the Raft paper (Figure 2). */
public record RequestVoteRequest(long term, NodeId candidateId, long lastLogIndex, long lastLogTerm) {

    public RequestVoteRequest {
        Objects.requireNonNull(candidateId, "candidateId must not be null");
    }
}
