package com.forge.cluster.consensus;

/** The RequestVote RPC's results, per the Raft paper (Figure 2). */
public record RequestVoteResponse(long term, boolean voteGranted) {
}
