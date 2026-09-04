package com.forge.cluster.consensus;

/** A {@link RaftNode}'s current role in the consensus protocol. */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
