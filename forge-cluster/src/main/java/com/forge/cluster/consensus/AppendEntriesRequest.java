package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;

import java.util.List;
import java.util.Objects;

/**
 * The AppendEntries RPC's arguments, per the Raft paper (Figure 2). Doubles
 * as the heartbeat when {@code entries} is empty — there is deliberately no
 * separate heartbeat message type, exactly as the paper specifies.
 */
public record AppendEntriesRequest(
        long term,
        NodeId leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit) {

    public AppendEntriesRequest {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        entries = List.copyOf(entries);
    }
}
