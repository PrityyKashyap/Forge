package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;

/**
 * An outbound RPC {@link RaftNode} wants sent, returned from {@link RaftNode#tick()}
 * / {@link RaftNode#handleRequestVoteResponse} rather than sent directly —
 * {@link RaftNode} performs no I/O of its own (see its class Javadoc on why),
 * so a driver (in production, {@link RaftCluster}) is responsible for
 * actually transmitting these and feeding the eventual response back in.
 */
public sealed interface RaftAction {

    NodeId to();

    record SendRequestVote(NodeId to, RequestVoteRequest request) implements RaftAction {
    }

    record SendAppendEntries(NodeId to, AppendEntriesRequest request) implements RaftAction {
    }
}
