package com.forge.cluster.consensus;

/**
 * The AppendEntries RPC's results. {@code matchedIndex} is not part of the
 * original paper's minimal RPC shape, but is included here so the leader's
 * response handler doesn't need to separately guess it from the request it
 * sent — see {@code RaftNode.handleAppendEntriesResponse}, which takes the
 * original request alongside this response for exactly that reason.
 */
public record AppendEntriesResponse(long term, boolean success, long matchedIndex) {
}
