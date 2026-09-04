/**
 * Phase 14: Raft-style consensus for leader election and failover — terms,
 * majority-vote elections, and log replication/commit via AppendEntries.
 * See {@link com.forge.cluster.consensus.RaftNode} for the state machine
 * and exactly what this is (and is not) used for in FORGE, and
 * {@link com.forge.cluster.consensus.RaftCluster} for the real networked
 * driver built on top of it.
 */
package com.forge.cluster.consensus;
