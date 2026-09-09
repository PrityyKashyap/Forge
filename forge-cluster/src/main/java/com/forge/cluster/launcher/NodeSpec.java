package com.forge.cluster.launcher;

import com.forge.cluster.NodeId;

import java.util.Objects;

/**
 * One line of a cluster config file (see {@link ClusterConfig}): everything
 * {@link ClusterNodeMain} needs to know about a single node in the cluster,
 * including nodes other than itself (their addresses are how this node
 * finds its Raft and replication peers).
 *
 * <p>{@code snapshotPort} (added alongside the {@code resync} subcommand):
 * every node runs a {@code SnapshotServer} unconditionally, exactly like
 * {@code replicationPort}'s {@code ReplicationServer} — so any node can
 * act as a full-resync source for a rejoining stale replica.
 */
public record NodeSpec(NodeId id, String host, int raftPort, int replicationPort, int clientPort, int snapshotPort) {

    public NodeSpec {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(host, "host must not be null");
    }
}
