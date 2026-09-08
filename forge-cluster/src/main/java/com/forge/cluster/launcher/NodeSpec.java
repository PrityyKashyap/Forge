package com.forge.cluster.launcher;

import com.forge.cluster.NodeId;

import java.util.Objects;

/**
 * One line of a cluster config file (see {@link ClusterConfig}): everything
 * {@link ClusterNodeMain} needs to know about a single node in the cluster,
 * including nodes other than itself (their addresses are how this node
 * finds its Raft and replication peers).
 */
public record NodeSpec(NodeId id, String host, int raftPort, int replicationPort, int clientPort) {

    public NodeSpec {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(host, "host must not be null");
    }
}
