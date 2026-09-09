package com.forge.cluster.launcher;

import com.forge.cluster.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Post-Phase-15-audit addition: a real CLI entry point that brings up one
 * full FORGE cluster node — storage, replication, Raft (with persistent
 * state), failover coordination, and the client-facing server — wired
 * together exactly as {@code docs/ARCHITECTURE.md} §3.13 describes and
 * exactly as {@code FailoverReplicationIntegrationTest} already proves
 * works (the actual wiring lives in {@link ClusterNode}; this class is
 * only the CLI shell: parse args, start it, register a shutdown hook).
 *
 * <p>This is deliberately the smallest thing that closes the "no way to
 * actually run an N-node cluster except from tests" gap named in
 * {@code docs/DEMO.md} and README's Future Work: one Java process per
 * node, real sockets, a plain-text config file (see {@link ClusterConfig})
 * listing every node's addresses. Running this once per line in that file
 * (in separate terminals, or as separate background processes) brings up a
 * genuine multi-node FORGE cluster reachable over real ports — the "type a
 * command, watch three terminals react" experience the test-driven demo
 * could not provide by itself.
 *
 * <p>Scope, stated plainly: single-partition (partition id {@code "p0"}),
 * matching {@code PartitionLeadership}'s current scope exactly — every
 * node in the config file is a replica of the same one partition. No
 * process-management, health-checking, or automatic node-list distribution
 * is attempted; each node reads the same static file and is started/stopped
 * independently, by hand or by a wrapping script.
 *
 * <p>Usage: {@code ClusterNodeMain <clusterConfigFile> <selfNodeId> <dataDirectory>}
 *
 * <h2>{@code resync} subcommand</h2>
 * {@code ClusterNodeMain resync <clusterConfigFile> <selfNodeId> <dataDirectory> <sourceNodeId>}
 * — the real, operator-facing answer to a
 * {@code ReplicationFollowerCoordinator} log line like {@code "my own data
 * implies term 1 but the leader is on term 2 — a full snapshot resync is
 * needed"}: stop the stale node's process first (this, like {@code status},
 * is an <b>offline</b> tool — it must never run against a directory a live
 * node already has open), then run this against its data directory, then
 * restart it normally. See {@link ClusterNode#resync} and {@link
 * com.forge.cluster.recovery.StaleReplicaRecovery} for exactly what this
 * does and guarantees.
 */
public final class ClusterNodeMain {

    private static final Logger log = LoggerFactory.getLogger(ClusterNodeMain.class);

    private ClusterNodeMain() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length > 0 && args[0].equals("resync")) {
            if (args.length != 5) {
                System.err.println("Usage: ClusterNodeMain resync <clusterConfigFile> <selfNodeId> <dataDirectory> <sourceNodeId>");
                System.exit(1);
                return;
            }
            runResync(Path.of(args[1]), new NodeId(args[2]), Path.of(args[3]), new NodeId(args[4]));
            return;
        }

        if (args.length != 3) {
            System.err.println("Usage: ClusterNodeMain <clusterConfigFile> <selfNodeId> <dataDirectory>");
            System.err.println("       ClusterNodeMain resync <clusterConfigFile> <selfNodeId> <dataDirectory> <sourceNodeId>");
            System.exit(1);
            return;
        }
        Path configFile = Path.of(args[0]);
        NodeId selfId = new NodeId(args[1]);
        Path dataDirectory = Path.of(args[2]);

        List<NodeSpec> specs = ClusterConfig.load(configFile);
        ClusterNode node = ClusterNode.start(specs, selfId, dataDirectory);

        log.info("FORGE cluster node '{}' up — client port {}, raft port {}, data directory {}",
                selfId, node.clientPort(), node.raftPort(), dataDirectory.toAbsolutePath());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down node '{}'", selfId);
            node.close();
        }, "cluster-node-shutdown-" + selfId));
    }

    private static void runResync(Path configFile, NodeId selfId, Path dataDirectory, NodeId sourceId) throws IOException {
        List<NodeSpec> specs = ClusterConfig.load(configFile);
        NodeSpec source = specs.stream().filter(spec -> spec.id().equals(sourceId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("source node id '" + sourceId + "' not found in cluster config " + configFile));
        log.info("resyncing '{}' at {} from '{}' ({}:{})", selfId, dataDirectory.toAbsolutePath(), sourceId,
                source.host(), source.snapshotPort());
        ClusterNode.resync(dataDirectory, source.host(), source.snapshotPort());
        log.info("resync of '{}' complete — it can now be started normally", selfId);
    }
}
