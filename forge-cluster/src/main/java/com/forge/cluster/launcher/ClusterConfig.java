package com.forge.cluster.launcher;

import com.forge.cluster.NodeId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Post-Phase-15-audit addition: a plain-text, one-line-per-node cluster
 * membership file — the real, deliberately minimal answer to "there is no
 * way to actually run an N-node FORGE cluster except from test code" (see
 * docs/DEMO.md's own disclosed gap and README's Future Work). No new
 * dependency (JSON/YAML library) is introduced; this project hand-rolls
 * every wire format already, and a cluster's node list is small and static
 * enough that a whitespace-separated line is entirely adequate.
 *
 * <h2>Format</h2>
 * One node per line:
 * {@code nodeId host raftPort replicationPort clientPort snapshotPort}.
 * Blank lines and lines starting with {@code #} are ignored. Every node in
 * the cluster — including the one about to be started — must appear
 * exactly once; {@link ClusterNodeMain} finds its own entry by id and
 * treats every other line as a peer. {@code snapshotPort} backs the
 * {@code resync} subcommand (every node runs a {@code SnapshotServer}
 * unconditionally, so any node can act as a resync source for a stale
 * peer rejoining the cluster).
 *
 * <pre>{@code
 * # a 3-node FORGE cluster, all on localhost
 * a  localhost  17001  17002  17003  17004
 * b  localhost  17011  17012  17013  17014
 * c  localhost  17021  17022  17023  17024
 * }</pre>
 */
public final class ClusterConfig {

    private ClusterConfig() {
    }

    /** @throws IOException if the file is missing, empty, malformed, or lists a node id more than once */
    public static List<NodeSpec> load(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);
        List<NodeSpec> specs = new ArrayList<>();
        for (String rawLine : lines) {
            String line = rawLine.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\\s+");
            if (parts.length != 6) {
                throw new IOException("malformed cluster config line in " + file
                        + " (expected 'nodeId host raftPort replicationPort clientPort snapshotPort'): " + rawLine);
            }
            try {
                specs.add(new NodeSpec(new NodeId(parts[0]), parts[1],
                        Integer.parseInt(parts[2]), Integer.parseInt(parts[3]), Integer.parseInt(parts[4]),
                        Integer.parseInt(parts[5])));
            } catch (NumberFormatException e) {
                throw new IOException("malformed port number in " + file + ": " + rawLine, e);
            }
        }
        if (specs.isEmpty()) {
            throw new IOException("cluster config file has no node entries: " + file);
        }
        Set<NodeId> seen = new HashSet<>();
        for (NodeSpec spec : specs) {
            if (!seen.add(spec.id())) {
                throw new IOException("duplicate node id '" + spec.id() + "' in cluster config: " + file);
            }
        }
        return List.copyOf(specs);
    }
}
