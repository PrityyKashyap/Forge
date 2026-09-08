package com.forge.cluster.launcher;

import com.forge.client.ForgeClient;
import com.forge.client.ForgeServerException;
import com.forge.cluster.NodeId;
import com.forge.common.protocol.ProtocolConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Proves {@link ClusterNode} — the wiring behind the real {@link
 * ClusterNodeMain} CLI entry point — actually produces a working,
 * real-socket, multi-node FORGE cluster from nothing but a {@link
 * ClusterConfig}-shaped node list: election, a client write through
 * whichever node is elected leader, and real replication to the other two.
 * This is the launcher's own end-to-end proof, parallel to (and reusing the
 * same wiring recipe as) {@code FailoverReplicationIntegrationTest}, but
 * exercised through the actual class real operators would run.
 */
class ClusterNodeLauncherIntegrationTest {

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeout);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    @Test
    @Timeout(30)
    void aThreeNodeClusterStartedFromASpecListElectsALeaderAndReplicatesARealWrite(@TempDir Path baseDir)
            throws Exception {
        List<NodeSpec> specs = List.of(
                new NodeSpec(new NodeId("launch-a"), "localhost", freePort(), freePort(), freePort()),
                new NodeSpec(new NodeId("launch-b"), "localhost", freePort(), freePort(), freePort()),
                new NodeSpec(new NodeId("launch-c"), "localhost", freePort(), freePort(), freePort()));

        ClusterNode a = ClusterNode.start(specs, new NodeId("launch-a"), baseDir.resolve("a"));
        ClusterNode b = ClusterNode.start(specs, new NodeId("launch-b"), baseDir.resolve("b"));
        ClusterNode c = ClusterNode.start(specs, new NodeId("launch-c"), baseDir.resolve("c"));
        List<ClusterNode> all = List.of(a, b, c);
        try {
            waitUntil(Duration.ofSeconds(15), () -> all.stream().anyMatch(n -> n.raftCluster().isConfirmedLeader()));
            ClusterNode leader = all.stream().filter(n -> n.raftCluster().isConfirmedLeader()).findFirst().orElseThrow();

            try (ForgeClient client = ForgeClient.connect("localhost", leader.clientPort())) {
                client.put("launcher-key", "launcher-value".getBytes());
            }

            for (ClusterNode node : all) {
                waitUntil(Duration.ofSeconds(10), () -> node.store().get("launcher-key").isPresent());
                assertArrayEquals("launcher-value".getBytes(), node.store().get("launcher-key").orElseThrow(),
                        "every node, including followers, must end up with the write via real replication");
            }
        } finally {
            a.close();
            b.close();
            c.close();
        }
    }

    @Test
    @Timeout(30)
    void aNonLeaderNodeRejectsAWriteWithNotLeader(@TempDir Path baseDir) throws Exception {
        List<NodeSpec> specs = List.of(
                new NodeSpec(new NodeId("nl-a"), "localhost", freePort(), freePort(), freePort()),
                new NodeSpec(new NodeId("nl-b"), "localhost", freePort(), freePort(), freePort()));

        ClusterNode a = ClusterNode.start(specs, new NodeId("nl-a"), baseDir.resolve("a"));
        ClusterNode b = ClusterNode.start(specs, new NodeId("nl-b"), baseDir.resolve("b"));
        List<ClusterNode> all = List.of(a, b);
        try {
            waitUntil(Duration.ofSeconds(15), () -> all.stream().anyMatch(n -> n.raftCluster().isConfirmedLeader()));
            ClusterNode nonLeader = all.stream().filter(n -> !n.raftCluster().isConfirmedLeader()).findFirst()
                    .orElseThrow();

            try (ForgeClient client = ForgeClient.connect("localhost", nonLeader.clientPort())) {
                ForgeServerException e = org.junit.jupiter.api.Assertions.assertThrows(ForgeServerException.class,
                        () -> client.put("k", "v".getBytes()));
                assertTrue(e.errorCode() == ProtocolConstants.ERROR_NOT_LEADER,
                        "a non-leader node must reject a write with ERROR_NOT_LEADER, not silently accept it");
            }
        } finally {
            a.close();
            b.close();
        }
    }
}
