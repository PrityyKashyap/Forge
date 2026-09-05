package com.forge.bench;

import com.forge.client.ForgeClient;
import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.cluster.leadership.PartitionLeadership;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * Phase 15, E17: measures real Raft-driven failover timing — how long
 * election actually takes, and how long until the cluster can serve a
 * write again — against a real 3-node {@link RaftCluster} +
 * {@link ForgeServer} + {@link PartitionLeadership} stack, exactly the
 * same components {@code StaleLeaderFencingIntegrationTest}/
 * {@code FailoverReplicationIntegrationTest} exercise for correctness.
 * Continues the E-numbering past Phase 13's E16.
 *
 * <p><b>What is and isn't measured, precisely</b>: this project has no
 * separate failure-detection step in the Raft failover path — Raft's own
 * randomized election timeout is what notices the leader is gone, there is
 * no independent {@code FailureDetector} wired into this decision (Phase
 * 8's detector is a separate mechanism, not connected to Raft elections).
 * So "election time" here already includes whatever time it takes the
 * survivors' election timeout to fire — it is not a separate, additional
 * detection delay on top of election time; there is only one number
 * because there is only one mechanism. This is stated explicitly rather
 * than implying two mechanisms exist.
 */
public final class FailoverBenchmarkRunner {

    private static final Logger log = LoggerFactory.getLogger(FailoverBenchmarkRunner.class);

    private static final int REPEATS = 5;
    private static final Duration LEADER_ELECTION_MIN = Duration.ofMillis(50);
    private static final Duration LEADER_ELECTION_MAX = Duration.ofMillis(80);
    private static final Duration FOLLOWER_ELECTION_MIN = Duration.ofMillis(150);
    private static final Duration FOLLOWER_ELECTION_MAX = Duration.ofMillis(300);
    private static final Duration HEARTBEAT = Duration.ofMillis(25);
    private static final Duration TICK = Duration.ofMillis(10);
    private static final Duration RPC_TIMEOUT = Duration.ofMillis(500);
    private static final Duration LEASE = LEADER_ELECTION_MIN;

    private FailoverBenchmarkRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path resultsDir = Path.of("forge-bench", "results");
        Files.createDirectories(resultsDir);
        String runId = Instant.now().toString().replace(":", "-");
        Path csv = resultsDir.resolve("phase15-failover-" + runId + ".csv");

        List<AmplificationBenchmarkResult> results = new ArrayList<>();
        for (int run = 1; run <= REPEATS; run++) {
            Path tempRoot = Files.createTempDirectory("forge-bench-failover-");
            try {
                runOnce(run, tempRoot, results);
            } finally {
                deleteRecursively(tempRoot);
            }
        }

        writeCsv(csv, results);
        printHumanSummary(System.out, results, csv);
    }

    private static void runOnce(int runIndex, Path baseDir, List<AmplificationBenchmarkResult> out) throws Exception {
        NodeId a = new NodeId("bench-a");
        NodeId b = new NodeId("bench-b");
        NodeId c = new NodeId("bench-c");
        int aPort = freePort();
        int bPort = freePort();
        int cPort = freePort();
        Map<NodeId, NodeAddress> addrs = Map.of(
                a, new NodeAddress("localhost", aPort), b, new NodeAddress("localhost", bPort), c, new NodeAddress("localhost", cPort));

        try (ConcurrentLsmKeyValueStore storeA = new ConcurrentLsmKeyValueStore(baseDir.resolve("a"));
             RaftCluster raftA = new RaftCluster(a, without(addrs, a), aPort, Clock.systemUTC(),
                     LEADER_ELECTION_MIN, LEADER_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(runIndex));
             RaftCluster raftB = new RaftCluster(b, without(addrs, b), bPort, Clock.systemUTC(),
                     FOLLOWER_ELECTION_MIN, FOLLOWER_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(100 + runIndex));
             RaftCluster raftC = new RaftCluster(c, without(addrs, c), cPort, Clock.systemUTC(),
                     FOLLOWER_ELECTION_MIN, FOLLOWER_ELECTION_MAX, HEARTBEAT, TICK, RPC_TIMEOUT, LEASE, new Random(200 + runIndex))) {

            PartitionLeadership leadershipA = new PartitionLeadership("p0", raftA, storeA);
            try (ForgeServer serverA = new ForgeServer(storeA, key -> true, leadershipA, 0)) {
                waitUntil(Duration.ofSeconds(10), leadershipA::canAcceptWrites);
                try (ForgeClient warmup = ForgeClient.connect("localhost", serverA.port())) {
                    warmup.put("warmup", "v".getBytes(StandardCharsets.UTF_8));
                }

                Instant crashInstant = Instant.now();
                serverA.close();
                raftA.close();

                List<RaftCluster> survivors = List.of(raftB, raftC);
                waitUntil(Duration.ofSeconds(10), () -> survivors.stream().anyMatch(RaftCluster::isConfirmedLeader));
                Instant electedInstant = Instant.now();
                double electionSeconds = Duration.between(crashInstant, electedInstant).toNanos() / 1_000_000_000.0;

                RaftCluster newLeaderRaft = raftB.isConfirmedLeader() ? raftB : raftC;
                NodeId newLeaderId = newLeaderRaft == raftB ? b : c;
                Path newLeaderDir = baseDir.resolve(newLeaderId.value());
                try (ConcurrentLsmKeyValueStore newLeaderStore = new ConcurrentLsmKeyValueStore(newLeaderDir)) {
                    PartitionLeadership newLeadership = new PartitionLeadership("p0", newLeaderRaft, newLeaderStore);
                    try (ForgeServer newServer = new ForgeServer(newLeaderStore, key -> true, newLeadership, 0)) {
                        waitUntil(Duration.ofSeconds(5), newLeadership::canAcceptWrites);
                        Instant leadershipConfirmedInstant = Instant.now();
                        double leadershipTransitionSeconds =
                                Duration.between(crashInstant, leadershipConfirmedInstant).toNanos() / 1_000_000_000.0;

                        try (ForgeClient client = ForgeClient.connect("localhost", newServer.port())) {
                            client.put("post-failover", "v".getBytes(StandardCharsets.UTF_8));
                        }
                        Instant firstWriteInstant = Instant.now();
                        double timeToFirstWriteSeconds =
                                Duration.between(crashInstant, firstWriteInstant).toNanos() / 1_000_000_000.0;

                        add(out, runIndex, "election_time_seconds", electionSeconds);
                        add(out, runIndex, "leadership_transition_seconds", leadershipTransitionSeconds);
                        add(out, runIndex, "time_to_first_successful_write_seconds", timeToFirstWriteSeconds);
                    }
                }
            }
        }
    }

    private static Map<NodeId, NodeAddress> without(Map<NodeId, NodeAddress> addresses, NodeId self) {
        Map<NodeId, NodeAddress> copy = new HashMap<>(addresses);
        copy.remove(self);
        return copy;
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        if (!condition.getAsBoolean()) {
            throw new IllegalStateException("condition not met within " + timeout);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    private static void add(List<AmplificationBenchmarkResult> out, int runIndex, String metric, double value) {
        out.add(new AmplificationBenchmarkResult("E17", "Failover timing (run " + runIndex + ")",
                Instant.now(), metric, value, "seconds"));
    }

    private static void writeCsv(Path file, List<AmplificationBenchmarkResult> results) throws IOException {
        try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write(AmplificationBenchmarkResult.CSV_HEADER);
            writer.newLine();
            for (AmplificationBenchmarkResult r : results) {
                writer.write(r.toCsvRow());
                writer.newLine();
            }
        }
    }

    private static void printHumanSummary(PrintStream out, List<AmplificationBenchmarkResult> results, Path csv) {
        out.println();
        out.println("==================== FORGE Phase 15 Failover Timing Summary ====================");
        out.printf("CSV: %s (%d rows)%n", csv.toAbsolutePath(), results.size());
        out.println("------------------------------------------------------------------------------------");
        out.printf("%-8s %-45s %12s %s%n", "run", "metric", "value", "unit");
        for (AmplificationBenchmarkResult r : results) {
            out.printf("%-8s %-45s %12.4f %s%n", r.description().replaceAll("[^0-9]", ""), r.metric(), r.value(), r.unit());
        }
        out.println("======================================================================================");
    }

    private static void deleteRecursively(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    log.warn("failed to delete temp file {} during cleanup", path, e);
                }
            });
        } catch (IOException e) {
            log.warn("failed to walk temp directory {} during cleanup", root, e);
        }
    }
}
