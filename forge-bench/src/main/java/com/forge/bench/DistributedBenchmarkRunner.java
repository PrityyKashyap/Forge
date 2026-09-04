package com.forge.bench;

import com.forge.cluster.ClusterTopology;
import com.forge.cluster.ConsistentHashRing;
import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import com.forge.cluster.PartitionedForgeClient;
import com.forge.cluster.replication.ReplicationFollower;
import com.forge.cluster.replication.ReplicationServer;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * Phase 12: distributed benchmarks, built on top of Phase 6's harness
 * ({@link BenchmarkResult}, {@link LatencyStats}, {@link CsvReportWriter})
 * and Phases 7–10's real cluster components. <b>Does not replace or discard
 * Phase 6's results</b> — this produces a separate CSV alongside the
 * existing {@code phase6-benchmarks-*.csv}; both are real, both are kept.
 *
 * <p>Four experiments, continuing DESIGN.md's E-numbering past E11 (which
 * predates partitioning/replication/recovery existing in detail): E12 node
 * scaling, E13 replication overhead, E14 recovery time vs. data size, E15
 * replica catch-up time vs. backlog size.
 */
public final class DistributedBenchmarkRunner {

    private static final Logger log = LoggerFactory.getLogger(DistributedBenchmarkRunner.class);

    private DistributedBenchmarkRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path resultsDir = Path.of("forge-bench", "results");
        Files.createDirectories(resultsDir);
        String runId = Instant.now().toString().replace(":", "-");
        Path throughputCsv = resultsDir.resolve("phase12-throughput-" + runId + ".csv");
        Path durationCsv = resultsDir.resolve("phase12-duration-" + runId + ".csv");

        Path tempRoot = Files.createTempDirectory("forge-bench-distributed-");
        List<BenchmarkResult> throughputResults = new ArrayList<>();
        List<DurationBenchmarkResult> durationResults = new ArrayList<>();
        Exception failure = null;
        long suiteStart = System.nanoTime();

        try {
            log.info("Running E12: node scaling...");
            NodeScalingExperiment.run(tempRoot.resolve("e12"), throughputResults);

            log.info("Running E13: replication overhead...");
            ReplicationOverheadExperiment.run(tempRoot.resolve("e13"), throughputResults);

            log.info("Running E14: recovery time vs. data size...");
            RecoveryTimeExperiment.run(tempRoot.resolve("e14"), durationResults);

            log.info("Running E15: replica catch-up time vs. backlog size...");
            CatchUpTimeExperiment.run(tempRoot.resolve("e15"), durationResults);
        } catch (Exception e) {
            failure = e;
            log.error("distributed benchmark suite failed partway through", e);
        } finally {
            deleteRecursively(tempRoot);
        }

        long suiteElapsedSeconds = (System.nanoTime() - suiteStart) / 1_000_000_000L;

        if (!throughputResults.isEmpty()) {
            CsvReportWriter.write(throughputCsv, throughputResults);
        }
        if (!durationResults.isEmpty()) {
            writeDurationCsv(durationCsv, durationResults);
        }
        printHumanSummary(System.out, throughputResults, durationResults, suiteElapsedSeconds, throughputCsv, durationCsv);

        if (failure != null) {
            throw failure;
        }
    }

    private static void writeDurationCsv(Path file, List<DurationBenchmarkResult> results) throws IOException {
        try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write(DurationBenchmarkResult.CSV_HEADER);
            writer.newLine();
            for (DurationBenchmarkResult r : results) {
                writer.write(r.toCsvRow());
                writer.newLine();
            }
        }
    }

    private static void printHumanSummary(PrintStream out, List<BenchmarkResult> throughputResults,
            List<DurationBenchmarkResult> durationResults, long suiteElapsedSeconds, Path throughputCsv, Path durationCsv) {
        out.println();
        out.println("==================== FORGE Phase 12 Distributed Benchmark Summary ====================");
        out.printf("Suite wall-clock time: %ds%n", suiteElapsedSeconds);
        out.printf("Throughput CSV: %s (%d rows)%n", throughputCsv.toAbsolutePath(), throughputResults.size());
        out.printf("Duration CSV: %s (%d rows)%n", durationCsv.toAbsolutePath(), durationResults.size());
        out.println("----------------------------------------------------------------------------------------");
        out.printf("%-4s %-8s %-14s %-5s %-4s %12s %10s %10s%n",
                "exp", "opType", "mode", "conc", "#run", "ops/sec", "avg(ms)", "p99(ms)");
        for (BenchmarkResult r : throughputResults) {
            out.printf("%-4s %-8s %-14s %-5d %-4d %12.1f %10.3f %10.3f%n",
                    r.experimentId(), r.opType(), r.mode(), r.concurrency(), r.runIndex(),
                    r.throughputOpsPerSec(), r.avgLatencyMs(), r.p99LatencyMs());
        }
        out.println("----------------------------------------------------------------------------------------");
        out.printf("%-4s %-30s %-10s %-4s %14s%n", "exp", "config", "size", "#run", "elapsed(s)");
        for (DurationBenchmarkResult r : durationResults) {
            out.printf("%-4s %-30s %-10d %-4d %14.3f%n",
                    r.experimentId(), r.configLabel(), r.datasetSize(), r.runIndex(), r.elapsedSeconds());
        }
        out.println("========================================================================================");
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

    // ---------------------------------------------------------------
    // Shared cluster-node helpers
    // ---------------------------------------------------------------

    record BenchNode(NodeId id, ConcurrentLsmKeyValueStore store, ForgeServer server) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            server.close();
            store.close();
        }
    }

    static BenchNode startNode(NodeId id, ConsistentHashRing ring, Path dataDir) throws IOException {
        ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
        ForgeServer server;
        try {
            server = new ForgeServer(store, key -> ring.ownerOf(key).equals(id), 0);
        } catch (IOException | RuntimeException e) {
            store.close();
            throw e;
        }
        return new BenchNode(id, store, server);
    }

    static ClusterTopology topologyOf(List<BenchNode> nodes) {
        ClusterTopology topology = ClusterTopology.empty();
        for (BenchNode node : nodes) {
            topology = topology.withNode(node.id(), new NodeAddress("localhost", node.server().port()));
        }
        return topology;
    }

    static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
    }

    // ---------------------------------------------------------------
    // E12: node scaling
    // ---------------------------------------------------------------

    private static final class NodeScalingExperiment {
        static void run(Path baseDir, List<BenchmarkResult> out) throws Exception {
            byte[] value = Workload.fixedValue(DistributedBenchmarkConstants.VALUE_SIZE_BYTES);
            for (int nodeCount : DistributedBenchmarkConstants.NODE_COUNTS) {
                for (int run = 1; run <= DistributedBenchmarkConstants.REPEATS; run++) {
                    runOnce(baseDir.resolve("n" + nodeCount + "-r" + run), nodeCount, run, value, out);
                }
            }
        }

        private static void runOnce(Path baseDir, int nodeCount, int runIndex, byte[] value, List<BenchmarkResult> out)
                throws Exception {
            Set<NodeId> ids = new HashSet<>();
            for (int i = 0; i < nodeCount; i++) {
                ids.add(new NodeId("scale-node-" + i));
            }
            ConsistentHashRing ring = ConsistentHashRing.of(ids);
            List<BenchNode> nodes = new ArrayList<>();
            try {
                for (NodeId id : ids) {
                    nodes.add(startNode(id, ring, baseDir.resolve(id.value())));
                }
                ClusterTopology topology = topologyOf(nodes);

                int concurrency = DistributedBenchmarkConstants.E12_CONCURRENCY_PER_NODE * nodeCount;
                int warmupPerThread = DistributedBenchmarkConstants.E12_WARMUP_OPS_PER_THREAD;
                int measuredPerThread = DistributedBenchmarkConstants.E12_MEASURED_OPS_PER_THREAD;

                ExecutorService pool = Executors.newFixedThreadPool(concurrency);
                CountDownLatch ready = new CountDownLatch(concurrency);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<long[]>> futures = new ArrayList<>();

                for (int t = 0; t < concurrency; t++) {
                    int threadId = t;
                    futures.add(pool.submit(() -> {
                        try (PartitionedForgeClient client = new PartitionedForgeClient(topology)) {
                            for (int i = 0; i < warmupPerThread; i++) {
                                client.put("scale-warmup-t" + threadId + "-" + i, value);
                            }
                            ready.countDown();
                            go.await();
                            long[] latencies = new long[measuredPerThread];
                            for (int i = 0; i < measuredPerThread; i++) {
                                String key = "scale-t" + threadId + "-" + i;
                                long start = System.nanoTime();
                                client.put(key, value);
                                latencies[i] = System.nanoTime() - start;
                            }
                            return latencies;
                        }
                    }));
                }
                ready.await();
                long phaseStart = System.nanoTime();
                go.countDown();

                long totalOps = 0;
                List<long[]> perThread = new ArrayList<>();
                for (Future<long[]> f : futures) {
                    long[] lat = f.get();
                    perThread.add(lat);
                    totalOps += lat.length;
                }
                long phaseEnd = System.nanoTime();
                pool.shutdown();

                long[] merged = new long[(int) totalOps];
                int offset = 0;
                for (long[] lat : perThread) {
                    System.arraycopy(lat, 0, merged, offset, lat.length);
                    offset += lat.length;
                }
                PhaseResult result = new PhaseResult(merged, phaseEnd - phaseStart, totalOps);
                out.add(BenchmarkResult.of("E12", "Node scaling: " + nodeCount + " node(s)", "PUT", "CLUSTER",
                        nodeCount, runIndex, (long) warmupPerThread * concurrency,
                        DistributedBenchmarkConstants.VALUE_SIZE_BYTES, result));
            } finally {
                for (BenchNode node : nodes) {
                    node.close();
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // E13: replication overhead
    // ---------------------------------------------------------------

    private static final class ReplicationOverheadExperiment {
        static void run(Path baseDir, List<BenchmarkResult> out) throws Exception {
            byte[] value = Workload.fixedValue(DistributedBenchmarkConstants.VALUE_SIZE_BYTES);
            for (int followerCount : DistributedBenchmarkConstants.FOLLOWER_COUNTS) {
                for (int run = 1; run <= DistributedBenchmarkConstants.REPEATS; run++) {
                    runOnce(baseDir.resolve("f" + followerCount + "-r" + run), followerCount, run, value, out);
                }
            }
        }

        private static void runOnce(Path baseDir, int followerCount, int runIndex, byte[] value,
                List<BenchmarkResult> out) throws Exception {
            try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("leader"));
                    ForgeServer leaderServer = new ForgeServer(leaderStore, 0);
                    ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0)) {

                List<ConcurrentLsmKeyValueStore> followerStores = new ArrayList<>();
                List<ReplicationFollower> followers = new ArrayList<>();
                try {
                    for (int i = 0; i < followerCount; i++) {
                        ConcurrentLsmKeyValueStore fStore = new ConcurrentLsmKeyValueStore(baseDir.resolve("follower" + i));
                        followerStores.add(fStore);
                        followers.add(new ReplicationFollower(new NodeId("bench-follower-" + i), "localhost",
                                replicationServer.port(), fStore, Duration.ofMillis(30)));
                    }

                    try (com.forge.client.ForgeClient client =
                            com.forge.client.ForgeClient.connect("localhost", leaderServer.port())) {
                        for (int i = 0; i < DistributedBenchmarkConstants.E13_WARMUP_OPS; i++) {
                            client.put("repl-warmup-" + i, value);
                        }
                        long[] latencies = new long[DistributedBenchmarkConstants.E13_MEASURED_OPS];
                        long phaseStart = System.nanoTime();
                        for (int i = 0; i < latencies.length; i++) {
                            long start = System.nanoTime();
                            client.put("repl-" + i, value);
                            latencies[i] = System.nanoTime() - start;
                        }
                        long phaseEnd = System.nanoTime();
                        PhaseResult result = new PhaseResult(latencies, phaseEnd - phaseStart, latencies.length);
                        out.add(BenchmarkResult.of("E13", "Replication overhead: " + followerCount + " follower(s)",
                                "PUT", "SEQUENTIAL", followerCount, runIndex,
                                DistributedBenchmarkConstants.E13_WARMUP_OPS, DistributedBenchmarkConstants.VALUE_SIZE_BYTES,
                                result));
                    }
                } finally {
                    for (ReplicationFollower f : followers) {
                        f.close();
                    }
                    for (ConcurrentLsmKeyValueStore s : followerStores) {
                        s.close();
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // E14: recovery time vs. data size
    // ---------------------------------------------------------------

    private static final class RecoveryTimeExperiment {
        static void run(Path baseDir, List<DurationBenchmarkResult> out) throws Exception {
            byte[] value = Workload.fixedValue(DistributedBenchmarkConstants.VALUE_SIZE_BYTES);
            for (long size : DistributedBenchmarkConstants.E14_DATASET_SIZES) {
                for (int run = 1; run <= DistributedBenchmarkConstants.REPEATS; run++) {
                    Path dir = baseDir.resolve("size" + size + "-r" + run);
                    try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
                        for (long i = 0; i < size; i++) {
                            store.put("k" + i, value);
                        }
                    }
                    long start = System.nanoTime();
                    try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir)) {
                        if (reopened.keys().size() != size) {
                            throw new IllegalStateException("recovery did not restore the expected key count");
                        }
                    }
                    long elapsedNanos = System.nanoTime() - start;
                    out.add(new DurationBenchmarkResult("E14", "Recovery (WAL replay) time vs. data size",
                            Instant.now(), "dataset_size=" + size, size, run, elapsedNanos / 1_000_000_000.0));
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // E15: replica catch-up time vs. backlog size
    // ---------------------------------------------------------------

    private static final class CatchUpTimeExperiment {
        static void run(Path baseDir, List<DurationBenchmarkResult> out) throws Exception {
            byte[] value = Workload.fixedValue(DistributedBenchmarkConstants.VALUE_SIZE_BYTES);
            for (long size : DistributedBenchmarkConstants.E15_BACKLOG_SIZES) {
                for (int run = 1; run <= DistributedBenchmarkConstants.REPEATS; run++) {
                    Path dir = baseDir.resolve("size" + size + "-r" + run);
                    try (ConcurrentLsmKeyValueStore leaderStore = new ConcurrentLsmKeyValueStore(dir.resolve("leader"));
                            ReplicationServer replicationServer = new ReplicationServer(leaderStore, 0)) {
                        for (long i = 0; i < size; i++) {
                            leaderStore.put("k" + i, value);
                        }

                        ConcurrentLsmKeyValueStore followerStore = new ConcurrentLsmKeyValueStore(dir.resolve("follower"));
                        long start = System.nanoTime();
                        try (ReplicationFollower follower = new ReplicationFollower(new NodeId("catchup-follower"),
                                "localhost", replicationServer.port(), followerStore, Duration.ofMillis(20))) {
                            waitUntil(Duration.ofSeconds(60), () -> followerStore.keys().size() == size);
                            if (followerStore.keys().size() != size) {
                                followerStore.close();
                                throw new IllegalStateException("follower did not catch up within the allotted time");
                            }
                        }
                        long elapsedNanos = System.nanoTime() - start;
                        followerStore.close();
                        out.add(new DurationBenchmarkResult("E15", "Replica catch-up time vs. backlog size",
                                Instant.now(), "backlog_size=" + size, size, run, elapsedNanos / 1_000_000_000.0));
                    }
                }
            }
        }
    }
}
