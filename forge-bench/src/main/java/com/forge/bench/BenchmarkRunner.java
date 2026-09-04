package com.forge.bench;

import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Phase 6 benchmark harness entry point. Starts one real {@link ForgeServer}
 * (backed by a real {@link ConcurrentLsmKeyValueStore}, WAL fsync always on
 * — see BENCHMARKS.md's methodology section) in this same JVM, listening on
 * an ephemeral loopback port, runs every Phase 6 experiment (E1, E3, E4, E5)
 * against it, writes a CSV of every measured result, prints a human-readable
 * summary, and tears everything down — server, stores, connections, and
 * temp data directories — before exiting, so nothing is left behind.
 *
 * <p>Run with: {@code mvn -pl forge-bench exec:java -Dexec.mainClass=com.forge.bench.BenchmarkRunner}
 * (or the packaged jar's main class), from the repo root.
 */
public final class BenchmarkRunner {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    private BenchmarkRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path resultsDir = Path.of("forge-bench", "results");
        Files.createDirectories(resultsDir);
        String runId = Instant.now().toString().replace(":", "-");
        Path csvFile = resultsDir.resolve("phase6-benchmarks-" + runId + ".csv");

        Path tempRoot = Files.createTempDirectory("forge-bench-");
        Path sharedServerDataDir = tempRoot.resolve("shared-server-store");
        Path e4NetworkDataDir = tempRoot.resolve("e4-network-store");
        Path e4InProcessDataDir = tempRoot.resolve("e4-in-process-store");

        List<BenchmarkResult> results = new ArrayList<>();
        Exception failure = null;
        long suiteStartNanos = System.nanoTime();

        // E1, E3, and E5 share one long-lived server across the whole suite —
        // realistic sustained-server behavior is exactly what those experiments
        // are meant to reflect. E4 deliberately does NOT use this server: see
        // NetworkOverheadExperiment's Javadoc for why it needs a fresh
        // server/store per repeat instead.
        try (ConcurrentLsmKeyValueStore sharedStore = new ConcurrentLsmKeyValueStore(sharedServerDataDir);
                ForgeServer sharedServer = new ForgeServer(sharedStore, 0)) {
            String host = "localhost";
            int port = sharedServer.port();
            log.info("FORGE bench server listening on {}:{} (data dir: {})", host, port, sharedServerDataDir);

            log.info("Running E1: sequential baseline...");
            SequentialBaselineExperiment.run(host, port, results);

            log.info("Running E3: concurrency scaling...");
            ConcurrencyScalingExperiment.run(host, port, results);

            log.info("Running E4: network overhead (in-process vs. loopback TCP, isolated servers)...");
            NetworkOverheadExperiment.run(e4NetworkDataDir, e4InProcessDataDir, results);

            log.info("Running E5: mixed read/write workload...");
            MixedWorkloadExperiment.run(host, port, results);
        } catch (Exception e) {
            failure = e;
            log.error("benchmark suite failed partway through", e);
        } finally {
            deleteRecursively(tempRoot);
        }

        long suiteElapsedSeconds = (System.nanoTime() - suiteStartNanos) / 1_000_000_000L;

        if (!results.isEmpty()) {
            CsvReportWriter.write(csvFile, results);
            printHumanSummary(System.out, results, suiteElapsedSeconds, csvFile);
        }

        if (failure != null) {
            throw failure;
        }
    }

    private static void printHumanSummary(PrintStream out, List<BenchmarkResult> results, long suiteElapsedSeconds,
            Path csvFile) {
        out.println();
        out.println("==================== FORGE Phase 6 Benchmark Summary ====================");
        out.printf("Total results: %d   Suite wall-clock time: %ds%n", results.size(), suiteElapsedSeconds);
        out.printf("Raw CSV: %s%n", csvFile.toAbsolutePath());
        out.println("---------------------------------------------------------------------------");
        out.printf("%-4s %-8s %-12s %-5s %-4s %12s %10s %10s %10s %10s%n",
                "exp", "opType", "mode", "conc", "#run", "ops/sec", "avg(ms)", "p95(ms)", "p99(ms)", "max(ms)");
        for (BenchmarkResult r : results) {
            out.printf("%-4s %-8s %-12s %-5d %-4d %12.1f %10.3f %10.3f %10.3f %10.3f%n",
                    r.experimentId(), r.opType(), r.mode(), r.concurrency(), r.runIndex(),
                    r.throughputOpsPerSec(), r.avgLatencyMs(), r.p95LatencyMs(), r.p99LatencyMs(), r.maxLatencyMs());
        }
        out.println("===========================================================================");
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
                    log.warn("failed to delete temp benchmark file {} during cleanup", path, e);
                }
            });
        } catch (IOException e) {
            log.warn("failed to walk temp benchmark directory {} during cleanup", root, e);
        }
    }
}
