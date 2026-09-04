package com.forge.bench;

import com.forge.client.ForgeClient;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the harness's own concurrency/timing mechanics (not the full,
 * slow Phase 6 experiment suite — that's run separately via
 * {@code BenchmarkRunner.main()}, not as part of the regression suite) with
 * small op counts against a real embedded {@link ForgeServer}, to catch
 * deadlocks, miscounted ops, or bad throughput/latency arithmetic cheaply
 * and on every build.
 */
class ConcurrentBenchRunTest {

    @Test
    @Timeout(30)
    void reportsExactlyTheExpectedTotalOpsAndPositiveLatencies(@TempDir Path dataDir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
                ForgeServer server = new ForgeServer(store, 0)) {
            String host = "localhost";
            int port = server.port();
            byte[] value = Workload.fixedValue(16);

            try (OpExecutor populate = new NetworkOpExecutor(host, port)) {
                BenchWorker.runUntimed(Workload.puts("smoke", 0, 100, value), populate);
            }

            int concurrency = 4;
            int perThread = 20;
            PhaseResult result = ConcurrentBenchRun.run(concurrency,
                    () -> newExecutor(host, port),
                    threadId -> Workload.getsRoundRobin("smoke", 100, (long) threadId * perThread, perThread));

            assertEquals(concurrency * perThread, result.totalOps());
            assertEquals(concurrency * perThread, result.latenciesNanos().length);
            for (long latency : result.latenciesNanos()) {
                assertTrue(latency > 0, "every recorded latency must be a positive duration");
            }
            assertTrue(result.elapsedNanos() > 0);
            assertTrue(result.throughputOpsPerSec() > 0);
        }
    }

    @Test
    @Timeout(30)
    void throughputEqualsTotalOpsDividedByElapsedSeconds(@TempDir Path dataDir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
                ForgeServer server = new ForgeServer(store, 0)) {
            String host = "localhost";
            int port = server.port();
            byte[] value = Workload.fixedValue(16);

            int concurrency = 3;
            int perThread = 15;
            PhaseResult result = ConcurrentBenchRun.run(concurrency,
                    () -> newExecutor(host, port),
                    threadId -> Workload.puts("smoke-put-t" + threadId, 0, perThread, value));

            double expectedThroughput = result.totalOps() / (result.elapsedNanos() / 1_000_000_000.0);
            assertEquals(expectedThroughput, result.throughputOpsPerSec(), 1e-6);
        }
    }

    @Test
    @Timeout(30)
    void latencyStatsIntegrateCleanlyWithAConcurrentRun(@TempDir Path dataDir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
                ForgeServer server = new ForgeServer(store, 0)) {
            String host = "localhost";
            int port = server.port();
            byte[] value = Workload.fixedValue(16);

            int concurrency = 4;
            int perThread = 25;
            PhaseResult result = ConcurrentBenchRun.run(concurrency,
                    () -> newExecutor(host, port),
                    threadId -> Workload.puts("smoke-stats-t" + threadId, 0, perThread, value));

            LatencyStats stats = LatencyStats.of(result.latenciesNanos());
            assertEquals(concurrency * perThread, stats.count());
            assertTrue(stats.p50Nanos() <= stats.p95Nanos());
            assertTrue(stats.p95Nanos() <= stats.p99Nanos());
            assertTrue(stats.p99Nanos() <= stats.maxNanos());
        }
    }

    @Test
    @Timeout(30)
    void perThreadKeyRangesDoNotCollideAcrossThreads(@TempDir Path dataDir) throws Exception {
        // If per-thread key ranges ever accidentally collided, this would still
        // "succeed" at the protocol level (PUT/PUT/GET all return OK) but silently
        // undercount distinct keys — assert the actual distinct key count instead.
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
                ForgeServer server = new ForgeServer(store, 0)) {
            String host = "localhost";
            int port = server.port();
            byte[] value = Workload.fixedValue(16);

            int concurrency = 8;
            int perThread = 10;
            ConcurrentBenchRun.run(concurrency,
                    () -> newExecutor(host, port),
                    threadId -> Workload.puts("collide-t" + threadId, 0, perThread, value));

            try (ForgeClient verify = ForgeClient.connect(host, port)) {
                for (int t = 0; t < concurrency; t++) {
                    for (int i = 0; i < perThread; i++) {
                        Optional<byte[]> result = verify.get(Workload.key("collide-t" + t, i));
                        assertTrue(result.isPresent(), "expected thread " + t + "'s key " + i + " to be present");
                        assertArrayEquals(value, result.get());
                    }
                }
            }
        }
    }

    private static OpExecutor newExecutor(String host, int port) {
        try {
            return new NetworkOpExecutor(host, port);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
