package com.forge.bench;

import java.util.List;

import static com.forge.bench.BenchmarkConstants.CONCURRENCY_LEVELS;
import static com.forge.bench.BenchmarkConstants.E3_GET_TOTAL_MEASURED_OPS;
import static com.forge.bench.BenchmarkConstants.E3_GET_TOTAL_WARMUP_OPS;
import static com.forge.bench.BenchmarkConstants.E3_PUT_TOTAL_MEASURED_OPS;
import static com.forge.bench.BenchmarkConstants.E3_PUT_TOTAL_WARMUP_OPS;
import static com.forge.bench.BenchmarkConstants.GET_DATASET_SIZE;
import static com.forge.bench.BenchmarkConstants.REPEATS;
import static com.forge.bench.BenchmarkConstants.VALUE_SIZE_BYTES;

/**
 * E3: concurrency scaling. Sweeps client connection count across
 * {@link BenchmarkConstants#CONCURRENCY_LEVELS}, separately for a GET-only
 * and a PUT-only workload, holding <em>total</em> measured ops per level
 * roughly constant (split evenly across threads) rather than per-thread ops
 * constant — the latter would make total work (and thus run time) grow
 * linearly with concurrency, which is not what "how does throughput scale
 * with concurrency" is asking.
 *
 * <p>GET threads all read from one shared, pre-populated dataset
 * (round-robin, offset per thread) — real concurrent read traffic against
 * shared data. PUT threads each write into their own disjoint per-thread key
 * range, so this sweep measures concurrency scaling itself, not artificial
 * same-key contention (that's a deliberately separate, targeted correctness
 * test, not a throughput experiment).
 */
final class ConcurrencyScalingExperiment {

    private static final String GET_DATASET_PREFIX = "e3-get";

    private ConcurrencyScalingExperiment() {
    }

    static List<BenchmarkResult> run(String host, int port, List<BenchmarkResult> out) throws Exception {
        byte[] value = Workload.fixedValue(VALUE_SIZE_BYTES);

        try (OpExecutor populate = new NetworkOpExecutor(host, port)) {
            BenchWorker.runUntimed(Workload.puts(GET_DATASET_PREFIX, 0, GET_DATASET_SIZE, value), populate);
        }

        for (int concurrency : CONCURRENCY_LEVELS) {
            for (int run = 1; run <= REPEATS; run++) {
                runGet(concurrency, run, host, port, out);
                runPut(concurrency, run, host, port, value, out);
            }
        }
        return out;
    }

    private static void runGet(int concurrency, int runIndex, String host, int port, List<BenchmarkResult> out)
            throws Exception {
        int warmupPerThread = Math.max(1, E3_GET_TOTAL_WARMUP_OPS / concurrency);
        int measuredPerThread = Math.max(1, E3_GET_TOTAL_MEASURED_OPS / concurrency);

        PhaseResult warmup = ConcurrentBenchRun.run(concurrency,
                () -> newExecutor(host, port),
                threadId -> Workload.getsRoundRobin(GET_DATASET_PREFIX, GET_DATASET_SIZE,
                        (long) threadId * warmupPerThread, warmupPerThread));
        // warmup result intentionally discarded

        // Measured phase's read offsets start right after warmup's, per thread,
        // so the two phases don't read the exact same key sequence.
        long measuredBaseOffset = (long) warmupPerThread * concurrency;
        PhaseResult measured = ConcurrentBenchRun.run(concurrency,
                () -> newExecutor(host, port),
                threadId -> Workload.getsRoundRobin(GET_DATASET_PREFIX, GET_DATASET_SIZE,
                        measuredBaseOffset + (long) threadId * measuredPerThread, measuredPerThread));

        out.add(BenchmarkResult.of("E3", "Concurrent GET scaling", "GET", "CONCURRENT",
                concurrency, runIndex, (long) warmupPerThread * concurrency, VALUE_SIZE_BYTES, measured));
    }

    private static void runPut(int concurrency, int runIndex, String host, int port, byte[] value,
            List<BenchmarkResult> out) throws Exception {
        int warmupPerThread = Math.max(1, E3_PUT_TOTAL_WARMUP_OPS / concurrency);
        int measuredPerThread = Math.max(1, E3_PUT_TOTAL_MEASURED_OPS / concurrency);
        String warmupPrefix = "e3-put-warmup-c" + concurrency + "-r" + runIndex;
        String measuredPrefix = "e3-put-c" + concurrency + "-r" + runIndex;

        ConcurrentBenchRun.run(concurrency,
                () -> newExecutor(host, port),
                threadId -> Workload.puts(warmupPrefix + "-t" + threadId, 0, warmupPerThread, value));
        // warmup result intentionally discarded

        PhaseResult measured = ConcurrentBenchRun.run(concurrency,
                () -> newExecutor(host, port),
                threadId -> Workload.puts(measuredPrefix + "-t" + threadId, 0, measuredPerThread, value));

        out.add(BenchmarkResult.of("E3", "Concurrent PUT scaling", "PUT", "CONCURRENT",
                concurrency, runIndex, (long) warmupPerThread * concurrency, VALUE_SIZE_BYTES, measured));
    }

    private static OpExecutor newExecutor(String host, int port) {
        try {
            return new NetworkOpExecutor(host, port);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
