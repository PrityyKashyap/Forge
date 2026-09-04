package com.forge.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.forge.bench.BenchmarkConstants.E5_CONCURRENCY;
import static com.forge.bench.BenchmarkConstants.E5_DATASET_SIZE;
import static com.forge.bench.BenchmarkConstants.E5_RANDOM_SEED;
import static com.forge.bench.BenchmarkConstants.E5_READ_FRACTION;
import static com.forge.bench.BenchmarkConstants.E5_TOTAL_MEASURED_OPS;
import static com.forge.bench.BenchmarkConstants.E5_TOTAL_WARMUP_OPS;
import static com.forge.bench.BenchmarkConstants.REPEATS;
import static com.forge.bench.BenchmarkConstants.VALUE_SIZE_BYTES;

/**
 * E5: mixed read/write workload — {@link BenchmarkConstants#E5_READ_FRACTION}
 * GET / remainder PUT, uniform key distribution, at a fixed, moderate
 * concurrency level ({@link BenchmarkConstants#E5_CONCURRENCY}, chosen as a
 * representative point already covered by E3's sweep rather than re-running
 * the full concurrency range for this too).
 *
 * <p>Key skew (hot-key/Zipfian distribution) is explicitly <b>not</b>
 * implemented this phase — DESIGN.md's E5 definition names it as a variable
 * to sweep, but it isn't part of the Phase 6 request and would need a real
 * Zipfian generator to do honestly; BENCHMARKS.md marks that sub-variable
 * NOT RUN rather than faking it with a plausible-looking distribution.
 *
 * <p>Reports combined throughput/latency plus separate GET-only and
 * PUT-only latency breakdowns — mixing very different operation costs into
 * one percentile series obscures what's actually happening at each.
 */
final class MixedWorkloadExperiment {

    private static final String KEY_PREFIX = "e5-mixed";

    private MixedWorkloadExperiment() {
    }

    static List<BenchmarkResult> run(String host, int port, List<BenchmarkResult> out) throws Exception {
        byte[] value = Workload.fixedValue(VALUE_SIZE_BYTES);

        try (OpExecutor populate = new NetworkOpExecutor(host, port)) {
            BenchWorker.runUntimed(Workload.puts(KEY_PREFIX, 0, E5_DATASET_SIZE, value), populate);
        }

        int warmupPerThread = Math.max(1, E5_TOTAL_WARMUP_OPS / E5_CONCURRENCY);
        int measuredPerThread = Math.max(1, E5_TOTAL_MEASURED_OPS / E5_CONCURRENCY);

        for (int run = 1; run <= REPEATS; run++) {
            runWarmup(host, port, warmupPerThread, run, value);
            TypedResult result = runMeasured(host, port, measuredPerThread, run, value);

            out.add(BenchmarkResult.of("E5", "Mixed workload, combined (" + (int) (E5_READ_FRACTION * 100) + "% GET)",
                    "MIXED", "CONCURRENT", E5_CONCURRENCY, run, (long) warmupPerThread * E5_CONCURRENCY,
                    VALUE_SIZE_BYTES, result.combined()));
            if (result.getLatencies().length > 0) {
                out.add(BenchmarkResult.of("E5", "Mixed workload, GET-only latency slice", "GET", "CONCURRENT",
                        E5_CONCURRENCY, run, 0, VALUE_SIZE_BYTES,
                        new PhaseResult(result.getLatencies(), result.combined().elapsedNanos(), result.getLatencies().length)));
            }
            if (result.putLatencies().length > 0) {
                out.add(BenchmarkResult.of("E5", "Mixed workload, PUT-only latency slice", "PUT", "CONCURRENT",
                        E5_CONCURRENCY, run, 0, VALUE_SIZE_BYTES,
                        new PhaseResult(result.putLatencies(), result.combined().elapsedNanos(), result.putLatencies().length)));
            }
        }
        return out;
    }

    private record TypedResult(PhaseResult combined, long[] getLatencies, long[] putLatencies) {
    }

    private record ThreadOutcome(List<BenchOperation> ops, long[] latencies) {
    }

    private record ConcurrentRunOutcome(List<ThreadOutcome> perThread, long elapsedNanos) {
    }

    private static void runWarmup(String host, int port, int perThread, int runIndex, byte[] value) throws Exception {
        runConcurrent(host, port, perThread, runIndex, /* isWarmup */ true, value);
    }

    private static TypedResult runMeasured(String host, int port, int perThread, int runIndex, byte[] value)
            throws Exception {
        ConcurrentRunOutcome run = runConcurrent(host, port, perThread, runIndex, /* isWarmup */ false, value);
        List<ThreadOutcome> outcomes = run.perThread();

        int totalOps = outcomes.stream().mapToInt(o -> o.latencies().length).sum();
        long[] combinedLatencies = new long[totalOps];
        List<Long> getLatencies = new ArrayList<>();
        List<Long> putLatencies = new ArrayList<>();

        int cursor = 0;
        for (ThreadOutcome outcome : outcomes) {
            for (int i = 0; i < outcome.latencies().length; i++) {
                long latency = outcome.latencies()[i];
                combinedLatencies[cursor++] = latency;
                if (outcome.ops().get(i) instanceof BenchOperation.Get) {
                    getLatencies.add(latency);
                } else {
                    putLatencies.add(latency);
                }
            }
        }

        return new TypedResult(
                new PhaseResult(combinedLatencies, run.elapsedNanos(), totalOps),
                toArray(getLatencies),
                toArray(putLatencies));
    }

    private static ConcurrentRunOutcome runConcurrent(String host, int port, int perThread, int runIndex,
            boolean isWarmup, byte[] value) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(E5_CONCURRENCY);
        CountDownLatch ready = new CountDownLatch(E5_CONCURRENCY);
        CountDownLatch go = new CountDownLatch(1);

        List<Future<ThreadOutcome>> futures = new ArrayList<>(E5_CONCURRENCY);
        for (int t = 0; t < E5_CONCURRENCY; t++) {
            long seed = E5_RANDOM_SEED + runIndex * 1000L + t + (isWarmup ? 0 : 500_000L);
            List<BenchOperation> ops = Workload.mixedReadWrite(KEY_PREFIX, E5_DATASET_SIZE, E5_READ_FRACTION,
                    perThread, seed, value);
            Callable<ThreadOutcome> task = () -> {
                try (OpExecutor executor = new NetworkOpExecutor(host, port)) {
                    ready.countDown();
                    go.await();
                    long[] latencies = BenchWorker.runTimed(ops, executor);
                    return new ThreadOutcome(ops, latencies);
                }
            };
            futures.add(pool.submit(task));
        }

        ready.await();
        long phaseStart = System.nanoTime();
        go.countDown();

        List<ThreadOutcome> outcomes = new ArrayList<>(E5_CONCURRENCY);
        for (Future<ThreadOutcome> future : futures) {
            outcomes.add(future.get());
        }
        long phaseEnd = System.nanoTime();

        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }

        return new ConcurrentRunOutcome(outcomes, phaseEnd - phaseStart);
    }

    private static long[] toArray(List<Long> boxed) {
        long[] result = new long[boxed.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = boxed.get(i);
        }
        return result;
    }
}
