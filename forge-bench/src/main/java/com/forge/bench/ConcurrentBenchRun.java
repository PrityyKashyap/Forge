package com.forge.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Runs {@code concurrency} worker threads, each on its own {@link OpExecutor}
 * (its own connection, for network runs), synchronized to start their timed
 * work at the same instant, and reports one merged result.
 *
 * <p><b>Throughput methodology</b>: connection setup happens before the
 * synchronized start and is never timed. {@code elapsedNanos} is wall-clock
 * time from just before every thread is released to just after the last one
 * finishes — <em>not</em> a sum of individual thread durations, which would
 * double-count under concurrency. {@code throughputOpsPerSec = totalOps /
 * (elapsedNanos / 1e9)}.
 */
final class ConcurrentBenchRun {

    private ConcurrentBenchRun() {
    }

    /**
     * @param concurrency     number of worker threads / connections
     * @param executorFactory creates one fresh {@link OpExecutor} per thread, called
     *                        before the timed phase starts (connection setup excluded)
     * @param opsForThread    the ops thread {@code i} (0-indexed) will run, timed
     */
    static PhaseResult run(int concurrency, Supplier<OpExecutor> executorFactory, IntFunction<List<BenchOperation>> opsForThread)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch go = new CountDownLatch(1);

        List<Future<long[]>> futures = new ArrayList<>(concurrency);
        for (int t = 0; t < concurrency; t++) {
            int threadId = t;
            Callable<long[]> task = () -> {
                try (OpExecutor executor = executorFactory.get()) {
                    List<BenchOperation> ops = opsForThread.apply(threadId);
                    ready.countDown();
                    go.await();
                    return BenchWorker.runTimed(ops, executor);
                }
            };
            futures.add(pool.submit(task));
        }

        ready.await();
        long phaseStart = System.nanoTime();
        go.countDown();

        List<long[]> perThreadLatencies = new ArrayList<>(concurrency);
        long totalOps = 0;
        for (Future<long[]> future : futures) {
            long[] latencies = future.get();
            perThreadLatencies.add(latencies);
            totalOps += latencies.length;
        }
        long phaseEnd = System.nanoTime();

        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }

        return new PhaseResult(merge(perThreadLatencies, (int) totalOps), phaseEnd - phaseStart, totalOps);
    }

    private static long[] merge(List<long[]> arrays, int totalSize) {
        long[] merged = new long[totalSize];
        int offset = 0;
        for (long[] arr : arrays) {
            System.arraycopy(arr, 0, merged, offset, arr.length);
            offset += arr.length;
        }
        return merged;
    }
}
