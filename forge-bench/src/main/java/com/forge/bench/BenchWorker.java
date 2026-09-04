package com.forge.bench;

import java.io.IOException;
import java.util.List;

/** Runs a fixed, ordered list of operations against one {@link OpExecutor}, timing each individually. */
final class BenchWorker {

    private BenchWorker() {
    }

    /** Executes every op in order, discarding the result — used for dataset population and warmup. */
    static void runUntimed(List<BenchOperation> ops, OpExecutor executor) throws IOException {
        for (BenchOperation op : ops) {
            executor.execute(op);
        }
    }

    /**
     * Executes every op in order, recording each one's wall-clock latency
     * via {@link System#nanoTime()} (monotonic; appropriate for measuring an
     * elapsed interval, not a timestamp).
     *
     * @return one latency in nanoseconds per op, same order as {@code ops}
     */
    static long[] runTimed(List<BenchOperation> ops, OpExecutor executor) throws IOException {
        long[] latencies = new long[ops.size()];
        for (int i = 0; i < ops.size(); i++) {
            long start = System.nanoTime();
            executor.execute(ops.get(i));
            latencies[i] = System.nanoTime() - start;
        }
        return latencies;
    }

    /** Single-threaded equivalent of {@link ConcurrentBenchRun#run}: one executor, no concurrency. */
    static PhaseResult runTimedPhase(List<BenchOperation> ops, OpExecutor executor) throws IOException {
        long phaseStart = System.nanoTime();
        long[] latencies = runTimed(ops, executor);
        long phaseEnd = System.nanoTime();
        return new PhaseResult(latencies, phaseEnd - phaseStart, latencies.length);
    }
}
