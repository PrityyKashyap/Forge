package com.forge.bench;

/**
 * The outcome of one timed phase (sequential or concurrent): every op's
 * latency plus the phase's total wall-clock elapsed time, measured around
 * the whole phase — not summed from individual thread durations, which
 * would double-count time under concurrency.
 */
record PhaseResult(long[] latenciesNanos, long elapsedNanos, long totalOps) {

    double elapsedSeconds() {
        return elapsedNanos / 1_000_000_000.0;
    }

    double throughputOpsPerSec() {
        return totalOps / elapsedSeconds();
    }
}
