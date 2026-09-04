package com.forge.bench;

import java.util.Arrays;

/**
 * Latency percentiles computed from a fixed set of per-operation
 * measurements, in nanoseconds.
 *
 * <p>Uses the "nearest-rank" method on a sorted-ascending copy of the
 * samples: for percentile {@code p} (0–100) over {@code n} samples, the
 * reported value is the sample at 0-indexed position
 * {@code ceil(p/100 * n) - 1}, clamped to {@code [0, n-1]}. E.g. for
 * {@code n=1000}, p99 is the 990th-smallest sample (index 989) — the
 * standard definition used by tools like {@code wrk}/{@code JMH}, not an
 * interpolated one.
 */
public final class LatencyStats {

    private final long count;
    private final double averageNanos;
    private final long p50Nanos;
    private final long p95Nanos;
    private final long p99Nanos;
    private final long maxNanos;

    private LatencyStats(long count, double averageNanos, long p50Nanos, long p95Nanos, long p99Nanos, long maxNanos) {
        this.count = count;
        this.averageNanos = averageNanos;
        this.p50Nanos = p50Nanos;
        this.p95Nanos = p95Nanos;
        this.p99Nanos = p99Nanos;
        this.maxNanos = maxNanos;
    }

    /**
     * @param latenciesNanos per-operation latencies in nanoseconds; not mutated,
     *                       not retained (a sorted copy is made internally)
     * @throws IllegalArgumentException if {@code latenciesNanos} is empty
     */
    public static LatencyStats of(long[] latenciesNanos) {
        if (latenciesNanos.length == 0) {
            throw new IllegalArgumentException("latenciesNanos must not be empty");
        }
        long[] sorted = latenciesNanos.clone();
        Arrays.sort(sorted);

        long sum = 0;
        for (long v : sorted) {
            sum += v;
        }
        double average = sum / (double) sorted.length;

        return new LatencyStats(
                sorted.length,
                average,
                percentile(sorted, 50),
                percentile(sorted, 95),
                percentile(sorted, 99),
                sorted[sorted.length - 1]);
    }

    private static long percentile(long[] sortedAscending, double p) {
        int n = sortedAscending.length;
        int index = (int) Math.ceil(p / 100.0 * n) - 1;
        index = Math.max(0, Math.min(n - 1, index));
        return sortedAscending[index];
    }

    public long count() {
        return count;
    }

    public double averageNanos() {
        return averageNanos;
    }

    public double averageMillis() {
        return averageNanos / 1_000_000.0;
    }

    public long p50Nanos() {
        return p50Nanos;
    }

    public long p95Nanos() {
        return p95Nanos;
    }

    public long p99Nanos() {
        return p99Nanos;
    }

    public long maxNanos() {
        return maxNanos;
    }

    public double p50Millis() {
        return p50Nanos / 1_000_000.0;
    }

    public double p95Millis() {
        return p95Nanos / 1_000_000.0;
    }

    public double p99Millis() {
        return p99Nanos / 1_000_000.0;
    }

    public double maxMillis() {
        return maxNanos / 1_000_000.0;
    }

    @Override
    public String toString() {
        return String.format(
                "count=%d avg=%.3fms p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
                count, averageMillis(), p50Millis(), p95Millis(), p99Millis(), maxMillis());
    }
}
