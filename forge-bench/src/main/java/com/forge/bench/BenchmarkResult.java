package com.forge.bench;

import java.time.Instant;

/**
 * One row of measured benchmark output: everything needed to reproduce and
 * interpret a single (experiment, configuration, run) result. Never
 * constructed with invented numbers — every field here is either an input
 * parameter the caller chose, or a value computed from real measurements
 * taken during the run.
 */
record BenchmarkResult(
        String experimentId,
        String description,
        Instant timestamp,
        String opType,
        String mode,
        int concurrency,
        int runIndex,
        long warmupOps,
        long measuredOps,
        int valueSizeBytes,
        double elapsedSeconds,
        double throughputOpsPerSec,
        double avgLatencyMs,
        double p50LatencyMs,
        double p95LatencyMs,
        double p99LatencyMs,
        double maxLatencyMs) {

    static BenchmarkResult of(String experimentId, String description, String opType, String mode,
            int concurrency, int runIndex, long warmupOps, int valueSizeBytes,
            PhaseResult runResult) {
        LatencyStats stats = LatencyStats.of(runResult.latenciesNanos());
        return new BenchmarkResult(
                experimentId, description, Instant.now(), opType, mode, concurrency, runIndex,
                warmupOps, stats.count(), valueSizeBytes,
                runResult.elapsedSeconds(), runResult.throughputOpsPerSec(),
                stats.averageMillis(), stats.p50Millis(), stats.p95Millis(), stats.p99Millis(), stats.maxMillis());
    }

    static final String CSV_HEADER = String.join(",",
            "experiment_id", "description", "timestamp", "op_type", "mode", "concurrency", "run_index",
            "warmup_ops", "measured_ops", "value_size_bytes", "elapsed_seconds", "throughput_ops_per_sec",
            "avg_latency_ms", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "max_latency_ms");

    String toCsvRow() {
        return String.join(",",
                experimentId,
                csvEscape(description),
                timestamp.toString(),
                opType,
                mode,
                Integer.toString(concurrency),
                Integer.toString(runIndex),
                Long.toString(warmupOps),
                Long.toString(measuredOps),
                Integer.toString(valueSizeBytes),
                String.format("%.6f", elapsedSeconds),
                String.format("%.3f", throughputOpsPerSec),
                String.format("%.4f", avgLatencyMs),
                String.format("%.4f", p50LatencyMs),
                String.format("%.4f", p95LatencyMs),
                String.format("%.4f", p99LatencyMs),
                String.format("%.4f", maxLatencyMs));
    }

    private static String csvEscape(String s) {
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }
}
