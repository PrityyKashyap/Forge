package com.forge.bench;

import java.time.Instant;

/**
 * One measured metric from {@link CompactionBenchmarkRunner} — a simple
 * name/value/unit row rather than {@link BenchmarkResult}'s fixed
 * throughput/latency shape, since Phase 13's amplification metrics are a
 * mix of byte counts, file counts, ratios, and latencies. Same rule as
 * every other benchmark result type in this project: every value here is
 * either an input parameter or computed from a real measurement taken
 * during the run — never invented.
 */
record AmplificationBenchmarkResult(
        String experimentId,
        String description,
        Instant timestamp,
        String metric,
        double value,
        String unit) {

    static final String CSV_HEADER = String.join(",",
            "experiment_id", "description", "timestamp", "metric", "value", "unit");

    String toCsvRow() {
        return String.join(",",
                experimentId,
                csvEscape(description),
                timestamp.toString(),
                metric,
                String.format("%.6f", value),
                unit);
    }

    private static String csvEscape(String s) {
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }
}
