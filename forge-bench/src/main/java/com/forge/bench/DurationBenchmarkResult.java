package com.forge.bench;

import java.time.Instant;

/**
 * One row of measured single-duration benchmark output — for experiments
 * whose result is "how long did this one operation take" (recovery time,
 * replica catch-up time) rather than a throughput/latency series over many
 * ops ({@link BenchmarkResult}'s shape). Never constructed with invented
 * numbers, same rule as {@link BenchmarkResult}.
 */
record DurationBenchmarkResult(
        String experimentId,
        String description,
        Instant timestamp,
        String configLabel,
        long datasetSize,
        int runIndex,
        double elapsedSeconds) {

    static final String CSV_HEADER = String.join(",",
            "experiment_id", "description", "timestamp", "config_label", "dataset_size", "run_index", "elapsed_seconds");

    String toCsvRow() {
        return String.join(",",
                experimentId,
                csvEscape(description),
                timestamp.toString(),
                configLabel,
                Long.toString(datasetSize),
                Integer.toString(runIndex),
                String.format("%.6f", elapsedSeconds));
    }

    private static String csvEscape(String s) {
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }
}
