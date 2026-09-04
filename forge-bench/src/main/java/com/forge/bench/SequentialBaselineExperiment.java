package com.forge.bench;

import java.io.IOException;
import java.util.List;

import static com.forge.bench.BenchmarkConstants.E1_MEASURED_OPS;
import static com.forge.bench.BenchmarkConstants.E1_WARMUP_OPS;
import static com.forge.bench.BenchmarkConstants.GET_DATASET_SIZE;
import static com.forge.bench.BenchmarkConstants.REPEATS;
import static com.forge.bench.BenchmarkConstants.VALUE_SIZE_BYTES;

/**
 * E1: sequential (single connection, single thread) GET/PUT/DELETE
 * throughput and latency, over a real loopback TCP connection — this is the
 * "first real benchmark... over the network" DESIGN.md's metrics table
 * calls for at Phase 6 (E1 was in-process-only through Phase 4).
 */
final class SequentialBaselineExperiment {

    private static final String GET_DATASET_PREFIX = "e1-get";
    private static final String PUT_WARMUP_PREFIX = "e1-put-warmup";
    private static final String PUT_PREFIX = "e1-put";
    private static final String DELETE_PREFIX = "e1-delete";

    private SequentialBaselineExperiment() {
    }

    static List<BenchmarkResult> run(String host, int port, List<BenchmarkResult> out) throws IOException {
        byte[] value = Workload.fixedValue(VALUE_SIZE_BYTES);

        try (OpExecutor populate = new NetworkOpExecutor(host, port)) {
            BenchWorker.runUntimed(Workload.puts(GET_DATASET_PREFIX, 0, GET_DATASET_SIZE, value), populate);
            // Pre-populate exactly enough keys that the first repeat deletes real,
            // previously-existing keys; later repeats redelete the same (now-absent)
            // keys, which costs the same amount of engine work (lookup + WAL
            // append+fsync + tombstone insert) either way — see Workload's Javadoc
            // discussion in BENCHMARKS.md for why this doesn't bias the measurement.
            BenchWorker.runUntimed(Workload.puts(DELETE_PREFIX, 0, E1_WARMUP_OPS + E1_MEASURED_OPS, value), populate);
        }

        for (int run = 1; run <= REPEATS; run++) {
            runOne("GET", run, host, port, value, out);
            runOne("PUT", run, host, port, value, out);
            runOne("DELETE", run, host, port, value, out);
        }
        return out;
    }

    private static void runOne(String opType, int runIndex, String host, int port, byte[] value,
            List<BenchmarkResult> out) throws IOException {
        List<BenchOperation> warmupOps;
        List<BenchOperation> measuredOps;
        switch (opType) {
            case "GET" -> {
                warmupOps = Workload.getsRoundRobin(GET_DATASET_PREFIX, GET_DATASET_SIZE, 0, E1_WARMUP_OPS);
                measuredOps = Workload.getsRoundRobin(GET_DATASET_PREFIX, GET_DATASET_SIZE, E1_WARMUP_OPS, E1_MEASURED_OPS);
            }
            case "PUT" -> {
                warmupOps = Workload.puts(PUT_WARMUP_PREFIX, 0, E1_WARMUP_OPS, value);
                measuredOps = Workload.puts(PUT_PREFIX, 0, E1_MEASURED_OPS, value);
            }
            case "DELETE" -> {
                warmupOps = Workload.deletes(DELETE_PREFIX, 0, E1_WARMUP_OPS);
                measuredOps = Workload.deletes(DELETE_PREFIX, E1_WARMUP_OPS, E1_MEASURED_OPS);
            }
            default -> throw new IllegalArgumentException("unknown opType: " + opType);
        }

        try (OpExecutor executor = new NetworkOpExecutor(host, port)) {
            BenchWorker.runUntimed(warmupOps, executor);
            PhaseResult result = BenchWorker.runTimedPhase(measuredOps, executor);
            out.add(BenchmarkResult.of("E1", "Sequential " + opType + " baseline (loopback TCP)",
                    opType, "SEQUENTIAL", 1, runIndex, warmupOps.size(), VALUE_SIZE_BYTES, result));
        }
    }
}
