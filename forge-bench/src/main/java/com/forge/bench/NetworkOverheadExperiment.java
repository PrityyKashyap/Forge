package com.forge.bench;

import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.forge.bench.BenchmarkConstants.E4_MEASURED_OPS;
import static com.forge.bench.BenchmarkConstants.E4_WARMUP_OPS;
import static com.forge.bench.BenchmarkConstants.VALUE_SIZE_BYTES;

/**
 * E4: network overhead. Compares the identical GET/PUT workload run two
 * ways: directly against a {@link ConcurrentLsmKeyValueStore} (no socket, no
 * wire protocol, no {@code ForgeServer}) versus through a real
 * {@code ForgeClient} over loopback TCP. The delta between the two isolates
 * what {@code FrameCodec} + the socket round trip + {@code ConnectionHandler}
 * actually cost, on top of the same storage engine.
 *
 * <p><b>Both sides get a brand-new, empty {@link ConcurrentLsmKeyValueStore}
 * (and, for the network side, a brand-new {@link ForgeServer}) for every
 * repeat.</b> An earlier version of this experiment reused one long-lived
 * server/store for the network side across all of E1/E3/E4 while giving the
 * in-process side a fresh store per repeat — an asymmetry that let unrelated
 * effects (accumulated flush activity, growing SSTable-scan-on-miss cost;
 * see BENCHMARKS.md's E4 section) leak into what was supposed to be an
 * isolated network-overhead measurement. Both sides are symmetric now.
 */
final class NetworkOverheadExperiment {

    private static final String GET_PREFIX = "e4-get";
    private static final String PUT_PREFIX = "e4-put";
    private static final int GET_DATASET_SIZE = E4_WARMUP_OPS + E4_MEASURED_OPS;

    private NetworkOverheadExperiment() {
    }

    static List<BenchmarkResult> run(Path networkDataDirBase, Path inProcessDataDirBase, List<BenchmarkResult> out)
            throws Exception {
        byte[] value = Workload.fixedValue(VALUE_SIZE_BYTES);

        for (int run = 1; run <= BenchmarkConstants.REPEATS; run++) {
            try (ConcurrentLsmKeyValueStore inProcessStore = new ConcurrentLsmKeyValueStore(
                    inProcessDataDirBase.resolve("run-" + run))) {
                BenchWorker.runUntimed(Workload.puts(GET_PREFIX, 0, GET_DATASET_SIZE, value),
                        new InProcessOpExecutor(inProcessStore));

                runGet("IN_PROCESS", run, () -> new InProcessOpExecutor(inProcessStore), out);
                runPut("IN_PROCESS", run, () -> new InProcessOpExecutor(inProcessStore), value, out);
            }

            try (ConcurrentLsmKeyValueStore networkStore = new ConcurrentLsmKeyValueStore(
                    networkDataDirBase.resolve("run-" + run));
                    ForgeServer server = new ForgeServer(networkStore, 0)) {
                String host = "localhost";
                int port = server.port();

                try (OpExecutor populate = new NetworkOpExecutor(host, port)) {
                    BenchWorker.runUntimed(Workload.puts(GET_PREFIX, 0, GET_DATASET_SIZE, value), populate);
                }

                runGet("LOOPBACK_TCP", run, () -> newNetworkExecutor(host, port), out);
                runPut("LOOPBACK_TCP", run, () -> newNetworkExecutor(host, port), value, out);
            }
        }
        return out;
    }

    private interface ExecutorFactory {
        OpExecutor create() throws IOException;
    }

    private static void runGet(String mode, int runIndex, ExecutorFactory factory, List<BenchmarkResult> out)
            throws IOException {
        List<BenchOperation> warmup = Workload.getsRoundRobin(GET_PREFIX, GET_DATASET_SIZE, 0, E4_WARMUP_OPS);
        List<BenchOperation> measured = Workload.getsRoundRobin(GET_PREFIX, GET_DATASET_SIZE, E4_WARMUP_OPS, E4_MEASURED_OPS);
        try (OpExecutor executor = factory.create()) {
            BenchWorker.runUntimed(warmup, executor);
            PhaseResult result = BenchWorker.runTimedPhase(measured, executor);
            out.add(BenchmarkResult.of("E4", "Network overhead: GET", "GET", mode,
                    1, runIndex, warmup.size(), VALUE_SIZE_BYTES, result));
        }
    }

    private static void runPut(String mode, int runIndex, ExecutorFactory factory, byte[] value, List<BenchmarkResult> out)
            throws IOException {
        List<BenchOperation> warmup = Workload.puts(PUT_PREFIX + "-warmup-" + mode + "-" + runIndex, 0, E4_WARMUP_OPS, value);
        List<BenchOperation> measured = Workload.puts(PUT_PREFIX + "-" + mode + "-" + runIndex, 0, E4_MEASURED_OPS, value);
        try (OpExecutor executor = factory.create()) {
            BenchWorker.runUntimed(warmup, executor);
            PhaseResult result = BenchWorker.runTimedPhase(measured, executor);
            out.add(BenchmarkResult.of("E4", "Network overhead: PUT", "PUT", mode,
                    1, runIndex, warmup.size(), VALUE_SIZE_BYTES, result));
        }
    }

    private static OpExecutor newNetworkExecutor(String host, int port) throws IOException {
        return new NetworkOpExecutor(host, port);
    }
}
