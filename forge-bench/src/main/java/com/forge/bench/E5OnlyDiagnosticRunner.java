package com.forge.bench;

import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Diagnostic-only entry point: runs E5 in isolation, immediately against a
 * freshly started server, with nothing else run first — used to determine
 * whether an anomalous E5 result seen in a full-suite run reflects E5 itself
 * or an effect of what ran before it. Not part of the regular Phase 6
 * benchmark output; not referenced by {@link BenchmarkRunner}.
 */
public final class E5OnlyDiagnosticRunner {

    private E5OnlyDiagnosticRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path tempRoot = Files.createTempDirectory("forge-bench-e5-diag-");
        List<BenchmarkResult> results = new ArrayList<>();
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(tempRoot.resolve("store"));
                ForgeServer server = new ForgeServer(store, 0)) {
            MixedWorkloadExperiment.run("localhost", server.port(), results);
        }
        PrintStream out = System.out;
        out.printf("%-4s %-8s %-6s %12s %10s %10s %10s%n", "exp", "opType", "#run", "ops/sec", "avg(ms)", "p99(ms)", "max(ms)");
        for (BenchmarkResult r : results) {
            out.printf("%-4s %-8s %-6d %12.1f %10.3f %10.3f %10.3f%n",
                    r.experimentId(), r.opType(), r.runIndex(), r.throughputOpsPerSec(), r.avgLatencyMs(), r.p99LatencyMs(), r.maxLatencyMs());
        }
    }
}
