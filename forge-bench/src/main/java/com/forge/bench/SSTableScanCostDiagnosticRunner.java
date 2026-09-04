package com.forge.bench;

import com.forge.storage.ConcurrentLsmKeyValueStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Diagnostic-only entry point: directly measures how GET latency for a
 * key that has fallen out of the active MemTable (i.e. lives only in an
 * on-disk SSTable) changes as more SSTables accumulate — used to confirm or
 * rule out "SSTable scan-on-miss cost" as the explanation for an anomaly
 * seen in a full-suite E5 run. Not part of the regular Phase 6 benchmark
 * output; in-process only (no network), small and fast by design so it
 * can't itself run away the way the full E5 experiment did.
 *
 * <p>{@link com.forge.storage.sstable.SSTableReader}'s own Javadoc already
 * states its design plainly: "performs a genuine scan of the on-disk file
 * every time... deliberately slower than an in-memory index." This runner
 * turns that documented design fact into an actual measured number.
 */
public final class SSTableScanCostDiagnosticRunner {

    private SSTableScanCostDiagnosticRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path tempRoot = Files.createTempDirectory("forge-bench-sstable-diag-");
        // A small flush threshold so a handful of PUTs is enough to force several flushes.
        long flushThresholdBytes = 32 * 1024;
        int keysPerBatch = 400; // ~400 * (~30 bytes key + 100 bytes value + overhead) safely exceeds 32KB

        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(tempRoot.resolve("store"), flushThresholdBytes)) {
            byte[] value = Workload.fixedValue(100);

            System.out.println("batch  sstables-approx  get-avg-us(first-batch-key)  get-avg-us(latest-batch-key)");

            String firstKey = Workload.key("diag", 0);
            store.put(firstKey, value);

            for (int batch = 0; batch < 8; batch++) {
                long base = 1L + (long) batch * keysPerBatch;
                for (int i = 0; i < keysPerBatch; i++) {
                    store.put(Workload.key("diag", base + i), value);
                }
                // Force a flush deterministically so batch boundaries line up with SSTable boundaries.
                store.flush();

                String latestKey = Workload.key("diag", base + keysPerBatch - 1);

                double firstKeyAvgMicros = averageGetMicros(store, firstKey, 200);
                double latestKeyAvgMicros = averageGetMicros(store, latestKey, 200);

                System.out.printf("%5d  %15d  %26.2f  %26.2f%n",
                        batch + 1, batch + 1, firstKeyAvgMicros, latestKeyAvgMicros);
            }
        }
    }

    private static double averageGetMicros(ConcurrentLsmKeyValueStore store, String key, int samples) {
        long totalNanos = 0;
        for (int i = 0; i < samples; i++) {
            long start = System.nanoTime();
            Optional<byte[]> result = store.get(key);
            totalNanos += System.nanoTime() - start;
            if (result.isEmpty()) {
                throw new IllegalStateException("expected key to be present: " + key);
            }
        }
        return (totalNanos / (double) samples) / 1000.0;
    }
}
