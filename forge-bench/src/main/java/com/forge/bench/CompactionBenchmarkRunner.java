package com.forge.bench;

import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Phase 13, E16: measures compaction's real effect on-disk (write
 * amplification / space reclaimed) and on point-lookup cost (a read-
 * amplification proxy: SSTable count, and measured miss-lookup latency,
 * before vs. after compaction) — real numbers from a real
 * {@link ConcurrentLsmKeyValueStore}, no estimates. Continues DESIGN.md's
 * E-numbering past Phase 12's E15.
 *
 * <p><b>Method</b>: populate a store with a small flush threshold and
 * compaction <em>disabled</em> (a huge trigger count) by repeatedly
 * overwriting the same key set — this forces many small SSTables, most of
 * whose content is immediately-stale overwritten data, which is exactly
 * what compaction exists to reclaim. Measure the on-disk footprint and
 * SSTable count, then explicitly trigger one full compaction
 * ({@link ConcurrentLsmKeyValueStore#compact()}) and measure again. Because
 * nothing consolidates anything before that explicit call, the "before"
 * on-disk total is exactly the total bytes every flush in this run ever
 * wrote — a real, direct write-amplification measurement, not an estimate.
 */
public final class CompactionBenchmarkRunner {

    private static final Logger log = LoggerFactory.getLogger(CompactionBenchmarkRunner.class);

    private static final int VALUE_SIZE_BYTES = 100;
    private static final int KEY_COUNT = 50;
    private static final int OVERWRITE_ROUNDS = 10;
    private static final long FLUSH_THRESHOLD_BYTES = 2048;
    private static final int MISS_LOOKUP_COUNT = 2000;

    private CompactionBenchmarkRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path resultsDir = Path.of("forge-bench", "results");
        Files.createDirectories(resultsDir);
        String runId = Instant.now().toString().replace(":", "-");
        Path csv = resultsDir.resolve("phase13-amplification-" + runId + ".csv");

        Path tempRoot = Files.createTempDirectory("forge-bench-compaction-");
        List<AmplificationBenchmarkResult> results = new ArrayList<>();
        try {
            run(tempRoot, results);
        } finally {
            deleteRecursively(tempRoot);
        }

        writeCsv(csv, results);
        printHumanSummary(System.out, results, csv);
    }

    private static void run(Path dir, List<AmplificationBenchmarkResult> out) throws IOException {
        byte[] value = Workload.fixedValue(VALUE_SIZE_BYTES);
        long logicalBytesPut = 0;

        try (ConcurrentLsmKeyValueStore store =
                new ConcurrentLsmKeyValueStore(dir, FLUSH_THRESHOLD_BYTES, Integer.MAX_VALUE)) {
            for (int round = 0; round < OVERWRITE_ROUNDS; round++) {
                for (int i = 0; i < KEY_COUNT; i++) {
                    String key = Workload.key("amp", i);
                    store.put(key, value);
                    logicalBytesPut += key.getBytes(StandardCharsets.UTF_8).length + value.length;
                }
            }
            store.flush(); // capture whatever the last round left sitting in the active MemTable

            long beforeDataBytes = totalBytes(dir, "sstable-*.sst");
            long beforeBloomBytes = totalBytes(dir, "sstable-*.bloom");
            long beforeFileCount = countFiles(dir, "sstable-*.sst");
            long[] missLatenciesBefore = timeMisses(store, MISS_LOOKUP_COUNT);

            store.compact();

            long afterDataBytes = totalBytes(dir, "sstable-*.sst");
            long afterBloomBytes = totalBytes(dir, "sstable-*.bloom");
            long afterFileCount = countFiles(dir, "sstable-*.sst");
            long[] missLatenciesAfter = timeMisses(store, MISS_LOOKUP_COUNT);

            for (int i = 0; i < KEY_COUNT; i++) {
                byte[] got = store.get(Workload.key("amp", i))
                        .orElseThrow(() -> new IllegalStateException("key vanished across compaction"));
                if (!Arrays.equals(got, value)) {
                    throw new IllegalStateException("compaction produced a wrong value for a live key");
                }
            }

            add(out, "sstable_count_before_compaction", beforeFileCount, "files");
            add(out, "sstable_count_after_compaction", afterFileCount, "files");
            add(out, "on_disk_data_bytes_before_compaction", beforeDataBytes, "bytes");
            add(out, "on_disk_data_bytes_after_compaction", afterDataBytes, "bytes");
            add(out, "bloom_sidecar_bytes_before_compaction", beforeBloomBytes, "bytes");
            add(out, "bloom_sidecar_bytes_after_compaction", afterBloomBytes, "bytes");
            add(out, "logical_bytes_put", logicalBytesPut, "bytes");
            add(out, "write_amplification_flush_to_disk", beforeDataBytes / (double) logicalBytesPut, "ratio");
            add(out, "space_reclaimed_by_compaction", beforeDataBytes - afterDataBytes, "bytes");
            add(out, "miss_lookup_avg_latency_before_compaction", averageMillis(missLatenciesBefore), "ms");
            add(out, "miss_lookup_avg_latency_after_compaction", averageMillis(missLatenciesAfter), "ms");
        }
    }

    private static long[] timeMisses(ConcurrentLsmKeyValueStore store, int count) {
        long[] latencies = new long[count];
        for (int i = 0; i < count; i++) {
            String key = Workload.key("amp-miss", i);
            long start = System.nanoTime();
            store.get(key);
            latencies[i] = System.nanoTime() - start;
        }
        return latencies;
    }

    private static double averageMillis(long[] latenciesNanos) {
        return LatencyStats.of(latenciesNanos).averageMillis();
    }

    private static void add(List<AmplificationBenchmarkResult> out, String metric, double value, String unit) {
        out.add(new AmplificationBenchmarkResult("E16", "Compaction read/write amplification",
                Instant.now(), metric, value, unit));
    }

    private static long totalBytes(Path dir, String glob) throws IOException {
        long total = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            for (Path path : stream) {
                total += Files.size(path);
            }
        }
        return total;
    }

    private static long countFiles(Path dir, String glob) throws IOException {
        long count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            for (Path ignored : stream) {
                count++;
            }
        }
        return count;
    }

    private static void writeCsv(Path file, List<AmplificationBenchmarkResult> results) throws IOException {
        try (var writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            writer.write(AmplificationBenchmarkResult.CSV_HEADER);
            writer.newLine();
            for (AmplificationBenchmarkResult r : results) {
                writer.write(r.toCsvRow());
                writer.newLine();
            }
        }
    }

    private static void printHumanSummary(PrintStream out, List<AmplificationBenchmarkResult> results, Path csv) {
        out.println();
        out.println("==================== FORGE Phase 13 Compaction Amplification Summary ====================");
        out.printf("CSV: %s (%d rows)%n", csv.toAbsolutePath(), results.size());
        out.println("-------------------------------------------------------------------------------------------");
        for (AmplificationBenchmarkResult r : results) {
            out.printf("%-45s %14.3f %s%n", r.metric(), r.value(), r.unit());
        }
        out.println("=============================================================================================");
    }

    private static void deleteRecursively(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    log.warn("failed to delete temp file {} during cleanup", path, e);
                }
            });
        } catch (IOException e) {
            log.warn("failed to walk temp directory {} during cleanup", root, e);
        }
    }
}
