package com.forge.bench;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Writes {@link BenchmarkResult} rows to a CSV file, header first. */
final class CsvReportWriter {

    private CsvReportWriter() {
    }

    static void write(Path csvFile, List<BenchmarkResult> results) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8)) {
            writer.write(BenchmarkResult.CSV_HEADER);
            writer.newLine();
            for (BenchmarkResult result : results) {
                writer.write(result.toCsvRow());
                writer.newLine();
            }
        }
    }
}
