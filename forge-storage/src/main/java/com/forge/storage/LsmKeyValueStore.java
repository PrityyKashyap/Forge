package com.forge.storage;

import com.forge.storage.memtable.MemTable;
import com.forge.storage.memtable.StoredEntry;
import com.forge.storage.sstable.SSTableReader;
import com.forge.storage.sstable.SSTableWriter;
import com.forge.storage.wal.WalRecord;
import com.forge.storage.wal.WriteAheadLog;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Phase 3 storage engine: a {@link KeyValueStore} backed by a
 * {@link WriteAheadLog} for durability, an active {@link MemTable} for
 * recent writes, and zero or more immutable {@link SSTableReader}s for
 * everything already flushed to disk.
 *
 * <p>This class is the only one that knows all three exist —
 * {@code WriteAheadLog}, {@code MemTable}, and the {@code sstable} package
 * are each self-contained and unaware of each other. It composes them
 * exactly as {@link DurableKeyValueStore} composes a WAL and an
 * {@link InMemoryKeyValueStore}; that simpler, WAL-only engine is
 * deliberately left unchanged and available — this class supersedes it in
 * capability, not in place.
 *
 * <p><b>Write path</b>: {@code put}/{@code delete} append-and-force a WAL
 * record <em>first</em>, then apply the mutation to the MemTable — the same
 * write-ahead ordering as Phase 2, now extended one layer further: a flush
 * writes a new SSTable to a temp file, {@code force}s it, atomically renames
 * it into place, and only <em>then</em> truncates the WAL. Reordering any of
 * this can lose data — see DESIGN.md's Phase 3 crash-scenario proof.
 *
 * <p><b>Read path</b>: {@code get} checks the current MemTable first, then
 * SSTables newest to oldest, stopping at the first generation with any
 * entry for the key — a {@link StoredEntry.Value} is returned, a
 * {@link StoredEntry.Tombstone} means "deleted here, stop looking further."
 *
 * <p><b>Recovery</b>: on construction, existing SSTables are discovered and
 * validated, the highest watermark {@code F} among them is computed, the WAL
 * is opened, and only WAL records with {@code sequenceNumber() > F} are
 * replayed into a fresh MemTable — records at or below {@code F} are already
 * durably reflected in the newest SSTable.
 *
 * <p><b>Not thread-safe</b> — exactly like every class it composes. Flushing
 * is synchronous: it blocks the triggering {@code put}/{@code delete} call.
 */
public final class LsmKeyValueStore implements KeyValueStore, Closeable {

    private static final String WAL_FILE_NAME = "forge.wal";
    private static final String FLUSH_TEMP_FILE_NAME = "flush.tmp";

    /** An explicitly unbenchmarked placeholder default — see DESIGN.md on not fabricating tuned numbers. */
    private static final long DEFAULT_FLUSH_THRESHOLD_BYTES = 4L * 1024 * 1024;

    private final Path dataDirectory;
    private final long flushThresholdBytes;
    private final WriteAheadLog wal;
    private final List<SSTableReader> sstables; // newest first

    private MemTable memTable;

    public LsmKeyValueStore(Path dataDirectory) throws IOException {
        this(dataDirectory, DEFAULT_FLUSH_THRESHOLD_BYTES);
    }

    public LsmKeyValueStore(Path dataDirectory, long flushThresholdBytes) throws IOException {
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        if (flushThresholdBytes <= 0) {
            throw new IllegalArgumentException("flushThresholdBytes must be positive");
        }
        this.flushThresholdBytes = flushThresholdBytes;

        Files.createDirectories(dataDirectory);
        this.sstables = discoverSSTables(dataDirectory);

        long watermark = sstables.stream().mapToLong(SSTableReader::maxSequenceNumber).max().orElse(0L);

        try {
            this.wal = WriteAheadLog.open(dataDirectory.resolve(WAL_FILE_NAME));
        } catch (IOException | RuntimeException e) {
            closeAll(sstables, e);
            throw e;
        }
        // A WAL previously truncated-to-empty by a completed flush has no
        // durable memory of that; the watermark that knowledge lives in is
        // the newest SSTable's header, which only this class has both of.
        // See WriteAheadLog.ensureNextSequenceNumberAtLeast's Javadoc.
        wal.ensureNextSequenceNumberAtLeast(watermark + 1);

        MemTable rebuilt = new MemTable();
        for (WalRecord record : wal.recoveredRecords()) {
            if (record.sequenceNumber() <= watermark) {
                continue; // already durably reflected in the newest SSTable
            }
            switch (record) {
                case WalRecord.Put put -> rebuilt.put(put.key(), put.value(), put.sequenceNumber());
                case WalRecord.Delete delete -> rebuilt.delete(delete.key(), delete.sequenceNumber());
            }
        }
        this.memTable = rebuilt;
    }

    @Override
    public Optional<byte[]> put(String key, byte[] value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        Optional<byte[]> previous = getInternal(key);
        long seq;
        try {
            seq = wal.appendPut(key, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        memTable.put(key, value, seq);
        maybeFlush();
        return previous;
    }

    @Override
    public Optional<byte[]> get(String key) {
        Objects.requireNonNull(key, "key must not be null");
        return getInternal(key);
    }

    @Override
    public Optional<byte[]> delete(String key) {
        Objects.requireNonNull(key, "key must not be null");
        Optional<byte[]> previous = getInternal(key);
        long seq;
        try {
            seq = wal.appendDelete(key);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        memTable.delete(key, seq);
        maybeFlush();
        return previous;
    }

    private Optional<byte[]> getInternal(String key) {
        Optional<StoredEntry> hit = memTable.get(key);
        if (hit.isPresent()) {
            return toValue(hit.get());
        }
        for (SSTableReader reader : sstables) {
            try {
                hit = reader.get(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (hit.isPresent()) {
                return toValue(hit.get());
            }
        }
        return Optional.empty();
    }

    private static Optional<byte[]> toValue(StoredEntry entry) {
        return switch (entry) {
            case StoredEntry.Value v -> Optional.of(v.bytes());
            case StoredEntry.Tombstone t -> Optional.empty();
        };
    }

    private void maybeFlush() {
        if (memTable.approximateSizeInBytes() >= flushThresholdBytes) {
            try {
                flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Flushes the current MemTable to a new, immutable SSTable, then
     * truncates the WAL. Exposed beyond {@link KeyValueStore} — like
     * {@link DurableKeyValueStore#close()} — so tests can trigger a flush
     * deterministically instead of writing data until the size threshold
     * trips naturally. A no-op if the MemTable is currently empty.
     *
     * <p>Ordering is load-bearing: write temp file, force it, atomically
     * rename it, <em>only then</em> truncate the WAL. See the class Javadoc.
     */
    public void flush() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }
        long watermark = memTable.maxSequenceNumber();
        Path tempFile = dataDirectory.resolve(FLUSH_TEMP_FILE_NAME);
        Path finalFile = dataDirectory.resolve(sstableFileName(watermark));

        SSTableWriter.write(tempFile, finalFile, memTable.entries(), watermark);
        wal.truncateAll();

        sstables.add(0, SSTableReader.open(finalFile));
        memTable = new MemTable();
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            wal.close();
        } catch (IOException e) {
            failure = e;
        }
        for (SSTableReader reader : sstables) {
            try {
                reader.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static String sstableFileName(long maxSequenceNumber) {
        return String.format("sstable-%019d.sst", maxSequenceNumber);
    }

    private static List<SSTableReader> discoverSSTables(Path dataDirectory) throws IOException {
        List<SSTableReader> readers = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDirectory, "sstable-*.sst")) {
            for (Path path : stream) {
                readers.add(SSTableReader.open(path));
            }
        } catch (IOException | RuntimeException e) {
            closeAll(readers, e);
            throw e;
        }
        readers.sort(Comparator.comparingLong(SSTableReader::maxSequenceNumber).reversed());
        return readers;
    }

    private static void closeAll(List<SSTableReader> readers, Exception primary) {
        for (SSTableReader reader : readers) {
            try {
                reader.close();
            } catch (IOException suppressed) {
                primary.addSuppressed(suppressed);
            }
        }
    }
}
