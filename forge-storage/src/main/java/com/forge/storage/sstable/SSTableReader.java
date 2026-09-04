package com.forge.storage.sstable;

import com.forge.storage.bloom.BloomFilter;
import com.forge.storage.bloom.BloomFilterFile;
import com.forge.storage.memtable.StoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

/**
 * Opens an existing, immutable SSTable file for point lookups.
 *
 * <p>The header (magic, version, watermark, entry count, header checksum) is
 * validated eagerly at {@link #open}. Record data is <b>not</b> preloaded
 * into memory: {@link #get} performs a genuine scan of the on-disk file
 * every time, stopping as soon as a key greater than the target is seen
 * (the file is sorted). This is deliberately slower than an in-memory
 * index — preloading would defeat the reason SSTables exist at all (bounding
 * memory below the size of the dataset). See DESIGN.md's Phase 3 notes.
 *
 * <p>Any corruption — a bad header, an impossible length, a checksum
 * mismatch anywhere in the file — is a fatal, thrown {@link IOException}.
 * Unlike the WAL, an SSTable represents data that was already durably
 * flushed and acknowledged; silently skipping or truncating around
 * corruption here would be a silent loss of already-acknowledged data, not
 * a safe discard of something that was never promised. See DESIGN.md's
 * Phase 3 notes on why WAL corruption and SSTable corruption are handled
 * differently.
 *
 * <h2>Phase 13 additions</h2>
 * <p><b>Bloom filter short-circuit</b>: if a sidecar filter loaded at
 * {@link #open} says a key is definitely absent, {@link #get} returns empty
 * without scanning the file at all. See {@link BloomFilter}'s Javadoc for
 * why this can never produce a wrong answer, and {@link BloomFilterFile}'s
 * for why a missing/corrupt sidecar just disables the optimization rather
 * than failing.
 *
 * <p><b>Reference-counted retirement</b>, for compaction: unlike Phase 3-12,
 * where an SSTable file was permanent for the life of the store, a
 * compaction now needs to retire (close and delete) SSTables it has merged
 * away <em>while the store stays live</em> — including while a concurrent
 * {@code get()}/{@code keys()} call may already be mid-scan against one of
 * them, having taken a snapshot of the (about to be superseded) list before
 * releasing {@code stateLock}. {@link #acquire()}/{@link #release()} let a
 * caller hold a reader open for the duration of such a lock-free scan;
 * {@link #retire()} (called by whoever removes this reader from the store's
 * live list, under the write lock) drops the reader's own baseline
 * reference and only actually closes the channel and deletes the file once
 * every acquired reference has also been released — immediately, if no scan
 * was in flight, or whenever the last in-flight scan finishes, otherwise.
 * This is unrelated to {@link #close()}, which remains the plain,
 * unconditional shutdown path used when the whole store is closing (a
 * pre-existing Phase 4 contract that already assumes the caller has
 * quiesced other access first — unchanged by Phase 13, since store shutdown
 * and mid-life compaction retirement are different lifecycles with
 * different concurrency assumptions).
 */
public final class SSTableReader implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SSTableReader.class);

    private final Path path;
    private final FileChannel channel;
    private final long maxSequenceNumber;
    private final long fileSize;
    private final BloomFilter bloomFilter;

    private final AtomicInteger refCount = new AtomicInteger(1);
    private final AtomicBoolean pendingRemoval = new AtomicBoolean(false);

    private SSTableReader(Path path, FileChannel channel, long maxSequenceNumber, long fileSize, BloomFilter bloomFilter) {
        this.path = path;
        this.channel = channel;
        this.maxSequenceNumber = maxSequenceNumber;
        this.fileSize = fileSize;
        this.bloomFilter = bloomFilter;
    }

    public static SSTableReader open(Path file) throws IOException {
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        try {
            long fileSize = channel.size();
            if (fileSize < SSTableFormat.HEADER_LENGTH) {
                throw new IOException("corrupt SSTable (smaller than a header): " + file);
            }

            ByteBuffer header = ByteBuffer.allocate(SSTableFormat.HEADER_LENGTH);
            readFully(channel, header, 0);
            header.flip();

            int magic = header.getInt();
            int version = header.getInt();
            long maxSequenceNumber = header.getLong();
            int entryCount = header.getInt();
            int storedHeaderChecksum = header.getInt();

            if (magic != SSTableFormat.MAGIC) {
                throw new IOException("not a FORGE SSTable (bad magic number): " + file);
            }
            if (version != SSTableFormat.FORMAT_VERSION) {
                throw new IOException("unsupported SSTable format version " + version + ": " + file);
            }
            if (entryCount < 0) {
                throw new IOException("corrupt SSTable header (negative entry count): " + file);
            }
            CRC32 crc = new CRC32();
            crc.update(header.array(), 0, SSTableFormat.HEADER_CHECKSUM_COVERAGE);
            if ((int) crc.getValue() != storedHeaderChecksum) {
                throw new IOException("corrupt SSTable header (checksum mismatch): " + file);
            }

            BloomFilter bloomFilter = BloomFilterFile.tryLoad(file).orElse(null);
            return new SSTableReader(file, channel, maxSequenceNumber, fileSize, bloomFilter);
        } catch (IOException | RuntimeException e) {
            try {
                channel.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    public long maxSequenceNumber() {
        return maxSequenceNumber;
    }

    /** This reader's underlying file path — used by compaction to size/retire it. */
    public Path path() {
        return path;
    }

    /** This file's current on-disk size in bytes, including its header — used to measure compaction's effect on disk usage. */
    public long fileSizeBytes() {
        return fileSize;
    }

    /**
     * Scans the file for {@code key}, stopping early once a greater key is
     * seen — or immediately, without touching the file at all, if this
     * table's Bloom filter says {@code key} is definitely absent.
     */
    public Optional<StoredEntry> get(String key) throws IOException {
        Objects.requireNonNull(key, "key must not be null");
        if (bloomFilter != null && !bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        long pos = SSTableFormat.HEADER_LENGTH;
        while (pos < fileSize) {
            ParsedRecord record = readRecordAt(pos);
            int comparison = record.key().compareTo(key);
            if (comparison == 0) {
                return Optional.of(record.entry());
            }
            if (comparison > 0) {
                return Optional.empty(); // sorted file: every remaining key is even greater
            }
            pos = record.nextPos();
        }
        return Optional.empty();
    }

    /**
     * Every record in the file, key ascending, including tombstones — added
     * in Phase 7 for partition rebalancing, which needs to enumerate a
     * node's actual stored keys rather than only ever looking one up by
     * name. Unlike {@link #get}, this always reads the whole file; there is
     * no early exit since every record is wanted.
     */
    public List<Map.Entry<String, StoredEntry>> scanAll() throws IOException {
        List<Map.Entry<String, StoredEntry>> result = new ArrayList<>();
        long pos = SSTableFormat.HEADER_LENGTH;
        while (pos < fileSize) {
            ParsedRecord record = readRecordAt(pos);
            result.add(Map.entry(record.key(), record.entry()));
            pos = record.nextPos();
        }
        return result;
    }

    private record ParsedRecord(String key, StoredEntry entry, long nextPos) {
    }

    /** Reads and validates exactly one record starting at {@code pos}; never touches the ring buffer or comparisons. */
    private ParsedRecord readRecordAt(long pos) throws IOException {
        long remaining = fileSize - pos;
        if (remaining < SSTableFormat.RECORD_LENGTH_PREFIX_BYTES) {
            throw new IOException("corrupt SSTable (truncated record length prefix)");
        }

        ByteBuffer lengthBuf = ByteBuffer.allocate(SSTableFormat.RECORD_LENGTH_PREFIX_BYTES);
        readFully(channel, lengthBuf, pos);
        int bodyLength = lengthBuf.flip().getInt();

        if (bodyLength < SSTableFormat.MIN_RECORD_BODY_LENGTH || bodyLength > SSTableFormat.MAX_RECORD_BODY_LENGTH) {
            throw new IOException("corrupt SSTable (impossible record length " + bodyLength + ")");
        }

        long bodyStart = pos + SSTableFormat.RECORD_LENGTH_PREFIX_BYTES;
        long remainingAfterPrefix = fileSize - bodyStart;
        if (bodyLength > remainingAfterPrefix) {
            throw new IOException("corrupt SSTable (record length exceeds remaining file size)");
        }

        ByteBuffer body = ByteBuffer.allocate(bodyLength);
        readFully(channel, body, bodyStart);
        body.flip();

        byte entryType = body.get();
        if (entryType != SSTableFormat.ENTRY_TYPE_VALUE && entryType != SSTableFormat.ENTRY_TYPE_TOMBSTONE) {
            throw new IOException("corrupt SSTable (unrecognized entry type " + entryType + ")");
        }

        int keyLength = body.getInt();
        int maxPossibleKeyLength = bodyLength - SSTableFormat.RECORD_FIXED_OVERHEAD;
        if (keyLength < 0 || keyLength > maxPossibleKeyLength) {
            throw new IOException("corrupt SSTable (impossible key length)");
        }
        byte[] keyBytes = new byte[keyLength];
        body.get(keyBytes);

        int valueLength = body.getInt();
        int expectedValueLength = bodyLength - SSTableFormat.RECORD_FIXED_OVERHEAD - keyLength;
        if (valueLength != expectedValueLength) {
            throw new IOException("corrupt SSTable (key/value length mismatch)");
        }
        byte[] valueBytes = new byte[valueLength];
        body.get(valueBytes);

        int contentLength = bodyLength - SSTableFormat.RECORD_CHECKSUM_BYTES;
        CRC32 crc = new CRC32();
        crc.update(body.array(), 0, contentLength);
        int storedChecksum = body.getInt();
        if ((int) crc.getValue() != storedChecksum) {
            throw new IOException("corrupt SSTable (record checksum mismatch)");
        }

        String recordKey = new String(keyBytes, StandardCharsets.UTF_8);
        StoredEntry entry = entryType == SSTableFormat.ENTRY_TYPE_VALUE
                ? new StoredEntry.Value(valueBytes)
                : new StoredEntry.Tombstone();
        return new ParsedRecord(recordKey, entry, bodyStart + bodyLength);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * Holds this reader open for the duration of a lock-free scan taken from
     * a snapshot of the store's live SSTable list. Must only be called while
     * still holding the lock under which that snapshot was read — see this
     * class's Javadoc.
     */
    public void acquire() {
        refCount.incrementAndGet();
    }

    /** Pairs with a prior {@link #acquire()}. */
    public void release() {
        decrementAndMaybeCleanUp();
    }

    /**
     * Drops this reader's own baseline reference (representing "still in the
     * store's live list") — called exactly once, by whoever removes this
     * reader from that list. Closes the channel and deletes the underlying
     * SSTable file (and its Bloom sidecar, if any) once no {@link #acquire()}
     * is still outstanding — immediately, if none is, or when the last one
     * releases, otherwise. Deletion failures are logged, not thrown: a
     * leaked, no-longer-referenced compacted-away file is undesirable but
     * not a correctness problem, unlike failing the compaction that already
     * durably completed by the time this runs.
     */
    public void retire() {
        pendingRemoval.set(true);
        decrementAndMaybeCleanUp();
    }

    private void decrementAndMaybeCleanUp() {
        if (refCount.decrementAndGet() == 0 && pendingRemoval.get()) {
            try {
                channel.close();
            } catch (IOException e) {
                log.warn("failed to close retired SSTable channel: {}", path, e);
            }
            try {
                Files.deleteIfExists(path);
                Files.deleteIfExists(BloomFilterFile.sidecarPathFor(path));
            } catch (IOException e) {
                log.warn("failed to delete retired SSTable file: {}", path, e);
            }
        }
    }

    /** Unlike the WAL's equivalent helper, premature EOF here is always fatal — see the class Javadoc. */
    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long pos = position;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, pos);
            if (n < 0) {
                throw new IOException("corrupt SSTable (unexpected end of file)");
            }
            pos += n;
        }
    }
}
