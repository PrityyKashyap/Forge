package com.forge.storage.sstable;

import com.forge.storage.memtable.StoredEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
 */
public final class SSTableReader implements Closeable {

    private final FileChannel channel;
    private final long maxSequenceNumber;
    private final long fileSize;

    private SSTableReader(FileChannel channel, long maxSequenceNumber, long fileSize) {
        this.channel = channel;
        this.maxSequenceNumber = maxSequenceNumber;
        this.fileSize = fileSize;
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

            return new SSTableReader(channel, maxSequenceNumber, fileSize);
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

    /** Scans the file for {@code key}, stopping early once a greater key is seen. */
    public Optional<StoredEntry> get(String key) throws IOException {
        Objects.requireNonNull(key, "key must not be null");

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
