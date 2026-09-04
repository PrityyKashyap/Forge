package com.forge.storage.sstable;

import com.forge.storage.memtable.StoredEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.SortedMap;
import java.util.zip.CRC32;

/**
 * Writes a complete, immutable SSTable file from a flushed MemTable's
 * entries: header + framed records to a temp file, {@code force(true)},
 * then an atomic rename into place.
 *
 * <p>Crash safety rests entirely on this ordering: nothing durable changes
 * until the atomic rename succeeds. Any crash before that point leaves only
 * an orphaned, never-visible temp file — see DESIGN.md's Phase 3 crash
 * scenario (b).
 */
public final class SSTableWriter {

    private SSTableWriter() {
    }

    /**
     * @param tempFile   scratch path to write to first; truncated if it already
     *                   exists (an orphan from a previously crashed flush attempt)
     * @param finalFile  the SSTable's permanent path; must not already exist
     * @param entries    the flushed MemTable's contents, already in ascending key order
     * @param maxSequenceNumber the highest WAL sequence number reflected by these entries
     */
    public static void write(Path tempFile, Path finalFile, SortedMap<String, StoredEntry> entries,
            long maxSequenceNumber) throws IOException {
        try (FileChannel channel = FileChannel.open(tempFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            writeHeader(channel, entries.size(), maxSequenceNumber);
            for (Map.Entry<String, StoredEntry> entry : entries.entrySet()) {
                writeRecord(channel, entry.getKey(), entry.getValue());
            }
            channel.force(true);
        }
        Files.move(tempFile, finalFile, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void writeHeader(FileChannel channel, int entryCount, long maxSequenceNumber) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(SSTableFormat.HEADER_LENGTH);
        buf.putInt(SSTableFormat.MAGIC);
        buf.putInt(SSTableFormat.FORMAT_VERSION);
        buf.putLong(maxSequenceNumber);
        buf.putInt(entryCount);

        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, SSTableFormat.HEADER_CHECKSUM_COVERAGE);
        buf.putInt((int) crc.getValue());

        buf.flip();
        writeFully(channel, buf);
    }

    private static void writeRecord(FileChannel channel, String key, StoredEntry entry) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte entryType;
        byte[] valueBytes;
        switch (entry) {
            case StoredEntry.Value v -> {
                entryType = SSTableFormat.ENTRY_TYPE_VALUE;
                valueBytes = v.bytes();
            }
            case StoredEntry.Tombstone t -> {
                entryType = SSTableFormat.ENTRY_TYPE_TOMBSTONE;
                valueBytes = SSTableFormat.EMPTY_VALUE;
            }
        }

        int bodyLength = SSTableFormat.RECORD_FIXED_OVERHEAD + keyBytes.length + valueBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(SSTableFormat.RECORD_LENGTH_PREFIX_BYTES + bodyLength);
        buf.putInt(bodyLength);
        buf.put(entryType);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(valueBytes.length);
        buf.put(valueBytes);

        int contentLength = buf.position() - SSTableFormat.RECORD_LENGTH_PREFIX_BYTES;
        CRC32 crc = new CRC32();
        crc.update(buf.array(), SSTableFormat.RECORD_LENGTH_PREFIX_BYTES, contentLength);
        buf.putInt((int) crc.getValue());

        buf.flip();
        writeFully(channel, buf);
    }

    private static void writeFully(FileChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }
}
