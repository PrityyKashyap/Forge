package com.forge.storage.sstable;

import com.forge.storage.memtable.MemTable;
import com.forge.storage.memtable.StoredEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Corruption tests here independently re-implement the SSTable format (see
 * {@link #writeRawHeader} / {@link #writeRawRecord}), the same deliberate
 * choice already made for {@code WriteAheadLogTest} — so a bug in the real
 * encoder can't also hide from the test meant to catch it.
 */
class SSTableWriterReaderTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Path tempAndFinal(Path dir, String name) {
        return dir.resolve(name);
    }

    // --- round trip -------------------------------------------------------

    @Test
    void writeThenReadRoundTripsValuesAndTombstones(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        memTable.put("a", bytes("1"), 1);
        memTable.put("b", bytes("2"), 2);
        memTable.delete("c", 3);

        Path temp = dir.resolve("flush.tmp");
        Path finalFile = dir.resolve("sstable-3.sst");
        SSTableWriter.write(temp, finalFile, memTable.entries(), memTable.maxSequenceNumber());

        assertTrue(Files.exists(finalFile));
        assertTrue(Files.notExists(temp), "the temp file must not remain after a successful atomic rename");

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            assertEquals(3L, reader.maxSequenceNumber());

            StoredEntry.Value a = assertInstanceOf(StoredEntry.Value.class, reader.get("a").orElseThrow());
            assertArrayEquals(bytes("1"), a.bytes());

            StoredEntry.Value b = assertInstanceOf(StoredEntry.Value.class, reader.get("b").orElseThrow());
            assertArrayEquals(bytes("2"), b.bytes());

            assertInstanceOf(StoredEntry.Tombstone.class, reader.get("c").orElseThrow());

            assertTrue(reader.get("never-written").isEmpty());
        }
    }

    @Test
    void scanAllReturnsEveryRecordKeyAscendingIncludingTombstones(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        memTable.put("b", bytes("2"), 2);
        memTable.put("a", bytes("1"), 1);
        memTable.delete("c", 3);

        Path finalFile = dir.resolve("sstable-3.sst");
        SSTableWriter.write(dir.resolve("flush.tmp"), finalFile, memTable.entries(), memTable.maxSequenceNumber());

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            var records = reader.scanAll();
            assertEquals(3, records.size());
            assertEquals("a", records.get(0).getKey());
            assertEquals("b", records.get(1).getKey());
            assertEquals("c", records.get(2).getKey());
            assertArrayEquals(bytes("1"), assertInstanceOf(StoredEntry.Value.class, records.get(0).getValue()).bytes());
            assertArrayEquals(bytes("2"), assertInstanceOf(StoredEntry.Value.class, records.get(1).getValue()).bytes());
            assertInstanceOf(StoredEntry.Tombstone.class, records.get(2).getValue());
        }
    }

    @Test
    void scanAllOnAnEmptySSTableReturnsAnEmptyList(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        Path finalFile = dir.resolve("sstable-0.sst");
        SSTableWriter.write(dir.resolve("flush.tmp"), finalFile, memTable.entries(), memTable.maxSequenceNumber());

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            assertTrue(reader.scanAll().isEmpty());
        }
    }

    @Test
    void scanAllDetectsTheSameCorruptionGetWouldHaveHitOnTheWayThere(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        memTable.put("a", bytes("1"), 1);
        memTable.put("z", bytes("2"), 2);

        Path finalFile = dir.resolve("sstable-2.sst");
        SSTableWriter.write(dir.resolve("flush.tmp"), finalFile, memTable.entries(), memTable.maxSequenceNumber());

        // Flip a byte inside the first record's body — scanAll must still surface the
        // corruption (via the checksum check), the same as a get() for a later key would.
        flipByte(finalFile, SSTableFormat.HEADER_LENGTH + SSTableFormat.RECORD_LENGTH_PREFIX_BYTES + 1);

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            assertThrows(IOException.class, reader::scanAll);
        }
    }

    @Test
    void emptyMemTableProducesAValidEmptySSTable(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        Path temp = dir.resolve("flush.tmp");
        Path finalFile = dir.resolve("sstable-0.sst");
        SSTableWriter.write(temp, finalFile, memTable.entries(), memTable.maxSequenceNumber());

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            assertEquals(0L, reader.maxSequenceNumber());
            assertTrue(reader.get("anything").isEmpty());
        }
    }

    @Test
    void emptyKeyAndEmptyValueRoundTrip(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        memTable.put("", new byte[0], 1);

        Path finalFile = dir.resolve("sstable-1.sst");
        SSTableWriter.write(dir.resolve("flush.tmp"), finalFile, memTable.entries(), memTable.maxSequenceNumber());

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            StoredEntry.Value v = assertInstanceOf(StoredEntry.Value.class, reader.get("").orElseThrow());
            assertEquals(0, v.bytes().length);
        }
    }

    @Test
    void arbitraryNonUtf8ValueRoundTripsExactly(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        byte[] binary = {(byte) 0xFF, (byte) 0xFE, 0x00, (byte) 0x80, 0x01};
        memTable.put("k", binary, 1);

        Path finalFile = dir.resolve("sstable-1.sst");
        SSTableWriter.write(dir.resolve("flush.tmp"), finalFile, memTable.entries(), memTable.maxSequenceNumber());

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            StoredEntry.Value v = assertInstanceOf(StoredEntry.Value.class, reader.get("k").orElseThrow());
            assertArrayEquals(binary, v.bytes());
        }
    }

    @Test
    void manyKeysAreFoundCorrectlyRegardlessOfPosition(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        String[] keys = {"alpha", "bravo", "charlie", "delta", "echo"};
        for (int i = 0; i < keys.length; i++) {
            memTable.put(keys[i], bytes(String.valueOf(i)), i + 1);
        }
        Path finalFile = dir.resolve("sstable-5.sst");
        SSTableWriter.write(dir.resolve("flush.tmp"), finalFile, memTable.entries(), memTable.maxSequenceNumber());

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            for (int i = 0; i < keys.length; i++) {
                StoredEntry.Value v = assertInstanceOf(StoredEntry.Value.class, reader.get(keys[i]).orElseThrow());
                assertArrayEquals(bytes(String.valueOf(i)), v.bytes());
            }
            assertTrue(reader.get("zulu").isEmpty(), "a key sorted after everything must return empty, not throw");
            assertTrue(reader.get("aaa").isEmpty(), "a key sorted before everything must return empty");
        }
    }

    @Test
    void tempFileFromAPriorCrashedAttemptIsOverwrittenNotAppendedTo(@TempDir Path dir) throws IOException {
        Path temp = dir.resolve("flush.tmp");
        Files.write(temp, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); // orphaned garbage from a "previous crash"

        MemTable memTable = new MemTable();
        memTable.put("a", bytes("1"), 1);
        Path finalFile = dir.resolve("sstable-1.sst");
        SSTableWriter.write(temp, finalFile, memTable.entries(), memTable.maxSequenceNumber());

        try (SSTableReader reader = SSTableReader.open(finalFile)) {
            assertArrayEquals(bytes("1"), ((StoredEntry.Value) reader.get("a").orElseThrow()).bytes());
        }
    }

    // --- corruption handling: fail loud, never silently skip --------------

    @Test
    void badMagicNumberIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeInt(0xDEADBEEF); // wrong magic
            raf.writeInt(SSTableFormat.FORMAT_VERSION);
            raf.writeLong(1L);
            raf.writeInt(0);
            raf.writeInt(0); // bogus header checksum too, doesn't matter, magic fails first
        }
        assertThrows(IOException.class, () -> SSTableReader.open(file));
    }

    @Test
    void unsupportedVersionIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        writeRawHeader(file, SSTableFormat.MAGIC, 99, 1L, 0);
        assertThrows(IOException.class, () -> SSTableReader.open(file));
    }

    @Test
    void corruptHeaderChecksumIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeInt(SSTableFormat.MAGIC);
            raf.writeInt(SSTableFormat.FORMAT_VERSION);
            raf.writeLong(1L);
            raf.writeInt(0);
            raf.writeInt(0xBADBAD); // wrong header checksum
        }
        assertThrows(IOException.class, () -> SSTableReader.open(file));
    }

    @Test
    void fileSmallerThanHeaderIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        Files.write(file, new byte[]{1, 2, 3});
        assertThrows(IOException.class, () -> SSTableReader.open(file));
    }

    @Test
    void corruptRecordChecksumIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        writeRawHeader(file, SSTableFormat.MAGIC, SSTableFormat.FORMAT_VERSION, 1L, 1);
        long recordOffset = writeRawRecord(file, SSTableFormat.ENTRY_TYPE_VALUE, bytes("a"), bytes("1"), null);
        flipByte(file, recordOffset + 5); // inside the record body

        try (SSTableReader reader = SSTableReader.open(file)) {
            assertThrows(IOException.class, () -> reader.get("a"));
        }
    }

    @Test
    void negativeRecordLengthIsRejectedWithoutHugeAllocation(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        writeRawHeader(file, SSTableFormat.MAGIC, SSTableFormat.FORMAT_VERSION, 1L, 1);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(-1);
        }
        try (SSTableReader reader = SSTableReader.open(file)) {
            assertThrows(IOException.class, () -> reader.get("a"));
        }
    }

    @Test
    void recordLengthExceedingRemainingFileIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        writeRawHeader(file, SSTableFormat.MAGIC, SSTableFormat.FORMAT_VERSION, 1L, 1);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(10_000_000); // well under the sanity ceiling, but the file has nowhere near this many bytes
            raf.write(new byte[]{1, 2, 3});
        }
        try (SSTableReader reader = SSTableReader.open(file)) {
            assertThrows(IOException.class, () -> reader.get("a"));
        }
    }

    @Test
    void keyLengthThatCannotFitIsRejectedWithoutHugeAllocation(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bad.sst");
        writeRawHeader(file, SSTableFormat.MAGIC, SSTableFormat.FORMAT_VERSION, 1L, 1);
        writeRawRecord(file, SSTableFormat.ENTRY_TYPE_VALUE, bytes("a"), bytes("1"), 50_000_000);

        try (SSTableReader reader = SSTableReader.open(file)) {
            assertThrows(IOException.class, () -> reader.get("a"));
        }
    }

    // --- independent, deliberately-permissive raw encoding for tests ------

    private static void writeRawHeader(Path file, int magic, int version, long maxSeq, int entryCount)
            throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        out.writeInt(magic);
        out.writeInt(version);
        out.writeLong(maxSeq);
        out.writeInt(entryCount);
        out.flush();
        byte[] content = buf.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(content);

        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.write(content);
            raf.writeInt((int) crc.getValue());
        }
    }

    /** @return the file offset where this record's length prefix starts */
    private static long writeRawRecord(Path file, byte entryType, byte[] key, byte[] value, Integer bogusKeyLength)
            throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        out.writeByte(entryType);
        out.writeInt(bogusKeyLength != null ? bogusKeyLength : key.length);
        out.write(key);
        out.writeInt(value.length);
        out.write(value);
        out.flush();
        byte[] content = buf.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(content);

        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            long offset = raf.length();
            raf.seek(offset);
            raf.writeInt(content.length + 4); // bodyLength includes the trailing checksum
            raf.write(content);
            raf.writeInt((int) crc.getValue());
            return offset;
        }
    }

    private static void flipByte(Path file, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 0xFF);
        }
    }
}
