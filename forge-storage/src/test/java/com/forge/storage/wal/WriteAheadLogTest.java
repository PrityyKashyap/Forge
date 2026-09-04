package com.forge.storage.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These tests independently re-implement the WAL's on-disk format (see
 * {@link #writeValidRecord}, {@link #writeRecordWithBadKeyLength}) rather than
 * reusing {@link WriteAheadLog}'s own encoding, so a bug in the real encoder
 * can't also hide from the test that's supposed to catch it.
 */
class WriteAheadLogTest {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;

    /** The declared length-prefix must cover the trailing checksum too, per WriteAheadLog's format. */
    private static final int CHECKSUM_BYTES = 4;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // --- round trips, ordering, sequence numbers -----------------------

    @Test
    void putRoundTrip(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            long seq = wal.appendPut("k", bytes("v"));
            assertEquals(1L, seq);
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(1, records.size());
            WalRecord.Put put = assertInstanceOf(WalRecord.Put.class, records.get(0));
            assertEquals(1L, put.sequenceNumber());
            assertEquals("k", put.key());
            assertArrayEquals(bytes("v"), put.value());
        }
    }

    @Test
    void deleteRoundTrip(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendDelete("k");
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(1, records.size());
            WalRecord.Delete delete = assertInstanceOf(WalRecord.Delete.class, records.get(0));
            assertEquals(1L, delete.sequenceNumber());
            assertEquals("k", delete.key());
        }
    }

    @Test
    void orderingIsPreserved(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            wal.appendDelete("a");
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(3, records.size());
            assertEquals("a", records.get(0).key());
            assertEquals("b", records.get(1).key());
            assertEquals("a", records.get(2).key());
            assertInstanceOf(WalRecord.Delete.class, records.get(2));
        }
    }

    @Test
    void sequenceNumbersIncreaseMonotonically(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertEquals(1L, wal.appendPut("a", bytes("1")));
            assertEquals(2L, wal.appendPut("b", bytes("2")));
            assertEquals(3L, wal.appendDelete("a"));
            assertEquals(4L, wal.appendPut("c", bytes("3")));
            assertEquals(5L, wal.appendPut("d", bytes("4")));
        }
    }

    @Test
    void sequenceNumbersContinueAcrossRestart(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            wal.appendPut("c", bytes("3"));
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertEquals(4L, reopened.appendPut("d", bytes("4")));
            assertEquals(5L, reopened.appendPut("e", bytes("5")));
        }
        try (WriteAheadLog finalOpen = WriteAheadLog.open(file)) {
            List<WalRecord> records = finalOpen.recoveredRecords();
            assertEquals(5, records.size());
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1L, records.get(i).sequenceNumber());
            }
        }
    }

    // --- truncateAll() (Phase 3: safe truncation after a flush) -----------

    @Test
    void truncateAllEmptiesTheFile(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            assertTrue(Files.size(file) > 0);

            wal.truncateAll();

            assertEquals(0L, Files.size(file));
        }
    }

    @Test
    void truncateAllPreservesTheSequenceNumberSequence(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            assertEquals(3L, wal.appendPut("c", bytes("3")));

            wal.truncateAll();

            assertEquals(4L, wal.appendPut("d", bytes("4")),
                    "the next sequence number must continue from before the truncation, not reset to 1");
        }
    }

    @Test
    void appendsAfterTruncateAllAreCorrectlyRecoveredOnReopen(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            wal.truncateAll();
            wal.appendPut("c", bytes("3"));
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(1, records.size(), "only the post-truncation record should remain");
            assertEquals("c", records.get(0).key());
            assertEquals(3L, records.get(0).sequenceNumber());
        }
    }

    @Test
    void truncateAllOnAnAlreadyEmptyWalIsHarmless(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.truncateAll();
            assertEquals(1L, wal.appendPut("a", bytes("1")));
        }
    }

    /**
     * This is the exact bug the crash-safety review caught: truncateAll()
     * only preserves the sequence counter for the lifetime of the instance
     * that called it. A file it emptied carries no durable memory of that
     * once closed — reopening it looks, from the WAL's own narrow point of
     * view, identical to a WAL that was never used. Without
     * ensureNextSequenceNumberAtLeast, this reopened WAL would hand out
     * sequence number 1 again, colliding with a sequence number a flushed
     * SSTable may have already claimed.
     */
    @Test
    void closingAndReopeningAfterTruncateAllResetsToOneWithoutInterventionFromTheCaller(@TempDir Path dir)
            throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.truncateAll();
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertEquals(1L, reopened.appendPut("b", bytes("2")),
                    "demonstrates the gap: without correction, the reopened WAL has no idea "
                            + "sequence number 1 was already used and durably flushed elsewhere");
        }
    }

    @Test
    void ensureNextSequenceNumberAtLeastRaisesTheCounterWhenNeeded(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.truncateAll();
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            reopened.ensureNextSequenceNumberAtLeast(2); // simulates LsmKeyValueStore supplying the SSTable watermark + 1
            assertEquals(2L, reopened.appendPut("b", bytes("2")),
                    "the caller-supplied watermark must close the exact gap demonstrated above");
        }
    }

    @Test
    void ensureNextSequenceNumberAtLeastNeverLowersTheCounter(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            wal.appendPut("c", bytes("3")); // next would naturally be 4

            wal.ensureNextSequenceNumberAtLeast(1); // lower than current; must be a no-op

            assertEquals(4L, wal.appendPut("d", bytes("4")));
        }
    }

    // --- truncateUpTo() (Phase 4: watermark-scoped truncation) ------------

    @Test
    void truncateUpToDiscardsAtOrBelowWatermarkKeepsNewer(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1")); // seq 1
            wal.appendPut("b", bytes("2")); // seq 2
            wal.appendPut("c", bytes("3")); // seq 3
            wal.appendPut("d", bytes("4")); // seq 4

            wal.truncateUpTo(2);
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(2, records.size(), "only seq 3 and 4 should survive a truncateUpTo(2)");
            assertEquals("c", records.get(0).key());
            assertEquals(3L, records.get(0).sequenceNumber());
            assertEquals("d", records.get(1).key());
            assertEquals(4L, records.get(1).sequenceNumber());
        }
    }

    @Test
    void truncateUpToWatermarkCoveringEverythingEmptiesTheFile(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            wal.truncateUpTo(2);
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertTrue(reopened.recoveredRecords().isEmpty());
        }
    }

    @Test
    void truncateUpToWatermarkBelowEverythingKeepsAllRecords(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            wal.truncateUpTo(0);
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertEquals(2, reopened.recoveredRecords().size());
        }
    }

    @Test
    void truncateUpToPreservesSequenceContinuationWithinTheSameInstance(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1")); // seq 1
            wal.appendPut("b", bytes("2")); // seq 2
            wal.truncateUpTo(1);
            assertEquals(3L, wal.appendPut("c", bytes("3")),
                    "truncateUpTo must not disturb nextSequenceNumber");
        }
    }

    @Test
    void truncateUpToDoesNotLeaveATempFileBehindOnSuccess(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            wal.appendPut("b", bytes("2"));
            wal.truncateUpTo(1);
        }
        assertTrue(Files.notExists(dir.resolve("wal.log.rewrite.tmp")));
    }

    @Test
    void appendAfterTruncateUpToIsCorrectlyRecoveredOnReopen(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1")); // seq 1
            wal.appendPut("b", bytes("2")); // seq 2
            wal.truncateUpTo(1);
            wal.appendPut("c", bytes("3")); // seq 3, appended after the rewrite+reopen
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(2, records.size());
            assertEquals("b", records.get(0).key());
            assertEquals(2L, records.get(0).sequenceNumber());
            assertEquals("c", records.get(1).key());
            assertEquals(3L, records.get(1).sequenceNumber());
        }
    }

    // --- concurrent writers (Phase 4) --------------------------------------

    /**
     * Many threads append concurrently to one WriteAheadLog. Verifies the
     * exact guarantees Phase 4's design depends on: every sequence number is
     * unique, they form a contiguous range (no gaps from a lost increment),
     * and — critically — the on-disk order recovered after reopening matches
     * sequence order exactly, proving allocation and physical append never
     * became separated under contention (the specific failure mode proven
     * possible if they used independent synchronization).
     */
    @Test
    void concurrentAppendsProduceUniqueStrictlyIncreasingSequenceNumbers(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("wal.log");
        int threadCount = 16;
        int perThread = 50;

        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            List<Long> allSequenceNumbers = Collections.synchronizedList(new ArrayList<>());
            List<Thread> threads = new ArrayList<>();
            ConcurrentLinkedQueue<Exception> failures = new ConcurrentLinkedQueue<>();

            for (int t = 0; t < threadCount; t++) {
                int threadIndex = t;
                Thread thread = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < perThread; i++) {
                            long seq = wal.appendPut("t" + threadIndex + "-" + i, bytes("v"));
                            allSequenceNumbers.add(seq);
                        }
                    } catch (Exception e) {
                        failures.add(e);
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }

            assertTrue(failures.isEmpty(), "no thread should have thrown: " + failures);

            int total = threadCount * perThread;
            assertEquals(total, new HashSet<>(allSequenceNumbers).size(),
                    "every sequence number returned must be unique");
            List<Long> sorted = new ArrayList<>(allSequenceNumbers);
            sorted.sort(Long::compareTo);
            for (int i = 0; i < total; i++) {
                assertEquals(i + 1L, sorted.get(i), "sequence numbers must form a contiguous range starting at 1");
            }
        }

        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(threadCount * perThread, records.size(),
                    "recovery must find every concurrently-appended record, none corrupted or torn");
            for (int i = 0; i < records.size() - 1; i++) {
                assertTrue(records.get(i).sequenceNumber() < records.get(i + 1).sequenceNumber(),
                        "on-disk order must exactly match sequence order, proving allocation and append "
                                + "never separated under contention");
            }
        }
    }

    // --- empty / nonexistent WAL -----------------------------------------

    @Test
    void emptyWalHasNoRecoveredRecordsAndStartsAtSequenceOne(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("empty.log");
        Files.createFile(file);
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
            assertEquals(1L, wal.appendPut("k", bytes("v")));
        }
    }

    @Test
    void nonexistentWalIsCreatedAndUsable(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("does-not-exist-yet.log");
        assertTrue(Files.notExists(file));
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
            assertEquals(1L, wal.appendPut("k", bytes("v")));
        }
        assertTrue(Files.exists(file));
    }

    // --- edge-case but legal content --------------------------------------

    @Test
    void emptyKeyRoundTrips(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("", bytes("value-for-empty-key"));
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            WalRecord.Put put = assertInstanceOf(WalRecord.Put.class, reopened.recoveredRecords().get(0));
            assertEquals("", put.key());
            assertArrayEquals(bytes("value-for-empty-key"), put.value());
        }
    }

    @Test
    void emptyValuePutRemainsDistinctFromDelete(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("k", new byte[0]);
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(1, records.size());
            WalRecord.Put put = assertInstanceOf(WalRecord.Put.class, records.get(0),
                    "an empty-value PUT must never be recovered as a DELETE");
            assertEquals(0, put.value().length);
        }
    }

    @Test
    void arbitraryNonUtf8ValueRoundTripsExactly(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        byte[] binary = {(byte) 0xFF, (byte) 0xFE, 0x00, (byte) 0x80, 0x01, (byte) 0xC0};
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("k", binary);
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            WalRecord.Put put = assertInstanceOf(WalRecord.Put.class, reopened.recoveredRecords().get(0));
            assertArrayEquals(binary, put.value());
        }
    }

    // --- corruption: checksum, torn, truncation, append-after ------------

    @Test
    void byteLevelCorruptionStopsBeforeTheCorruptedRecordAndTruncates(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        long firstRecordEnd;
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            writeValidRecord(raf, 1, OP_PUT, bytes("a"), bytes("1"));
            firstRecordEnd = raf.length();
            long secondRecordValueOffset = recordValueOffset(firstRecordEnd, bytes("b"));
            writeValidRecord(raf, 2, OP_PUT, bytes("b"), bytes("2"));
            flipByte(raf, secondRecordValueOffset);
        }

        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            List<WalRecord> records = wal.recoveredRecords();
            assertEquals(1, records.size(), "only the record before the corruption should survive");
            assertEquals("a", records.get(0).key());
        }
        assertEquals(firstRecordEnd, Files.size(file), "file must be physically truncated to the last valid record");
    }

    @Test
    void corruptedFirstRecordYieldsNoRecordsAndTruncatesToZero(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            writeValidRecord(raf, 1, OP_PUT, bytes("a"), bytes("1"));
            flipByte(raf, recordValueOffset(0, bytes("a")));
        }

        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
        }
        assertEquals(0L, Files.size(file));
    }

    @Test
    void tornRecordIsExcludedAndFileIsTruncated(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        long firstRecordEnd;
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            firstRecordEnd = Files.size(file);
            wal.appendPut("b", bytes("this record will be chopped off"));
        }
        long fullLength = Files.size(file);
        assertTrue(fullLength > firstRecordEnd);

        // Simulate a crash mid-write: cut off the last few bytes of the second record.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(fullLength - 5);
        }

        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(1, records.size());
            assertEquals("a", records.get(0).key());
        }
        assertEquals(firstRecordEnd, Files.size(file));
    }

    @Test
    void appendAfterTruncationContinuesCorrectly(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        long firstRecordEnd;
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("a", bytes("1"));
            firstRecordEnd = Files.size(file);
            wal.appendPut("b", bytes("gets chopped"));
        }
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(Files.size(file) - 3);
        }

        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertEquals(1, wal.recoveredRecords().size());
            long newSeq = wal.appendPut("c", bytes("new"));
            assertEquals(2L, newSeq, "the discarded torn record's slot is reused, not skipped");
        }

        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(2, records.size());
            assertEquals("a", records.get(0).key());
            assertEquals("c", records.get(1).key());
        }
        assertTrue(Files.size(file) > firstRecordEnd);
    }

    // --- malformed length fields: rejected safely, no huge allocation -----

    @Test
    void negativeDeclaredLengthIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeInt(-1);
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
        }
        assertEquals(0L, Files.size(file));
    }

    @Test
    void impossiblySmallDeclaredLengthIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeInt(5); // below the fixed 21-byte minimum overhead
            raf.write(new byte[5]);
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
        }
        assertEquals(0L, Files.size(file));
    }

    @Test
    void keyLengthThatCannotFitInsideRecordLengthIsRejectedWithoutHugeAllocation(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            // bodyLength is correct for the bytes actually present, but the embedded
            // keyLength field lies and claims far more bytes than fit.
            writeRecordWithBadKeyLength(raf, 1, OP_PUT, bytes("a"), bytes("1"), 50_000_000);
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty(),
                    "a key length that cannot fit inside the declared record length must be rejected");
        }
        assertEquals(0L, Files.size(file));
    }

    @Test
    void declaredLengthExceedingRemainingFileIsRejectedWithoutOverAllocating(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeInt(10_000_000); // well under the 64 MiB sanity cap, but the file has nowhere near this many bytes
            raf.write(new byte[]{1, 2, 3, 4, 5});
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
        }
        assertEquals(0L, Files.size(file));
    }

    @Test
    void absurdlyLargeDeclaredLengthIsRejectedByTheSanityCeiling(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.writeInt(Integer.MAX_VALUE - 10); // exceeds the 64 MiB per-record ceiling outright
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
        }
        assertEquals(0L, Files.size(file));
    }

    @Test
    void outOfOrderSequenceNumberIsRejectedEvenIfOtherwiseValid(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        long firstRecordEnd;
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            writeValidRecord(raf, 5, OP_PUT, bytes("a"), bytes("1"));
            firstRecordEnd = raf.length();
            // A second, individually well-formed and checksum-valid record, but
            // its sequence number does not exceed the previous one.
            writeValidRecord(raf, 5, OP_PUT, bytes("b"), bytes("2"));
        }

        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            List<WalRecord> records = wal.recoveredRecords();
            assertEquals(1, records.size());
            assertEquals("a", records.get(0).key());
        }
        assertEquals(firstRecordEnd, Files.size(file));
    }

    @Test
    void checksumMismatchWithOtherwiseValidFramingIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            writeValidRecord(raf, 1, OP_PUT, bytes("a"), bytes("1"));
            // Flip a byte inside the value, leaving every length field self-consistent.
            flipByte(raf, recordValueOffset(0, bytes("a")));
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertTrue(wal.recoveredRecords().isEmpty());
        }
        assertEquals(0L, Files.size(file));
    }

    // --- appendReplicated / nextSequenceNumber (Phase 9) -----------------

    @Test
    void nextSequenceNumberStartsAtOneForAFreshLog(@TempDir Path dir) throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve("wal.log"))) {
            assertEquals(1L, wal.nextSequenceNumber());
        }
    }

    @Test
    void nextSequenceNumberAdvancesAfterAnOrdinaryAppend(@TempDir Path dir) throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve("wal.log"))) {
            wal.appendPut("a", bytes("1"));
            assertEquals(2L, wal.nextSequenceNumber());
        }
    }

    @Test
    void appendReplicatedAppliesARecordAtExactlyTheExpectedSequenceNumber(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            ReplicationOutcome outcome = wal.appendReplicated(new WalRecord.Put(1, "a", bytes("1")));
            assertEquals(ReplicationOutcome.APPLIED, outcome);
            assertEquals(2L, wal.nextSequenceNumber());
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(1, records.size());
            WalRecord.Put put = assertInstanceOf(WalRecord.Put.class, records.get(0));
            assertEquals(1L, put.sequenceNumber());
            assertArrayEquals(bytes("1"), put.value());
        }
    }

    @Test
    void appendReplicatedAppliesADeleteRecordCorrectly(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Delete(1, "a")));
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            List<WalRecord> records = reopened.recoveredRecords();
            assertEquals(1, records.size());
            WalRecord.Delete delete = assertInstanceOf(WalRecord.Delete.class, records.get(0));
            assertEquals(1L, delete.sequenceNumber());
            assertEquals("a", delete.key());
        }
    }

    @Test
    void appendReplicatedAppliesASequenceOfRecordsInOrder(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Put(1, "a", bytes("1"))));
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Put(2, "b", bytes("2"))));
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Delete(3, "a")));
            assertEquals(4L, wal.nextSequenceNumber());
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertEquals(3, reopened.recoveredRecords().size());
        }
    }

    @Test
    void appendReplicatedIsIdempotentForAnAlreadyAppliedSequenceNumber(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Put(1, "a", bytes("1"))));
            // The exact same record, offered again -- as a leader retrying after an ack timeout might do.
            ReplicationOutcome outcome = wal.appendReplicated(new WalRecord.Put(1, "a", bytes("1")));
            assertEquals(ReplicationOutcome.ALREADY_APPLIED, outcome);
            assertEquals(2L, wal.nextSequenceNumber(), "a duplicate must not advance the sequence counter again");
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertEquals(1, reopened.recoveredRecords().size(), "the duplicate must not have been written to disk at all");
        }
    }

    @Test
    void appendReplicatedIsIdempotentEvenIfTheDuplicateCarriesADifferentValue(@TempDir Path dir) throws IOException {
        // Proves the idempotency check is purely about the sequence number, not content
        // equality -- a stale resend must never be re-applied regardless of what it contains.
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve("wal.log"))) {
            wal.appendReplicated(new WalRecord.Put(1, "a", bytes("original")));
            ReplicationOutcome outcome = wal.appendReplicated(new WalRecord.Put(1, "a", bytes("different")));
            assertEquals(ReplicationOutcome.ALREADY_APPLIED, outcome);
        }
    }

    @Test
    void appendReplicatedRefusesToCreateAGap(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            // Sequence number 1 is missing; offering 2 first must be refused, not accepted with a hole.
            ReplicationOutcome outcome = wal.appendReplicated(new WalRecord.Put(2, "b", bytes("2")));
            assertEquals(ReplicationOutcome.GAP_DETECTED, outcome);
            assertEquals(1L, wal.nextSequenceNumber(), "a refused gap must not advance the sequence counter");
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertTrue(reopened.recoveredRecords().isEmpty(), "nothing should have been written for a refused gap");
        }
    }

    @Test
    void appendReplicatedAcceptsTheCorrectRecordAfterAPreviousGapWasRefused(@TempDir Path dir) throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve("wal.log"))) {
            assertEquals(ReplicationOutcome.GAP_DETECTED, wal.appendReplicated(new WalRecord.Put(2, "b", bytes("2"))));
            // Catch-up: the correct next record (seq 1) arrives.
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Put(1, "a", bytes("1"))));
            assertEquals(2L, wal.nextSequenceNumber());
            // Now seq 2 is exactly expected and succeeds.
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Put(2, "b", bytes("2"))));
            assertEquals(3L, wal.nextSequenceNumber());
        }
    }

    @Test
    void appendReplicatedInterleavesCorrectlyWithOrdinaryAppendsToTheSameSequenceSpace(@TempDir Path dir)
            throws IOException {
        // Simulates a store recovering local writes made before replication started,
        // then switching to applying a leader's stream from exactly where it left off.
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog wal = WriteAheadLog.open(file)) {
            wal.appendPut("local-a", bytes("1")); // ordinary append: assigns seq 1
            assertEquals(2L, wal.nextSequenceNumber());
            assertEquals(ReplicationOutcome.APPLIED, wal.appendReplicated(new WalRecord.Put(2, "replicated-b", bytes("2"))));
            assertEquals(3L, wal.nextSequenceNumber());
        }
        try (WriteAheadLog reopened = WriteAheadLog.open(file)) {
            assertEquals(2, reopened.recoveredRecords().size());
        }
    }

    // --- independent, deliberately-permissive raw record encoding for tests ---

    private static void writeValidRecord(RandomAccessFile raf, long seq, byte opType, byte[] key, byte[] value)
            throws IOException {
        byte[] content = recordContent(seq, opType, key.length, key, value.length, value);
        CRC32 crc = new CRC32();
        crc.update(content);
        int checksum = (int) crc.getValue();

        raf.seek(raf.length());
        raf.writeInt(content.length + CHECKSUM_BYTES); // bodyLength includes the trailing checksum
        raf.write(content);
        raf.writeInt(checksum);
    }

    /** Writes a record whose bodyLength/physical bytes are correct, but whose keyLength field lies. */
    private static void writeRecordWithBadKeyLength(RandomAccessFile raf, long seq, byte opType,
            byte[] key, byte[] value, int bogusKeyLength) throws IOException {
        byte[] content = recordContent(seq, opType, bogusKeyLength, key, value.length, value);
        CRC32 crc = new CRC32();
        crc.update(content);
        int checksum = (int) crc.getValue();

        raf.seek(raf.length());
        raf.writeInt(content.length + CHECKSUM_BYTES); // bodyLength includes the trailing checksum
        raf.write(content);
        raf.writeInt(checksum);
    }

    private static byte[] recordContent(long seq, byte opType, int keyLengthField, byte[] keyBytes,
            int valueLengthField, byte[] valueBytes) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        out.writeLong(seq);
        out.writeByte(opType);
        out.writeInt(keyLengthField);
        out.write(keyBytes);
        out.writeInt(valueLengthField);
        out.write(valueBytes);
        out.flush();
        return buf.toByteArray();
    }

    /** Absolute file offset of the first byte of a record's value, given where the record starts. */
    private static long recordValueOffset(long recordStart, byte[] key) {
        return recordStart + 4 /* length prefix */ + 8 /* seq */ + 1 /* opType */
                + 4 /* keyLength */ + key.length + 4 /* valueLength */;
    }

    private static void flipByte(RandomAccessFile raf, long offset) throws IOException {
        raf.seek(offset);
        int b = raf.read();
        raf.seek(offset);
        raf.write(b ^ 0xFF);
    }
}
