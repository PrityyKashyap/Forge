package com.forge.storage;

import com.forge.storage.memtable.MemTable;
import com.forge.storage.sstable.SSTableWriter;
import com.forge.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LsmKeyValueStoreTest {

    /** Small enough that a handful of writes triggers a real flush deterministically. */
    private static final long TINY_THRESHOLD = 32;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static long countSSTableFiles(Path dir) throws IOException {
        long count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sstable-*.sst")) {
            for (Path ignored : stream) {
                count++;
            }
        }
        return count;
    }

    // --- normal CRUD, matching the KeyValueStore contract -------------------

    @Test
    void putThenGetReturnsStoredValue(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    @Test
    void putOverwriteReturnsPreviousValue(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertTrue(store.put("a", bytes("1")).isEmpty());
            assertArrayEquals(bytes("1"), store.put("a", bytes("2")).orElseThrow());
            assertArrayEquals(bytes("2"), store.get("a").orElseThrow());
        }
    }

    @Test
    void deleteExistingKeyReturnsItsValue(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            assertArrayEquals(bytes("1"), store.delete("a").orElseThrow());
            assertTrue(store.get("a").isEmpty());
        }
    }

    @Test
    void deleteMissingKeyIsNoOpAndReturnsEmpty(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertTrue(store.delete("never-existed").isEmpty());
        }
    }

    @Test
    void putRejectsNullKeyAndValue(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertThrows(NullPointerException.class, () -> store.put(null, bytes("v")));
            assertThrows(NullPointerException.class, () -> store.put("k", null));
        }
    }

    @Test
    void getAndDeleteRejectNullKey(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertThrows(NullPointerException.class, () -> store.get(null));
            assertThrows(NullPointerException.class, () -> store.delete(null));
        }
    }

    @Test
    void constructorRejectsNonPositiveFlushThreshold(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class, () -> new LsmKeyValueStore(dir, 0));
        assertThrows(IllegalArgumentException.class, () -> new LsmKeyValueStore(dir, -1));
    }

    // --- flushing -----------------------------------------------------------

    @Test
    void automaticFlushProducesAnSSTableAndShrinksTheWal(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir, TINY_THRESHOLD)) {
            store.put("aaaaaaaa", bytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")); // well over the threshold
            assertEquals(1, countSSTableFiles(dir));
            assertEquals(0L, Files.size(dir.resolve("forge.wal")));
        }
    }

    @Test
    void explicitFlushWorksEvenBelowThreshold(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) { // default (large) threshold
            store.put("a", bytes("1"));
            store.flush();
            assertEquals(1, countSSTableFiles(dir));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow(), "flushing must not lose the value");
        }
    }

    @Test
    void flushOnAnEmptyMemTableIsANoOp(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.flush();
            assertEquals(0, countSSTableFiles(dir));
        }
    }

    @Test
    void getFindsAValueThatExistsOnlyInAnSSTable(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush(); // "a" is now ONLY in an SSTable; the fresh MemTable knows nothing about it
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    // --- the critical LSM correctness tests ---------------------------------

    @Test
    void tombstoneInMemTableShadowsAnOlderValueInAnSSTable(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush(); // "a" -> "1" is now durable in an SSTable
            store.delete("a"); // tombstone lands in the NEW, post-flush MemTable only

            assertTrue(store.get("a").isEmpty(),
                    "a tombstone in the MemTable must hide the older SSTable value, not be shadowed by it");
        }
    }

    @Test
    void newerMemTableValueShadowsOlderSSTableValueForTheSameKey(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("first"));
            store.flush();
            store.put("a", bytes("second")); // new MemTable, no second flush yet

            assertArrayEquals(bytes("second"), store.get("a").orElseThrow());
        }
    }

    @Test
    void multipleSSTablesAreEachSearchedCorrectly(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
            store.put("b", bytes("2"));
            store.flush();
            store.put("c", bytes("3"));
            store.flush();

            assertEquals(3, countSSTableFiles(dir));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            assertArrayEquals(bytes("3"), store.get("c").orElseThrow());
            assertTrue(store.get("d").isEmpty());
        }
    }

    // --- restart / recovery: the headline scenario --------------------------

    @Test
    void restartRecoversFlushedAndUnflushedDataTogether(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("flushed", bytes("in-an-sstable"));
            store.flush();
            store.put("unflushed", bytes("wal-only"));
        }
        try (LsmKeyValueStore reopened = new LsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("in-an-sstable"), reopened.get("flushed").orElseThrow());
            assertArrayEquals(bytes("wal-only"), reopened.get("unflushed").orElseThrow());
        }
    }

    @Test
    void multipleRestartCyclesInterleavedWithFlushesAccumulateCorrectly(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
        }
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            store.put("b", bytes("2")); // left unflushed on purpose
        }
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            store.delete("a");
            store.flush();
        }
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertTrue(store.get("a").isEmpty());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
        }
    }

    @Test
    void sequenceNumbersContinueAcrossAFlushAndRestart(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
            store.put("b", bytes("2")); // must not reuse sequence number 1
        }
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            // If sequence numbers had reset after the flush, "b" would be
            // indistinguishable from (or older than) the flushed watermark
            // and would be incorrectly skipped during recovery.
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
        }
    }

    /**
     * Directly reproduces crash scenario (c) from the Phase 3 design proof:
     * the SSTable is fully durable (written, forced, atomically renamed) but
     * the WAL truncation that should follow never happened — as if the
     * process crashed in the gap between those two steps. This is done by
     * driving {@link WriteAheadLog} and {@link SSTableWriter} directly,
     * deliberately skipping {@code truncateAll()}, rather than trying to
     * interrupt a real in-progress {@link LsmKeyValueStore#flush()}.
     */
    @Test
    void recoveryToleratesAnUntruncatedWalAlongsideAnAlreadyCompletedSSTable(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        long seq;
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve("forge.wal"))) {
            seq = wal.appendPut("a", bytes("1"));
            memTable.put("a", bytes("1"), seq);
            // Deliberately no wal.truncateAll() here.
        }
        SSTableWriter.write(dir.resolve("flush.tmp"), dir.resolve(String.format("sstable-%019d.sst", seq)),
                memTable.entries(), seq);
        // State now: a fully valid, durable SSTable for "a", AND the WAL
        // still (redundantly) holding the very record that produced it.

        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow(),
                    "a redundant, not-yet-truncated WAL record must not cause duplication or loss");
            store.put("b", bytes("2"));
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
        }
        try (LsmKeyValueStore reopened = new LsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), reopened.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), reopened.get("b").orElseThrow());
        }
    }

    // --- failure injection ---------------------------------------------------

    @Test
    void corruptedSSTableAtStartupIsAFatalError(@TempDir Path dir) throws IOException {
        Path sstableFile;
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sstable-*.sst")) {
            sstableFile = stream.iterator().next();
        }
        try (RandomAccessFile raf = new RandomAccessFile(sstableFile.toFile(), "rw")) {
            raf.seek(0);
            raf.writeInt(0xBADBAD); // corrupt the magic number
        }

        assertThrows(IOException.class, () -> new LsmKeyValueStore(dir));
    }

    @Test
    void orphanedTempFileFromACrashedFlushIsIgnoredAtStartup(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("a", bytes("1")); // durable via WAL; never flushed
        }
        // Simulate a crash that got partway through writing a flush's temp
        // file but never reached the atomic rename.
        Files.write(dir.resolve("flush.tmp"), new byte[]{1, 2, 3, 4, 5});

        try (LsmKeyValueStore reopened = new LsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), reopened.get("a").orElseThrow(),
                    "an orphaned temp file must not prevent recovering WAL-backed data");
            assertEquals(0, countSSTableFiles(dir), "no SSTable was ever actually completed");
        }
    }

    // --- defensive-copy semantics must still hold at this layer -------------

    @Test
    void mutatingInputArrayAfterPutDoesNotAffectStoredValue(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            byte[] input = bytes("original");
            store.put("k", input);
            input[0] = (byte) 'X';
            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }

    @Test
    void mutatingReturnedArrayFromGetDoesNotAffectStoredValue(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("k", bytes("original"));
            byte[] firstRead = store.get("k").orElseThrow();
            firstRead[0] = (byte) 'X';
            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }

    @Test
    void defensiveCopyHoldsForValuesReadBackFromAnSSTable(@TempDir Path dir) throws IOException {
        try (LsmKeyValueStore store = new LsmKeyValueStore(dir)) {
            store.put("k", bytes("original"));
            store.flush();

            byte[] firstRead = store.get("k").orElseThrow();
            firstRead[0] = (byte) 'X';

            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }
}
