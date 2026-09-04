package com.forge.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableKeyValueStoreTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // --- normal CRUD, matching the Phase 1 contract -----------------------

    @Test
    void putThenGetReturnsStoredValue(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            store.put("a", bytes("1"));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    @Test
    void putOverwriteReturnsPreviousValue(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            assertTrue(store.put("a", bytes("1")).isEmpty());
            Optional<byte[]> previous = store.put("a", bytes("2"));
            assertArrayEquals(bytes("1"), previous.orElseThrow());
            assertArrayEquals(bytes("2"), store.get("a").orElseThrow());
        }
    }

    @Test
    void deleteExistingKeyRemovesItAndReturnsItsValue(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            store.put("a", bytes("1"));
            assertArrayEquals(bytes("1"), store.delete("a").orElseThrow());
            assertTrue(store.get("a").isEmpty());
        }
    }

    @Test
    void deleteMissingKeyIsNoOpAndReturnsEmpty(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            assertTrue(store.delete("never-existed").isEmpty());
        }
    }

    // --- null rejection, and that a rejected call leaves no WAL trace ------

    @Test
    void putRejectsNullKey(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            assertThrows(NullPointerException.class, () -> store.put(null, bytes("v")));
        }
    }

    @Test
    void putRejectsNullValue(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            assertThrows(NullPointerException.class, () -> store.put("k", null));
        }
    }

    @Test
    void getRejectsNullKey(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            assertThrows(NullPointerException.class, () -> store.get(null));
        }
    }

    @Test
    void deleteRejectsNullKey(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            assertThrows(NullPointerException.class, () -> store.delete(null));
        }
    }

    @Test
    void rejectedNullPutLeavesNoPartialWalRecord(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            assertThrows(NullPointerException.class, () -> store.put(null, bytes("v")));
            assertThrows(NullPointerException.class, () -> store.put("k", null));
            store.put("real", bytes("value"));
        }
        try (DurableKeyValueStore reopened = new DurableKeyValueStore(file)) {
            assertArrayEquals(bytes("value"), reopened.get("real").orElseThrow());
            // if either rejected call had written anything, this would be 2, not 1.
        }
    }

    // --- the core scenario: WAL -> memory -> crash -> recovery -------------

    @Test
    void restartRecoversAllPriorState(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            store.put("a", bytes("1"));
            store.put("b", bytes("2"));
            store.put("c", bytes("3"));
        }
        try (DurableKeyValueStore reopened = new DurableKeyValueStore(file)) {
            assertArrayEquals(bytes("1"), reopened.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), reopened.get("b").orElseThrow());
            assertArrayEquals(bytes("3"), reopened.get("c").orElseThrow());
        }
    }

    @Test
    void multipleRestartCyclesAccumulateCorrectly(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            store.put("a", bytes("1"));
        }
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            store.put("b", bytes("2"));
        }
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            store.put("c", bytes("3"));
        }
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            assertArrayEquals(bytes("3"), store.get("c").orElseThrow());
        }
    }

    @Test
    void overwriteBeforeRestartRecoversTheLastValue(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            store.put("a", bytes("first"));
            store.put("a", bytes("second"));
            store.put("a", bytes("third"));
        }
        try (DurableKeyValueStore reopened = new DurableKeyValueStore(file)) {
            assertArrayEquals(bytes("third"), reopened.get("a").orElseThrow());
        }
    }

    @Test
    void deleteBeforeRestartRecoversAsAbsent(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            store.put("a", bytes("1"));
            store.delete("a");
        }
        try (DurableKeyValueStore reopened = new DurableKeyValueStore(file)) {
            assertTrue(reopened.get("a").isEmpty());
        }
    }

    @Test
    void deletingAKeyThatWasNeverPutRecoversHarmlessly(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            store.delete("never-existed");
            store.put("other", bytes("still works"));
        }
        try (DurableKeyValueStore reopened = new DurableKeyValueStore(file)) {
            assertTrue(reopened.get("never-existed").isEmpty());
            assertArrayEquals(bytes("still works"), reopened.get("other").orElseThrow());
        }
    }

    @Test
    void corruptedWalRecoversExactlyTheValidPrefix(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("wal.log");
        long validPrefixEnd;
        try (DurableKeyValueStore store = new DurableKeyValueStore(file)) {
            store.put("a", bytes("1"));
            validPrefixEnd = Files.size(file);
            store.put("b", bytes("this write's record will be torn"));
        }
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(Files.size(file) - 4); // simulate a crash mid-write on the second record
        }

        try (DurableKeyValueStore reopened = new DurableKeyValueStore(file)) {
            assertArrayEquals(bytes("1"), reopened.get("a").orElseThrow());
            assertTrue(reopened.get("b").isEmpty(), "the torn record must not have been applied");
        }
        assertEquals(validPrefixEnd, Files.size(file));
    }

    // --- defensive-copy semantics must still hold through this class -------

    @Test
    void mutatingInputArrayAfterPutDoesNotAffectStoredValue(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            byte[] input = bytes("original");
            store.put("k", input);
            input[0] = (byte) 'X';
            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }

    @Test
    void mutatingReturnedArrayFromGetDoesNotAffectStoredValue(@TempDir Path dir) throws IOException {
        try (DurableKeyValueStore store = new DurableKeyValueStore(dir.resolve("wal.log"))) {
            store.put("k", bytes("original"));
            byte[] firstRead = store.get("k").orElseThrow();
            firstRead[0] = (byte) 'X';
            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }
}
