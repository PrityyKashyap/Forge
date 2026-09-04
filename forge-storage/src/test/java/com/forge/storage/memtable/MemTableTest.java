package com.forge.storage.memtable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemTableTest {

    private MemTable memTable;

    @BeforeEach
    void setUp() {
        memTable = new MemTable();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void newMemTableIsEmpty() {
        assertTrue(memTable.isEmpty());
        assertEquals(0, memTable.size());
        assertEquals(0, memTable.approximateSizeInBytes());
        assertEquals(0, memTable.maxSequenceNumber());
    }

    @Test
    void putThenGetReturnsValue() {
        memTable.put("a", bytes("1"), 1);
        StoredEntry.Value value = assertInstanceOf(StoredEntry.Value.class, memTable.get("a").orElseThrow());
        assertArrayEquals(bytes("1"), value.bytes());
    }

    @Test
    void getOnMissingKeyReturnsEmpty() {
        assertTrue(memTable.get("nope").isEmpty());
    }

    @Test
    void deleteInsertsATombstoneRatherThanRemovingTheKey() {
        memTable.put("a", bytes("1"), 1);
        memTable.delete("a", 2);

        Optional<StoredEntry> result = memTable.get("a");
        assertTrue(result.isPresent(), "the key must still be PRESENT in the map, as a tombstone");
        assertInstanceOf(StoredEntry.Tombstone.class, result.get());
    }

    @Test
    void deletingAKeyNeverWrittenStillRecordsATombstone() {
        memTable.delete("never-put", 1);
        assertInstanceOf(StoredEntry.Tombstone.class, memTable.get("never-put").orElseThrow());
    }

    @Test
    void putRejectsNullKeyAndValue() {
        assertThrows(NullPointerException.class, () -> memTable.put(null, bytes("v"), 1));
        assertThrows(NullPointerException.class, () -> memTable.put("k", null, 1));
    }

    @Test
    void deleteRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> memTable.delete(null, 1));
    }

    @Test
    void entriesAreIteratedInAscendingKeyOrder() {
        memTable.put("charlie", bytes("3"), 1);
        memTable.put("alpha", bytes("1"), 2);
        memTable.put("bravo", bytes("2"), 3);

        List<String> keysInOrder = memTable.entries().keySet().stream().toList();
        assertEquals(List.of("alpha", "bravo", "charlie"), keysInOrder);
    }

    @Test
    void maxSequenceNumberTracksTheHighestSeen() {
        memTable.put("a", bytes("1"), 5);
        memTable.delete("b", 12);
        memTable.put("c", bytes("3"), 7);

        assertEquals(12, memTable.maxSequenceNumber());
    }

    @Test
    void approximateSizeGrowsOnNewKeysAndAdjustsOnOverwrite() {
        memTable.put("ab", bytes("xyz"), 1); // key 2 bytes + value 3 bytes = 5
        assertEquals(5, memTable.approximateSizeInBytes());

        memTable.put("ab", bytes("x"), 2); // same key, shorter value: 2 + 1 = 3
        assertEquals(3, memTable.approximateSizeInBytes());
    }

    @Test
    void approximateSizeAccountsForATombstoneAsKeyOnlyNoValueBytes() {
        memTable.put("key", bytes("some-value"), 1); // 3 + 10 = 13
        assertEquals(13, memTable.approximateSizeInBytes());

        memTable.delete("key", 2); // tombstone: key bytes only = 3
        assertEquals(3, memTable.approximateSizeInBytes());
    }

    @Test
    void sizeCountsDistinctKeysIncludingTombstones() {
        memTable.put("a", bytes("1"), 1);
        memTable.delete("b", 2);
        assertEquals(2, memTable.size());
    }

    @Test
    void entriesViewIsUnmodifiable() {
        memTable.put("a", bytes("1"), 1);
        Map<String, StoredEntry> view = memTable.entries();
        assertThrows(UnsupportedOperationException.class, () -> view.put("b", new StoredEntry.Tombstone()));
    }
}
