package com.forge.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryKeyValueStoreTest {

    private InMemoryKeyValueStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // --- basic contract -----------------------------------------------

    @Test
    void getFromEmptyStoreReturnsEmpty() {
        assertTrue(store.get("missing").isEmpty());
    }

    @Test
    void putThenGetReturnsTheStoredValue() {
        store.put("a", bytes("1"));
        Optional<byte[]> result = store.get("a");
        assertTrue(result.isPresent());
        assertArrayEquals(bytes("1"), result.get());
    }

    @Test
    void putOverwriteReturnsPreviousValueAndUpdatesStoredValue() {
        Optional<byte[]> firstPut = store.put("a", bytes("1"));
        assertTrue(firstPut.isEmpty(), "first PUT for a new key must report no previous value");

        Optional<byte[]> secondPut = store.put("a", bytes("2"));
        assertTrue(secondPut.isPresent());
        assertArrayEquals(bytes("1"), secondPut.get(), "overwrite must return the value it replaced");

        assertArrayEquals(bytes("2"), store.get("a").orElseThrow(), "GET must reflect the new value");
    }

    @Test
    void deleteExistingKeyRemovesItAndReturnsItsValue() {
        store.put("a", bytes("1"));
        Optional<byte[]> removed = store.delete("a");
        assertTrue(removed.isPresent());
        assertArrayEquals(bytes("1"), removed.get());
        assertTrue(store.get("a").isEmpty(), "key must be gone after delete");
    }

    @Test
    void deleteMissingKeyIsNoOpAndReturnsEmpty() {
        Optional<byte[]> removed = store.delete("never-existed");
        assertTrue(removed.isEmpty());
    }

    // --- null rejection -------------------------------------------------

    @Test
    void putRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> store.put(null, bytes("v")));
    }

    @Test
    void putRejectsNullValue() {
        assertThrows(NullPointerException.class, () -> store.put("k", null));
    }

    @Test
    void getRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> store.get(null));
    }

    @Test
    void deleteRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> store.delete(null));
    }

    // --- edge-case but legal inputs --------------------------------------

    @Test
    void emptyStringKeyIsLegal() {
        store.put("", bytes("value-for-empty-key"));
        assertArrayEquals(bytes("value-for-empty-key"), store.get("").orElseThrow());
    }

    @Test
    void emptyByteArrayValueIsLegalAndDistinctFromAbsent() {
        store.put("k", new byte[0]);
        Optional<byte[]> result = store.get("k");
        assertTrue(result.isPresent(), "an empty value is still a present value, not absent");
        assertEquals(0, result.get().length);
    }

    // --- defensive copying (aliasing) ------------------------------------

    @Test
    void mutatingInputArrayAfterPutDoesNotAffectStoredValue() {
        byte[] input = bytes("original");
        store.put("k", input);

        input[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), store.get("k").orElseThrow(),
                "mutating the caller's array after PUT must not change the stored value");
    }

    @Test
    void mutatingReturnedArrayFromGetDoesNotAffectStoredValue() {
        store.put("k", bytes("original"));

        byte[] firstRead = store.get("k").orElseThrow();
        firstRead[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), store.get("k").orElseThrow(),
                "mutating a value returned by GET must not change the stored value");
    }

    // --- multi-key / repeated-operation behavior -------------------------

    @Test
    void multipleKeysAreIndependent() {
        store.put("a", bytes("1"));
        store.put("b", bytes("2"));
        store.put("c", bytes("3"));

        assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
        assertArrayEquals(bytes("3"), store.get("c").orElseThrow());

        store.delete("b");
        assertArrayEquals(bytes("1"), store.get("a").orElseThrow(), "deleting b must not affect a");
        assertTrue(store.get("b").isEmpty());
        assertArrayEquals(bytes("3"), store.get("c").orElseThrow(), "deleting b must not affect c");
    }

    @Test
    void repeatedPutOfSameKeyAndValueIsIdempotent() {
        store.put("k", bytes("v"));
        Optional<byte[]> second = store.put("k", bytes("v"));
        Optional<byte[]> third = store.put("k", bytes("v"));

        assertTrue(second.isPresent());
        assertArrayEquals(bytes("v"), second.get());
        assertTrue(third.isPresent());
        assertArrayEquals(bytes("v"), third.get());
        assertArrayEquals(bytes("v"), store.get("k").orElseThrow());
    }

    @Test
    void repeatedDeleteOfSameKeyIsIdempotent() {
        store.put("k", bytes("v"));

        Optional<byte[]> first = store.delete("k");
        Optional<byte[]> second = store.delete("k");
        Optional<byte[]> third = store.delete("k");

        assertTrue(first.isPresent());
        assertArrayEquals(bytes("v"), first.get());
        assertFalse(second.isPresent(), "second delete of an already-removed key must be a no-op");
        assertFalse(third.isPresent(), "third delete of an already-removed key must be a no-op");
    }
}
