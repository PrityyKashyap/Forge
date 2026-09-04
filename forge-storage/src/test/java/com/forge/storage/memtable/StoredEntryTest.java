package com.forge.storage.memtable;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StoredEntryTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void valueRejectsNullBytes() {
        assertThrows(NullPointerException.class, () -> new StoredEntry.Value(null));
    }

    @Test
    void valueDefensivelyCopiesBytesOnConstruction() {
        byte[] input = bytes("original");
        StoredEntry.Value value = new StoredEntry.Value(input);

        input[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), value.bytes());
    }

    @Test
    void valueDefensivelyCopiesBytesOnEveryRead() {
        StoredEntry.Value value = new StoredEntry.Value(bytes("original"));

        byte[] firstRead = value.bytes();
        firstRead[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), value.bytes());
    }

    @Test
    void valueEqualsUsesContentBasedComparison() {
        StoredEntry.Value a = new StoredEntry.Value(bytes("v"));
        StoredEntry.Value b = new StoredEntry.Value(bytes("v"));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void valueNotEqualsWhenContentDiffers() {
        StoredEntry.Value a = new StoredEntry.Value(bytes("v1"));
        StoredEntry.Value b = new StoredEntry.Value(bytes("v2"));

        assertNotEquals(a, b);
    }

    @Test
    void tombstonesAreEqual() {
        assertEquals(new StoredEntry.Tombstone(), new StoredEntry.Tombstone());
    }

    @Test
    void valueAndTombstoneAreNeverEqual() {
        assertNotEquals(new StoredEntry.Value(new byte[0]), new StoredEntry.Tombstone());
    }
}
