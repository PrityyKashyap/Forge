package com.forge.storage.wal;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WalRecordTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> new WalRecord.Put(1, null, bytes("v")));
    }

    @Test
    void putRejectsNullValue() {
        assertThrows(NullPointerException.class, () -> new WalRecord.Put(1, "k", null));
    }

    @Test
    void deleteRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> new WalRecord.Delete(1, null));
    }

    @Test
    void putDefensivelyCopiesValueOnConstruction() {
        byte[] input = bytes("original");
        WalRecord.Put put = new WalRecord.Put(1, "k", input);

        input[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), put.value(),
                "mutating the array passed to the constructor must not affect the stored value");
    }

    @Test
    void putDefensivelyCopiesValueOnEveryRead() {
        WalRecord.Put put = new WalRecord.Put(1, "k", bytes("original"));

        byte[] firstRead = put.value();
        firstRead[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), put.value(),
                "mutating a value returned by value() must not affect subsequent reads");
    }

    @Test
    void putEqualsUsesContentBasedArrayComparisonNotIdentity() {
        WalRecord.Put a = new WalRecord.Put(1, "k", bytes("v"));
        WalRecord.Put b = new WalRecord.Put(1, "k", bytes("v")); // distinct byte[] instance, same content

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void putNotEqualsWhenValueContentDiffers() {
        WalRecord.Put a = new WalRecord.Put(1, "k", bytes("v1"));
        WalRecord.Put b = new WalRecord.Put(1, "k", bytes("v2"));

        assertNotEquals(a, b);
    }

    @Test
    void deleteEqualsAndHashCodeWorkForEqualContent() {
        WalRecord.Delete a = new WalRecord.Delete(1, "k");
        WalRecord.Delete b = new WalRecord.Delete(1, "k");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
