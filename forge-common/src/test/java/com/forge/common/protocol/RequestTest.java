package com.forge.common.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> new Request.Put(null, bytes("v")));
    }

    @Test
    void putRejectsNullValue() {
        assertThrows(NullPointerException.class, () -> new Request.Put("k", null));
    }

    @Test
    void getRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> new Request.Get(null));
    }

    @Test
    void deleteRejectsNullKey() {
        assertThrows(NullPointerException.class, () -> new Request.Delete(null));
    }

    @Test
    void putDefensivelyCopiesValueOnConstruction() {
        byte[] input = bytes("original");
        Request.Put put = new Request.Put("k", input);

        input[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), put.value(),
                "mutating the array passed to the constructor must not affect the stored value");
    }

    @Test
    void putDefensivelyCopiesValueOnEveryRead() {
        Request.Put put = new Request.Put("k", bytes("original"));

        byte[] firstRead = put.value();
        firstRead[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), put.value(),
                "mutating a value returned by value() must not affect subsequent reads");
    }

    @Test
    void putEqualsUsesContentBasedArrayComparisonNotIdentity() {
        Request.Put a = new Request.Put("k", bytes("v"));
        Request.Put b = new Request.Put("k", bytes("v"));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void putNotEqualsWhenValueContentDiffers() {
        Request.Put a = new Request.Put("k", bytes("v1"));
        Request.Put b = new Request.Put("k", bytes("v2"));

        assertNotEquals(a, b);
    }

    @Test
    void getEqualsAndHashCodeWorkForEqualContent() {
        Request.Get a = new Request.Get("k");
        Request.Get b = new Request.Get("k");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void deleteEqualsAndHashCodeWorkForEqualContent() {
        Request.Delete a = new Request.Delete("k");
        Request.Delete b = new Request.Delete("k");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
