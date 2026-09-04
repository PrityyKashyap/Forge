package com.forge.common.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResponseTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void okPresentRejectsNullValue() {
        assertThrows(NullPointerException.class, () -> new Response.OkPresent(null));
    }

    @Test
    void errorRejectsNullMessage() {
        assertThrows(NullPointerException.class,
                () -> new Response.Error(ProtocolConstants.ERROR_STORAGE_ERROR, null));
    }

    @Test
    void okPresentDefensivelyCopiesValueOnConstruction() {
        byte[] input = bytes("original");
        Response.OkPresent present = new Response.OkPresent(input);

        input[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), present.value(),
                "mutating the array passed to the constructor must not affect the stored value");
    }

    @Test
    void okPresentDefensivelyCopiesValueOnEveryRead() {
        Response.OkPresent present = new Response.OkPresent(bytes("original"));

        byte[] firstRead = present.value();
        firstRead[0] = (byte) 'X';

        assertArrayEquals(bytes("original"), present.value(),
                "mutating a value returned by value() must not affect subsequent reads");
    }

    @Test
    void okPresentEqualsUsesContentBasedArrayComparisonNotIdentity() {
        Response.OkPresent a = new Response.OkPresent(bytes("v"));
        Response.OkPresent b = new Response.OkPresent(bytes("v"));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void okPresentNotEqualsWhenValueContentDiffers() {
        Response.OkPresent a = new Response.OkPresent(bytes("v1"));
        Response.OkPresent b = new Response.OkPresent(bytes("v2"));

        assertNotEquals(a, b);
    }

    @Test
    void okAbsentInstancesAreEqual() {
        assertEquals(new Response.OkAbsent(), new Response.OkAbsent());
    }

    @Test
    void errorEqualsAndHashCodeWorkForEqualContent() {
        Response.Error a = new Response.Error(ProtocolConstants.ERROR_STORAGE_ERROR, "disk full");
        Response.Error b = new Response.Error(ProtocolConstants.ERROR_STORAGE_ERROR, "disk full");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
