package com.forge.common.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * These tests independently hand-craft raw frame bytes for the malformed
 * cases (see {@link #frame}) rather than reusing {@link FrameCodec}'s own
 * encoder, so a bug in the real encoder can't also hide from the test
 * that's supposed to catch it.
 */
class FrameCodecTest {

    private static final int MAX_FRAME = ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH;
    private static final int MAX_KEY = ProtocolConstants.DEFAULT_MAX_KEY_LENGTH;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Wraps a hand-built payload in a valid [int32 length][payload] frame. */
    private static byte[] frame(byte[] payload) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(out);
        data.writeInt(payload.length);
        data.write(payload);
        return out.toByteArray();
    }

    /** Hand-builds a raw request payload: opCode, key, value — bypassing FrameCodec entirely. */
    private static byte[] rawRequestPayload(byte opCode, byte[] key, byte[] value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(out);
        data.writeByte(opCode);
        data.writeInt(key.length);
        data.write(key);
        data.writeInt(value.length);
        data.write(value);
        return out.toByteArray();
    }

    private static byte[] rawResponsePayload(byte status, byte errorCode, byte[] payload) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(out);
        data.writeByte(status);
        data.writeByte(errorCode);
        data.writeInt(payload.length);
        data.write(payload);
        return out.toByteArray();
    }

    // --- round trips -----------------------------------------------------

    @Test
    void putRoundTrips() throws IOException {
        Request.Put original = new Request.Put("k", bytes("v"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeRequest(out, original);

        Request decoded = FrameCodec.readRequest(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME, MAX_KEY);

        Request.Put decodedPut = assertInstanceOf(Request.Put.class, decoded);
        assertEquals("k", decodedPut.key());
        assertArrayEquals(bytes("v"), decodedPut.value());
    }

    @Test
    void getRoundTrips() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeRequest(out, new Request.Get("k"));

        Request decoded = FrameCodec.readRequest(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME, MAX_KEY);

        assertEquals(new Request.Get("k"), decoded);
    }

    @Test
    void deleteRoundTrips() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeRequest(out, new Request.Delete("k"));

        Request decoded = FrameCodec.readRequest(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME, MAX_KEY);

        assertEquals(new Request.Delete("k"), decoded);
    }

    @Test
    void putWithEmptyValueRoundTrips() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeRequest(out, new Request.Put("k", new byte[0]));

        Request decoded = FrameCodec.readRequest(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME, MAX_KEY);

        Request.Put decodedPut = assertInstanceOf(Request.Put.class, decoded);
        assertArrayEquals(new byte[0], decodedPut.value());
    }

    @Test
    void okAbsentRoundTrips() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeResponse(out, new Response.OkAbsent());

        Response decoded = FrameCodec.readResponse(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME);

        assertEquals(new Response.OkAbsent(), decoded);
    }

    @Test
    void okPresentRoundTrips() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeResponse(out, new Response.OkPresent(bytes("v")));

        Response decoded = FrameCodec.readResponse(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME);

        Response.OkPresent decodedPresent = assertInstanceOf(Response.OkPresent.class, decoded);
        assertArrayEquals(bytes("v"), decodedPresent.value());
    }

    @Test
    void errorRoundTrips() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeResponse(out, new Response.Error(ProtocolConstants.ERROR_STORAGE_ERROR, "disk full"));

        Response decoded = FrameCodec.readResponse(new ByteArrayInputStream(out.toByteArray()), MAX_FRAME);

        Response.Error decodedError = assertInstanceOf(Response.Error.class, decoded);
        assertEquals(ProtocolConstants.ERROR_STORAGE_ERROR, decodedError.errorCode());
        assertEquals("disk full", decodedError.message());
    }

    @Test
    void multipleFramesOnOneStreamReadSequentiallyAndCorrectly() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FrameCodec.writeRequest(out, new Request.Put("a", bytes("1")));
        FrameCodec.writeRequest(out, new Request.Get("b"));
        FrameCodec.writeRequest(out, new Request.Delete("c"));

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        assertEquals(new Request.Put("a", bytes("1")), FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(new Request.Get("b"), FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(new Request.Delete("c"), FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
    }

    // --- EOF / disconnect handling ----------------------------------------

    @Test
    void cleanEofAtFrameBoundaryThrowsEofException() {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        assertThrows(EOFException.class, () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
    }

    @Test
    void truncatedLengthPrefixThrowsEofException() {
        InputStream in = new ByteArrayInputStream(new byte[]{0, 0}); // only 2 of 4 length bytes
        assertThrows(EOFException.class, () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
    }

    @Test
    void truncatedPayloadThrowsEofException() throws IOException {
        byte[] payload = rawRequestPayload(ProtocolConstants.OP_GET, bytes("k"), new byte[0]);
        byte[] full = frame(payload);
        byte[] truncated = new byte[full.length - 1]; // drop the last payload byte
        System.arraycopy(full, 0, truncated, 0, truncated.length);

        InputStream in = new ByteArrayInputStream(truncated);
        assertThrows(EOFException.class, () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
    }

    // --- malformed frame length --------------------------------------------

    @Test
    void negativeFrameLengthIsRejectedWithoutAllocating() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new DataOutputStream(out).writeInt(-1);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_OVERSIZED_REQUEST, e.errorCode());
    }

    @Test
    void frameLengthExceedingMaximumIsRejectedWithoutAllocating() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new DataOutputStream(out).writeInt(Integer.MAX_VALUE);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, 1024, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_OVERSIZED_REQUEST, e.errorCode());
    }

    // --- malformed request payloads -----------------------------------------

    @Test
    void unknownOpCodeIsRejectedAfterFullyConsumingTheFrame() throws IOException {
        byte[] badFrame = frame(rawRequestPayload((byte) 99, bytes("k"), bytes("v")));
        byte[] goodFrame = frame(rawRequestPayload(ProtocolConstants.OP_GET, bytes("next"), new byte[0]));

        ByteArrayOutputStream combined = new ByteArrayOutputStream();
        combined.write(badFrame);
        combined.write(goodFrame);

        InputStream in = new ByteArrayInputStream(combined.toByteArray());
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_UNKNOWN_OPERATION, e.errorCode());

        // The bad frame's bytes were fully consumed, so the stream is positioned at the next frame.
        Request decoded = FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY);
        assertEquals(new Request.Get("next"), decoded);
    }

    @Test
    void negativeKeyLengthIsRejected() throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(payload);
        data.writeByte(ProtocolConstants.OP_GET);
        data.writeInt(-1);

        InputStream in = new ByteArrayInputStream(frame(payload.toByteArray()));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    @Test
    void keyLengthExceedingConfiguredMaximumIsRejectedWithoutAllocating() throws IOException {
        byte[] payload = rawRequestPayload(ProtocolConstants.OP_GET, bytes("some key"), new byte[0]);

        InputStream in = new ByteArrayInputStream(frame(payload));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, 4)); // "some key" is longer than 4 bytes
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    @Test
    void keyLengthExceedingRemainingFrameBytesIsRejectedWithoutAllocating() throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(payload);
        data.writeByte(ProtocolConstants.OP_GET);
        data.writeInt(1_000_000); // claims a huge key but the frame is tiny

        InputStream in = new ByteArrayInputStream(frame(payload.toByteArray()));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    @Test
    void valueLengthNotMatchingRemainingBytesIsRejected() throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(payload);
        data.writeByte(ProtocolConstants.OP_PUT);
        byte[] key = bytes("k");
        data.writeInt(key.length);
        data.write(key);
        data.writeInt(999); // does not match what's actually left in the frame
        data.write(bytes("v"));

        InputStream in = new ByteArrayInputStream(frame(payload.toByteArray()));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    @Test
    void requestFrameTooShortToContainHeaderIsRejected() throws IOException {
        InputStream in = new ByteArrayInputStream(frame(new byte[]{ProtocolConstants.OP_GET}));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    @Test
    void keyThatIsNotValidUtf8IsRejected() throws IOException {
        byte[] invalidUtf8 = new byte[]{(byte) 0xFF, (byte) 0xFE};
        byte[] payload = rawRequestPayload(ProtocolConstants.OP_GET, invalidUtf8, new byte[0]);

        InputStream in = new ByteArrayInputStream(frame(payload));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readRequest(in, MAX_FRAME, MAX_KEY));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    // --- malformed response payloads ----------------------------------------

    @Test
    void unknownResponseStatusIsRejected() throws IOException {
        byte[] payload = rawResponsePayload((byte) 99, ProtocolConstants.ERROR_NONE, new byte[0]);

        InputStream in = new ByteArrayInputStream(frame(payload));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readResponse(in, MAX_FRAME));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    @Test
    void responsePayloadLengthNotMatchingRemainingBytesIsRejected() throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(payload);
        data.writeByte(ProtocolConstants.STATUS_OK_PRESENT);
        data.writeByte(ProtocolConstants.ERROR_NONE);
        data.writeInt(999);
        data.write(bytes("v"));

        InputStream in = new ByteArrayInputStream(frame(payload.toByteArray()));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readResponse(in, MAX_FRAME));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }

    @Test
    void responseFrameTooShortToContainHeaderIsRejected() throws IOException {
        InputStream in = new ByteArrayInputStream(frame(new byte[]{ProtocolConstants.STATUS_OK_ABSENT}));
        ProtocolException e = assertThrows(ProtocolException.class,
                () -> FrameCodec.readResponse(in, MAX_FRAME));
        assertEquals(ProtocolConstants.ERROR_MALFORMED_REQUEST, e.errorCode());
    }
}
