package com.forge.common.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Encodes and decodes FORGE's wire protocol.
 *
 * <p>Wire format, matching the length-prefixed style already used by
 * {@code WriteAheadLog} and the SSTable format:
 *
 * <pre>
 * FRAME:            [int32 frameLength][payload]
 * REQUEST payload:  [int8 opCode][int32 keyLength][keyBytes][int32 valueLength][valueBytes]
 * RESPONSE payload: [int8 status][int8 errorCode][int32 payloadLength][payloadBytes]
 * </pre>
 *
 * <p>As with WAL recovery, every length is validated against both a
 * configured maximum and the bytes actually available before it is used to
 * allocate an array — a corrupted or hostile length field can never trigger
 * an oversized allocation.
 */
public final class FrameCodec {

    private FrameCodec() {
    }

    private static final byte[] EMPTY = new byte[0];
    private static final int MIN_REQUEST_PAYLOAD_BYTES = 1 + 4 + 4;
    private static final int MIN_RESPONSE_PAYLOAD_BYTES = 1 + 1 + 4;

    // ---------------------------------------------------------------
    // writing
    // ---------------------------------------------------------------

    public static void writeRequest(OutputStream out, Request request) throws IOException {
        writeFrame(out, encodeRequest(request));
    }

    public static void writeResponse(OutputStream out, Response response) throws IOException {
        writeFrame(out, encodeResponse(response));
    }

    private static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        DataOutputStream data = out instanceof DataOutputStream d ? d : new DataOutputStream(out);
        data.writeInt(payload.length);
        data.write(payload);
        data.flush();
    }

    private static byte[] encodeRequest(Request request) {
        byte opCode;
        String key;
        byte[] value;
        switch (request) {
            case Request.Put put -> {
                opCode = ProtocolConstants.OP_PUT;
                key = put.key();
                value = put.value();
            }
            case Request.Get get -> {
                opCode = ProtocolConstants.OP_GET;
                key = get.key();
                value = EMPTY;
            }
            case Request.Delete delete -> {
                opCode = ProtocolConstants.OP_DELETE;
                key = delete.key();
                value = EMPTY;
            }
        }
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + keyBytes.length + 4 + value.length);
        buf.put(opCode);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(value.length);
        buf.put(value);
        return buf.array();
    }

    private static byte[] encodeResponse(Response response) {
        byte status;
        byte errorCode = ProtocolConstants.ERROR_NONE;
        byte[] payload;
        switch (response) {
            case Response.OkAbsent ignored -> {
                status = ProtocolConstants.STATUS_OK_ABSENT;
                payload = EMPTY;
            }
            case Response.OkPresent present -> {
                status = ProtocolConstants.STATUS_OK_PRESENT;
                payload = present.value();
            }
            case Response.Error error -> {
                status = ProtocolConstants.STATUS_ERROR;
                errorCode = error.errorCode();
                payload = error.message().getBytes(StandardCharsets.UTF_8);
            }
        }
        ByteBuffer buf = ByteBuffer.allocate(1 + 1 + 4 + payload.length);
        buf.put(status);
        buf.put(errorCode);
        buf.putInt(payload.length);
        buf.put(payload);
        return buf.array();
    }

    // ---------------------------------------------------------------
    // reading
    // ---------------------------------------------------------------

    /**
     * Reads one request frame.
     *
     * @throws EOFException     if the stream ends before or during the frame
     *                           (a peer disconnect; expected at any frame boundary)
     * @throws ProtocolException if bytes were present but do not form a
     *                           well-formed request
     */
    public static Request readRequest(InputStream in, int maxFrameLength, int maxKeyLength)
            throws IOException {
        byte[] payload = readFrame(in, maxFrameLength);
        return decodeRequest(payload, maxKeyLength);
    }

    /**
     * Reads one response frame.
     *
     * @throws EOFException     if the stream ends before or during the frame
     * @throws ProtocolException if bytes were present but do not form a
     *                           well-formed response
     */
    public static Response readResponse(InputStream in, int maxFrameLength) throws IOException {
        byte[] payload = readFrame(in, maxFrameLength);
        return decodeResponse(payload);
    }

    private static byte[] readFrame(InputStream in, int maxFrameLength) throws IOException {
        DataInputStream data = in instanceof DataInputStream d ? d : new DataInputStream(in);
        int frameLength = data.readInt();
        if (frameLength < 0 || frameLength > maxFrameLength) {
            throw new ProtocolException(ProtocolConstants.ERROR_OVERSIZED_REQUEST,
                    "declared frame length " + frameLength + " is invalid or exceeds maximum " + maxFrameLength);
        }
        byte[] payload = new byte[frameLength];
        data.readFully(payload);
        return payload;
    }

    private static Request decodeRequest(byte[] payload, int maxKeyLength) throws ProtocolException {
        if (payload.length < MIN_REQUEST_PAYLOAD_BYTES) {
            throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST,
                    "request frame too short: " + payload.length + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(payload);
        try {
            byte opCode = buf.get();

            int keyLength = buf.getInt();
            if (keyLength < 0 || keyLength > maxKeyLength || keyLength > buf.remaining()) {
                throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST,
                        "invalid key length " + keyLength);
            }
            byte[] keyBytes = new byte[keyLength];
            buf.get(keyBytes);
            String key = decodeUtf8Strict(keyBytes);

            if (buf.remaining() < 4) {
                throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST,
                        "truncated value length field");
            }
            int valueLength = buf.getInt();
            if (valueLength < 0 || valueLength != buf.remaining()) {
                throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST,
                        "invalid value length " + valueLength);
            }
            byte[] value = new byte[valueLength];
            buf.get(value);

            return switch (opCode) {
                case ProtocolConstants.OP_PUT -> new Request.Put(key, value);
                case ProtocolConstants.OP_GET -> new Request.Get(key);
                case ProtocolConstants.OP_DELETE -> new Request.Delete(key);
                default -> throw new ProtocolException(ProtocolConstants.ERROR_UNKNOWN_OPERATION,
                        "unknown opcode " + opCode);
            };
        } catch (BufferUnderflowException e) {
            throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST, "truncated request frame");
        }
    }

    private static Response decodeResponse(byte[] payload) throws ProtocolException {
        if (payload.length < MIN_RESPONSE_PAYLOAD_BYTES) {
            throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST,
                    "response frame too short: " + payload.length + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(payload);
        try {
            byte status = buf.get();
            byte errorCode = buf.get();
            int payloadLength = buf.getInt();
            if (payloadLength < 0 || payloadLength != buf.remaining()) {
                throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST,
                        "invalid response payload length " + payloadLength);
            }
            byte[] responsePayload = new byte[payloadLength];
            buf.get(responsePayload);

            return switch (status) {
                case ProtocolConstants.STATUS_OK_ABSENT -> new Response.OkAbsent();
                case ProtocolConstants.STATUS_OK_PRESENT -> new Response.OkPresent(responsePayload);
                case ProtocolConstants.STATUS_ERROR ->
                        new Response.Error(errorCode, new String(responsePayload, StandardCharsets.UTF_8));
                default -> throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST,
                        "unknown response status " + status);
            };
        } catch (BufferUnderflowException e) {
            throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST, "truncated response frame");
        }
    }

    private static String decodeUtf8Strict(byte[] bytes) throws ProtocolException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new ProtocolException(ProtocolConstants.ERROR_MALFORMED_REQUEST, "key is not valid UTF-8");
        }
    }
}
