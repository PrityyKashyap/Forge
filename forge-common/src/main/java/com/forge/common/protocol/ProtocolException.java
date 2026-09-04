package com.forge.common.protocol;

import java.io.IOException;

/**
 * The bytes read off the wire do not form a well-formed frame: a declared
 * length is out of bounds, an opcode/status byte is unrecognized, or a key
 * is not valid UTF-8.
 *
 * <p>Extends {@link IOException} so callers that don't care about the
 * distinction can catch {@code IOException} uniformly, while callers that
 * do (namely the server's connection handler, which reports a specific
 * {@code errorCode} back to the client) can catch this type specifically.
 *
 * <p>Distinct from a plain {@link java.io.EOFException}: EOF means the
 * stream ended (a disconnect, expected at any frame boundary); this means
 * bytes were present but did not make sense (a malformed or misbehaving
 * peer).
 */
public final class ProtocolException extends IOException {

    private final byte errorCode;

    public ProtocolException(byte errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /** One of the {@code ERROR_*} constants in {@link ProtocolConstants}. */
    public byte errorCode() {
        return errorCode;
    }
}
