package com.forge.client;

import java.io.IOException;

/**
 * The server processed the request and reported failure ({@code Response.Error}),
 * as opposed to a transport-level failure. Extends {@link IOException} so
 * callers can catch one exception hierarchy for both kinds of failure, or
 * catch this type specifically to inspect {@link #errorCode()}.
 */
public final class ForgeServerException extends IOException {

    private final byte errorCode;

    public ForgeServerException(byte errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /** One of the {@code ERROR_*} constants in {@code com.forge.common.protocol.ProtocolConstants}. */
    public byte errorCode() {
        return errorCode;
    }
}
