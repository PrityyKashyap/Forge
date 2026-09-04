package com.forge.common.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * A server-to-client response: {@link OkAbsent}, {@link OkPresent}, or
 * {@link Error}.
 */
public sealed interface Response permits Response.OkAbsent, Response.OkPresent, Response.Error {

    /** The request succeeded; the key has no value. */
    record OkAbsent() implements Response {
    }

    /**
     * The request succeeded and returned {@code value}.
     *
     * <p>Defensively copies {@code value} on construction and on every read
     * via {@link #value()}.
     */
    record OkPresent(byte[] value) implements Response {

        public OkPresent {
            Objects.requireNonNull(value, "value must not be null");
            value = value.clone();
        }

        @Override
        public byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OkPresent other)) {
                return false;
            }
            return Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            return "OkPresent[value.length=" + value.length + "]";
        }
    }

    /**
     * The request failed. {@code errorCode} is one of the {@code ERROR_*}
     * constants in {@link ProtocolConstants}; {@code message} is a
     * human-readable diagnostic, not part of the protocol's semantics.
     */
    record Error(byte errorCode, String message) implements Response {

        public Error {
            Objects.requireNonNull(message, "message must not be null");
        }
    }
}
