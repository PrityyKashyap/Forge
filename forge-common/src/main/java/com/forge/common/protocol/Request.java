package com.forge.common.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * A client-to-server request: {@link Put}, {@link Get}, or {@link Delete}.
 *
 * <p>As with {@code WalRecord} in {@code forge-storage}, the concrete
 * variant (not the presence or length of a value) is always the sole
 * discriminator between operations.
 */
public sealed interface Request permits Request.Put, Request.Get, Request.Delete {

    /** The key this request operates on. */
    String key();

    /**
     * Set {@code key} to {@code value}.
     *
     * <p>Defensively copies {@code value} on construction and on every read
     * via {@link #value()}.
     */
    record Put(String key, byte[] value) implements Request {

        public Put {
            Objects.requireNonNull(key, "key must not be null");
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
            if (!(o instanceof Put other)) {
                return false;
            }
            return key.equals(other.key) && Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, Arrays.hashCode(value));
        }

        @Override
        public String toString() {
            return "Put[key=" + key + ", value.length=" + value.length + "]";
        }
    }

    /** Look up the current value of {@code key}, if any. */
    record Get(String key) implements Request {

        public Get {
            Objects.requireNonNull(key, "key must not be null");
        }
    }

    /** Delete {@code key}, if present. */
    record Delete(String key) implements Request {

        public Delete {
            Objects.requireNonNull(key, "key must not be null");
        }
    }
}
