package com.forge.storage.wal;

import java.util.Arrays;
import java.util.Objects;

/**
 * One decoded, durable WAL entry: either a {@link Put} or a {@link Delete}.
 *
 * <p>{@code opType} on disk (not modeled here directly, see {@link WriteAheadLog})
 * is always the sole discriminator between these two cases — never the
 * presence or length of a value — so that a {@code Put} with an empty
 * {@code byte[]} value is never confused with a {@code Delete}.
 */
public sealed interface WalRecord permits WalRecord.Put, WalRecord.Delete {

    /** Monotonically increasing across the WAL's entire lifetime, including restarts. */
    long sequenceNumber();

    /** The key this record affects. */
    String key();

    /**
     * A durable record that {@code key} was set to {@code value}.
     *
     * <p>Defensively copies {@code value} on construction and on every read
     * via {@link #value()}, for the same reason {@link com.forge.storage.InMemoryKeyValueStore}
     * does: nothing that leaves this class is ever the live internal array.
     */
    record Put(long sequenceNumber, String key, byte[] value) implements WalRecord {

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
            return sequenceNumber == other.sequenceNumber
                    && key.equals(other.key)
                    && Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sequenceNumber, key, Arrays.hashCode(value));
        }

        @Override
        public String toString() {
            return "Put[sequenceNumber=" + sequenceNumber + ", key=" + key
                    + ", value.length=" + value.length + "]";
        }
    }

    /** A durable record that {@code key} was deleted. */
    record Delete(long sequenceNumber, String key) implements WalRecord {

        public Delete {
            Objects.requireNonNull(key, "key must not be null");
        }
    }
}
