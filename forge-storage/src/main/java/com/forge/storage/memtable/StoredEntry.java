package com.forge.storage.memtable;

import java.util.Arrays;
import java.util.Objects;

/**
 * One logical entry for a key, shared by the MemTable (in memory) and
 * SSTables (on disk): either a live {@link Value} or an explicit
 * {@link Tombstone}.
 *
 * <p>A tombstone is not "absence" — it is a positive record that a key was
 * deleted as of this generation, which must shadow (and never be confused
 * with) whatever an older generation might still say about that key. See
 * DESIGN.md's Phase 3 design notes on why DELETE cannot simply remove a key
 * from the MemTable.
 *
 * <p>Unlike {@link com.forge.storage.wal.WalRecord}, entries here carry no
 * per-record sequence number: within one MemTable/SSTable generation there
 * is no ordering ambiguity to resolve (a generation is a single, atomic
 * snapshot) — only the generation's own watermark matters when comparing
 * across generations.
 */
public sealed interface StoredEntry permits StoredEntry.Value, StoredEntry.Tombstone {

    /**
     * A live value. Defensively copies {@code bytes} on construction and on
     * every read via {@link #bytes()} — the same discipline already applied
     * to {@link com.forge.storage.InMemoryKeyValueStore} and
     * {@link com.forge.storage.wal.WalRecord.Put}.
     */
    record Value(byte[] bytes) implements StoredEntry {

        public Value {
            Objects.requireNonNull(bytes, "bytes must not be null");
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Value other)) {
                return false;
            }
            return Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return "Value[bytes.length=" + bytes.length + "]";
        }
    }

    /** An explicit marker that a key was deleted as of this generation. */
    record Tombstone() implements StoredEntry {
    }
}
