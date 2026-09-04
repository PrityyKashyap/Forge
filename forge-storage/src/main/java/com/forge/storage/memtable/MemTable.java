package com.forge.storage.memtable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The Phase 3 write-side staging structure: a sorted, in-memory,
 * single-threaded map from key to {@link StoredEntry}, absorbing writes
 * between flushes.
 *
 * <p>Sorted ({@link TreeMap}) so that flushing to an SSTable never needs a
 * separate sort pass — iteration is already in key order, for free.
 *
 * <p>{@code delete} inserts a {@link StoredEntry.Tombstone} rather than
 * removing the key: a MemTable does not know, on its own, whether an older
 * SSTable holds a stale value for that key that the deletion must shadow.
 *
 * <p>Tracks an approximate byte size (for the flush-trigger threshold) and
 * the highest sequence number it has absorbed (for the eventual SSTable
 * header's watermark) — both updated incrementally on every mutation.
 *
 * <p><b>Not thread-safe</b> — exactly like every other Phase 1-3 engine
 * class. Concurrent access is undefined behavior until Phase 4.
 */
public final class MemTable {

    private final TreeMap<String, StoredEntry> entries = new TreeMap<>();
    private long approximateSizeInBytes;
    private long maxSequenceNumber;

    public void put(String key, byte[] value, long sequenceNumber) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        record(key, new StoredEntry.Value(value), sequenceNumber);
    }

    public void delete(String key, long sequenceNumber) {
        Objects.requireNonNull(key, "key must not be null");
        record(key, new StoredEntry.Tombstone(), sequenceNumber);
    }

    private void record(String key, StoredEntry entry, long sequenceNumber) {
        StoredEntry previous = entries.put(key, entry);
        if (previous != null) {
            approximateSizeInBytes -= sizeOf(key, previous);
        }
        approximateSizeInBytes += sizeOf(key, entry);
        maxSequenceNumber = Math.max(maxSequenceNumber, sequenceNumber);
    }

    private static long sizeOf(String key, StoredEntry entry) {
        long keyBytes = key.getBytes(StandardCharsets.UTF_8).length;
        long valueBytes = (entry instanceof StoredEntry.Value v) ? v.bytes().length : 0;
        return keyBytes + valueBytes;
    }

    public Optional<StoredEntry> get(String key) {
        Objects.requireNonNull(key, "key must not be null");
        return Optional.ofNullable(entries.get(key));
    }

    /** A read-only, ascending-key-order view, for flush iteration. Not a defensive copy: the MemTable being flushed is never written to again. */
    public SortedMap<String, StoredEntry> entries() {
        return Collections.unmodifiableSortedMap(entries);
    }

    public long approximateSizeInBytes() {
        return approximateSizeInBytes;
    }

    /** The highest sequence number absorbed so far; 0 if nothing has been written yet. */
    public long maxSequenceNumber() {
        return maxSequenceNumber;
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }
}
