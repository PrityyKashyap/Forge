package com.forge.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Phase 1 storage engine: a single-threaded, purely in-memory
 * {@link KeyValueStore} backed by a {@link HashMap}.
 *
 * <p><b>Not thread-safe.</b> This class relies on being called from a
 * single thread. Concurrent access from multiple threads is undefined
 * behavior until Phase 4 adds explicit concurrency control.
 *
 * <p><b>Not durable.</b> All state is lost when the process exits. This is
 * intentional for Phase 1 — durability arrives in Phase 2 (write-ahead log).
 *
 * <p>Every {@code byte[]} that crosses this class's boundary, in either
 * direction, is defensively copied: a value is cloned on the way in (
 * {@link #put}) and cloned on the way out ({@link #get}, {@link #delete}).
 * This guarantees a caller can never mutate this store's state by holding a
 * reference to an array they passed in or received back.
 */
public final class InMemoryKeyValueStore implements KeyValueStore {

    private final Map<String, byte[]> data = new HashMap<>();

    @Override
    public Optional<byte[]> put(String key, byte[] value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        byte[] previous = data.put(key, value.clone());
        return copyOf(previous);
    }

    @Override
    public Optional<byte[]> get(String key) {
        Objects.requireNonNull(key, "key must not be null");
        return copyOf(data.get(key));
    }

    @Override
    public Optional<byte[]> delete(String key) {
        Objects.requireNonNull(key, "key must not be null");
        return copyOf(data.remove(key));
    }

    private static Optional<byte[]> copyOf(byte[] value) {
        return Optional.ofNullable(value).map(byte[]::clone);
    }
}
