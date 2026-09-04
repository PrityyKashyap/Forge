package com.forge.storage;

import java.util.Optional;

/**
 * The GET/PUT/DELETE contract for a FORGE storage engine, independent of how
 * (or whether) an implementation persists data. See ARCHITECTURE.md &sect;3.2.
 *
 * <p>Implementations must reject {@code null} keys and {@code null} values,
 * must never conflate "no value for this key" with any legal stored value
 * (including an empty {@code byte[]}), and must not let a caller mutate
 * stored state by retaining a reference to an array passed into or returned
 * from this interface.
 */
public interface KeyValueStore {

    /**
     * Stores {@code value} under {@code key}, replacing any existing value.
     *
     * @return the previous value for {@code key}, or {@link Optional#empty()}
     *         if it had none
     * @throws NullPointerException if {@code key} or {@code value} is null
     */
    Optional<byte[]> put(String key, byte[] value);

    /**
     * @return the value stored for {@code key}, or {@link Optional#empty()}
     *         if it is not present
     * @throws NullPointerException if {@code key} is null
     */
    Optional<byte[]> get(String key);

    /**
     * Removes any value stored for {@code key}. Idempotent: deleting a key
     * that is not present is not an error.
     *
     * @return the value that was removed, or {@link Optional#empty()} if
     *         {@code key} was not present
     * @throws NullPointerException if {@code key} is null
     */
    Optional<byte[]> delete(String key);
}
