package com.forge.bench;

import com.forge.storage.KeyValueStore;

/**
 * Executes operations directly against a {@link KeyValueStore}, with no
 * socket, no wire protocol, and no {@code ForgeServer} in the path — the
 * baseline E4 compares real client/server traffic against.
 *
 * <p>Does not own {@code store}'s lifecycle: {@link #close()} is a no-op,
 * since the store is shared across every thread in an in-process run and is
 * closed once by whoever created it.
 */
final class InProcessOpExecutor implements OpExecutor {

    private final KeyValueStore store;

    InProcessOpExecutor(KeyValueStore store) {
        this.store = store;
    }

    @Override
    public void execute(BenchOperation op) {
        switch (op) {
            case BenchOperation.Get get -> store.get(get.key());
            case BenchOperation.Put put -> store.put(put.key(), put.value());
            case BenchOperation.Delete delete -> store.delete(delete.key());
        }
    }

    @Override
    public void close() {
        // no-op: shared store's lifecycle belongs to the caller that created it
    }
}
