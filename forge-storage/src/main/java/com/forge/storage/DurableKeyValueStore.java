package com.forge.storage;

import com.forge.storage.wal.WalRecord;
import com.forge.storage.wal.WriteAheadLog;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Phase 2 storage engine: a {@link KeyValueStore} whose mutations are durable
 * across a process crash. Composes a {@link WriteAheadLog} (durability) with
 * an {@link InMemoryKeyValueStore} (the servable data structure) — neither
 * of those two classes knows the other exists; this class is the only one
 * that does.
 *
 * <p>Every {@code put}/{@code delete} appends and forces a WAL record
 * <em>before</em> touching in-memory state (write-ahead ordering — see
 * DESIGN.md &sect;2 and &sect;11). {@code get} never touches the WAL at all.
 *
 * <p>On construction, every valid record recovered from the WAL file is
 * replayed, in order, into a fresh {@link InMemoryKeyValueStore} — this is
 * the crash-recovery path.
 *
 * <p>{@link KeyValueStore}'s methods cannot declare a checked exception, so
 * an {@link IOException} from the WAL (e.g. disk full) is rethrown as an
 * {@link UncheckedIOException}, the same pattern already used by
 * {@link com.forge.storage.repl.Repl#run()}.
 *
 * <p><b>Not thread-safe</b> — exactly like the classes it composes.
 */
public final class DurableKeyValueStore implements KeyValueStore, Closeable {

    private final InMemoryKeyValueStore memory = new InMemoryKeyValueStore();
    private final WriteAheadLog wal;

    public DurableKeyValueStore(Path walFile) throws IOException {
        this.wal = WriteAheadLog.open(walFile);
        for (WalRecord record : wal.recoveredRecords()) {
            switch (record) {
                case WalRecord.Put put -> memory.put(put.key(), put.value());
                case WalRecord.Delete delete -> memory.delete(delete.key());
            }
        }
    }

    @Override
    public Optional<byte[]> put(String key, byte[] value) {
        try {
            wal.appendPut(key, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return memory.put(key, value);
    }

    @Override
    public Optional<byte[]> get(String key) {
        return memory.get(key);
    }

    @Override
    public Optional<byte[]> delete(String key) {
        try {
            wal.appendDelete(key);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return memory.delete(key);
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }
}
