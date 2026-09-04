package com.forge.storage;

import com.forge.storage.compaction.Compactor;
import com.forge.storage.memtable.MemTable;
import com.forge.storage.memtable.StoredEntry;
import com.forge.storage.sstable.SSTableReader;
import com.forge.storage.sstable.SSTableWriter;
import com.forge.storage.wal.ReplicationOutcome;
import com.forge.storage.wal.WalRecord;
import com.forge.storage.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Phase 4 storage engine: a thread-safe {@link KeyValueStore} providing the
 * same durability and recovery guarantees as {@link LsmKeyValueStore}, safe
 * for concurrent PUT/GET/DELETE from multiple threads.
 *
 * <p>This is <b>not</b> a wrapper around {@link LsmKeyValueStore} — that
 * class is left completely untouched (it remains the Phase 3 baseline for
 * benchmarking, and a valid, simpler, single-threaded engine in its own
 * right). Making flush's slow disk I/O never block concurrent writers
 * requires splitting flush into locked and lock-free phases inside what
 * would otherwise be one monolithic call, which isn't expressible by
 * wrapping {@code LsmKeyValueStore}'s existing, already-sequenced
 * {@code flush()} with an outer lock. So this class directly composes the
 * same lower-level building blocks ({@link WriteAheadLog}, {@link MemTable},
 * {@link SSTableWriter}, {@link SSTableReader}) with its own orchestration.
 *
 * <h2>Synchronization model</h2>
 * One {@link ReentrantReadWriteLock} ({@code stateLock}) protects exactly
 * three fields: {@link #active}, {@link #frozen}, and {@link #sstables}.
 * <ul>
 *   <li>{@code get()} takes the <b>read</b> lock just long enough to snapshot
 *       {@code active}/{@code frozen}/{@code sstables} together as one
 *       consistent group, then releases it before scanning any SSTable on
 *       disk — the (possibly slow) scan is lock-free.</li>
 *   <li>{@code put()}/{@code delete()} take the <b>write</b> lock for the
 *       in-memory lookup (active/frozen only), the WAL append (which
 *       internally also takes {@link WriteAheadLog}'s own lock), and the
 *       {@code active} mutation — then release it before any deferred
 *       SSTable scan (for the return value) or triggered flush's disk I/O.</li>
 *   <li>A flush's slow phase — writing, forcing, and atomically renaming the
 *       new SSTable, and truncating the WAL — holds <b>no {@code stateLock}
 *       at all</b>. Only the brief freeze (swap {@code active}/{@code frozen})
 *       and the brief completion (register the new reader, clear
 *       {@code frozen}) touch it, each for microseconds.</li>
 * </ul>
 * This is why concurrent writers are never blocked by SSTable disk I/O: the
 * only things ever guarded by {@code stateLock} are a handful of reference
 * reads/writes, never a disk operation. WAL truncation uses
 * {@link WriteAheadLog#truncateUpTo}, not {@code stateLock} — so it never
 * blocks a concurrent {@code get()} either, which has no dependency on WAL
 * state at all.
 *
 * <h2>Why a "frozen" MemTable tier exists</h2>
 * {@code delete()} always mutates {@code active}, never {@code frozen} —
 * once frozen, a MemTable is never written to again. {@code get()} must
 * therefore check both tiers (active, then frozen) before falling through to
 * SSTables — and critically, it must read all three fields
 * ({@code active}/{@code frozen}/{@code sstables}) as one atomic group, not
 * as three independent reads. Reading them independently is not safe: a
 * reader could observe {@code frozen == null} (already cleared, because a
 * flush just completed) paired with an {@code sstables} snapshot taken
 * *before* that same flush added its new reader — a combination that never
 * existed as one real state, and which would make a fully, durably flushed
 * key transiently invisible to GET. The fix is that {@code frozen} is
 * cleared and the new reader is added to {@code sstables} in the very same
 * {@code stateLock} write-lock hold, so no reader can ever observe one
 * without the other.
 *
 * <h2>Recovery</h2>
 * Runs entirely inside the constructor — the same algorithm as
 * {@link LsmKeyValueStore}'s (discover and validate SSTables, compute the
 * watermark, open the WAL, {@code ensureNextSequenceNumberAtLeast}, replay
 * only records newer than the watermark) — before the object is published
 * to any other thread. No locking is needed or used during construction.
 */
public final class ConcurrentLsmKeyValueStore implements KeyValueStore, Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentLsmKeyValueStore.class);

    private static final String WAL_FILE_NAME = "forge.wal";
    private static final String FLUSH_TEMP_FILE_NAME = "flush.tmp";
    private static final String COMPACTION_TEMP_FILE_NAME = "compaction.tmp";

    /** An explicitly unbenchmarked placeholder default — see DESIGN.md on not fabricating tuned numbers. */
    private static final long DEFAULT_FLUSH_THRESHOLD_BYTES = 4L * 1024 * 1024;

    /**
     * Phase 13: once a flush leaves this many SSTables live, a full
     * compaction runs synchronously (on the same thread that just
     * completed the flush) before that write call returns — matching
     * flush's own "off the lock, but still inline" pattern. An explicitly
     * unbenchmarked placeholder default, same disclosure as
     * {@link #DEFAULT_FLUSH_THRESHOLD_BYTES}.
     */
    private static final int DEFAULT_COMPACTION_TRIGGER_COUNT = 4;

    private final Path dataDirectory;
    private final long flushThresholdBytes;
    private final int compactionTriggerCount;
    private final WriteAheadLog wal;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /** Guards against two compactions running at once; unrelated to {@link #stateLock}, which only ever needs to be held briefly. */
    private final AtomicBoolean compactionInProgress = new AtomicBoolean(false);
    /** Gives every compaction's output file a name no flush could ever produce — see {@code compactedSstableFileName}. */
    private final AtomicLong compactionCounter = new AtomicLong();

    /**
     * Phase 9: notified with every record this store durably applies —
     * whether from a local {@link #put}/{@link #delete} or from another
     * node's stream via {@link #applyReplicated} — so a leader can forward
     * its own write stream to followers. Deliberately a plain list under no
     * lock of its own: notification always happens <em>after</em>
     * {@code stateLock} has already been released (see {@link #put}), the
     * same place {@code flush()}'s disk I/O is deferred to, so a slow or
     * misbehaving listener (e.g. a stalled network write to a follower)
     * cannot block other writers the way running it under the lock would.
     * A listener that throws is caught and logged, never allowed to break
     * the write path that already durably succeeded before it ran.
     */
    private final CopyOnWriteArrayList<Consumer<WalRecord>> replicationListeners = new CopyOnWriteArrayList<>();

    /** Guarded by {@link #stateLock}. Accepts new writes; never null. */
    private MemTable active;

    /** Guarded by {@link #stateLock}. Non-null only while a flush is draining it to disk; never written to again once set. */
    private MemTable frozen;

    /** Guarded by {@link #stateLock}. Newest-first. Reassigned wholesale on each flush completion so earlier snapshots stay valid. */
    private List<SSTableReader> sstables;

    public ConcurrentLsmKeyValueStore(Path dataDirectory) throws IOException {
        this(dataDirectory, DEFAULT_FLUSH_THRESHOLD_BYTES);
    }

    public ConcurrentLsmKeyValueStore(Path dataDirectory, long flushThresholdBytes) throws IOException {
        this(dataDirectory, flushThresholdBytes, DEFAULT_COMPACTION_TRIGGER_COUNT);
    }

    /**
     * @param compactionTriggerCount SSTable count at which a full compaction
     *                               is triggered after a flush; exposed
     *                               mainly so tests can force compaction
     *                               deterministically without needing to
     *                               generate {@link #DEFAULT_COMPACTION_TRIGGER_COUNT}
     *                               flushes' worth of data
     */
    public ConcurrentLsmKeyValueStore(Path dataDirectory, long flushThresholdBytes, int compactionTriggerCount)
            throws IOException {
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        if (flushThresholdBytes <= 0) {
            throw new IllegalArgumentException("flushThresholdBytes must be positive");
        }
        if (compactionTriggerCount < 2) {
            throw new IllegalArgumentException("compactionTriggerCount must be at least 2");
        }
        this.flushThresholdBytes = flushThresholdBytes;
        this.compactionTriggerCount = compactionTriggerCount;

        Files.createDirectories(dataDirectory);
        List<SSTableReader> discovered = discoverSSTables(dataDirectory);
        long watermark = discovered.stream().mapToLong(SSTableReader::maxSequenceNumber).max().orElse(0L);

        WriteAheadLog openedWal;
        try {
            openedWal = WriteAheadLog.open(dataDirectory.resolve(WAL_FILE_NAME));
        } catch (IOException | RuntimeException e) {
            closeAll(discovered, e);
            throw e;
        }
        this.wal = openedWal;
        wal.ensureNextSequenceNumberAtLeast(watermark + 1);

        MemTable rebuilt = new MemTable();
        for (WalRecord record : wal.recoveredRecords()) {
            if (record.sequenceNumber() <= watermark) {
                continue; // already durably reflected in the newest SSTable
            }
            switch (record) {
                case WalRecord.Put put -> rebuilt.put(put.key(), put.value(), put.sequenceNumber());
                case WalRecord.Delete delete -> rebuilt.delete(delete.key(), delete.sequenceNumber());
            }
        }
        this.active = rebuilt;
        this.frozen = null;
        this.sstables = List.copyOf(discovered);
    }

    @Override
    public Optional<byte[]> put(String key, byte[] value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");

        Optional<byte[]> previous;
        List<SSTableReader> sstablesSnapshot = null;
        FlushJob flushJob;
        long seq;

        stateLock.writeLock().lock();
        try {
            Optional<StoredEntry> hit = lookupMemTablesLocked(key);
            if (hit.isPresent()) {
                previous = toValue(hit.get());
            } else {
                previous = Optional.empty();
                sstablesSnapshot = sstables;
            }

            try {
                seq = wal.appendPut(key, value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            active.put(key, value, seq);
            flushJob = maybeBeginFreezeLocked();
            if (sstablesSnapshot != null) {
                acquireAll(sstablesSnapshot);
            }
        } finally {
            stateLock.writeLock().unlock();
        }

        if (sstablesSnapshot != null) {
            try {
                previous = scanSstables(sstablesSnapshot, key);
            } finally {
                releaseAll(sstablesSnapshot);
            }
        }
        if (flushJob != null) {
            completeFlush(flushJob);
        }
        notifyReplicationListeners(new WalRecord.Put(seq, key, value));
        return previous;
    }

    @Override
    public Optional<byte[]> get(String key) {
        Objects.requireNonNull(key, "key must not be null");

        List<SSTableReader> sstablesSnapshot;
        stateLock.readLock().lock();
        try {
            Optional<StoredEntry> hit = lookupMemTablesLocked(key);
            if (hit.isPresent()) {
                return toValue(hit.get());
            }
            sstablesSnapshot = sstables;
            acquireAll(sstablesSnapshot);
        } finally {
            stateLock.readLock().unlock();
        }
        try {
            return scanSstables(sstablesSnapshot, key);
        } finally {
            releaseAll(sstablesSnapshot);
        }
    }

    @Override
    public Optional<byte[]> delete(String key) {
        Objects.requireNonNull(key, "key must not be null");

        Optional<byte[]> previous;
        List<SSTableReader> sstablesSnapshot = null;
        FlushJob flushJob;
        long seq;

        stateLock.writeLock().lock();
        try {
            Optional<StoredEntry> hit = lookupMemTablesLocked(key);
            if (hit.isPresent()) {
                previous = toValue(hit.get());
            } else {
                previous = Optional.empty();
                sstablesSnapshot = sstables;
            }

            try {
                seq = wal.appendDelete(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            active.delete(key, seq);
            flushJob = maybeBeginFreezeLocked();
            if (sstablesSnapshot != null) {
                acquireAll(sstablesSnapshot);
            }
        } finally {
            stateLock.writeLock().unlock();
        }

        if (sstablesSnapshot != null) {
            try {
                previous = scanSstables(sstablesSnapshot, key);
            } finally {
                releaseAll(sstablesSnapshot);
            }
        }
        if (flushJob != null) {
            completeFlush(flushJob);
        }
        notifyReplicationListeners(new WalRecord.Delete(seq, key));
        return previous;
    }

    /**
     * Applies one already-numbered WAL record — a replication follower's
     * apply path (Phase 9): unlike {@link #put}/{@link #delete}, which
     * assign a fresh sequence number from this store's own WAL, this uses
     * exactly the sequence number {@code record} already carries, because it
     * came from a leader's WAL and must be applied under the leader's
     * numbering and order, never this store's own.
     *
     * <p>Same locking discipline as {@code put}/{@code delete}: the WAL
     * append and the {@code active} MemTable mutation happen together under
     * {@link #stateLock}'s write lock; a triggered flush's disk I/O still
     * happens afterward, lock-free. {@link ReplicationOutcome#ALREADY_APPLIED}
     * and {@link ReplicationOutcome#GAP_DETECTED} never touch {@code active}
     * at all — only {@link ReplicationOutcome#APPLIED} does.
     *
     * @return what happened — see {@link ReplicationOutcome}
     */
    public ReplicationOutcome applyReplicated(WalRecord record) {
        Objects.requireNonNull(record, "record must not be null");

        ReplicationOutcome outcome;
        FlushJob flushJob = null;

        stateLock.writeLock().lock();
        try {
            try {
                outcome = wal.appendReplicated(record);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (outcome == ReplicationOutcome.APPLIED) {
                switch (record) {
                    case WalRecord.Put put -> active.put(put.key(), put.value(), put.sequenceNumber());
                    case WalRecord.Delete delete -> active.delete(delete.key(), delete.sequenceNumber());
                }
                flushJob = maybeBeginFreezeLocked();
            }
        } finally {
            stateLock.writeLock().unlock();
        }

        if (flushJob != null) {
            completeFlush(flushJob);
        }
        return outcome;
    }

    /**
     * One past the sequence number of the last WAL record this store has
     * actually applied (locally written, or, as a replica, applied from a
     * leader) — equivalently, the sequence number a leader should start
     * streaming from to bring this store fully caught up. Phase 9's
     * follower catch-up protocol is built directly on this number.
     */
    public long lastAppliedSequenceNumber() {
        return wal.nextSequenceNumber();
    }

    /**
     * Every record currently in this store's WAL — a pass-through to
     * {@link WriteAheadLog#currentRecords()}, added for the same
     * replication-catch-up reason. Does not include anything already
     * flushed to an SSTable; see that method's Javadoc for the boundary
     * this implies.
     */
    public List<WalRecord> currentWalRecords() {
        try {
            return wal.currentRecords();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Bulk-loads {@code keyValues} as a single new SSTable stamped with
     * {@code watermark}, then raises the WAL's next sequence number past it
     * — Phase 10's bootstrap path for a brand-new or far-behind node that a
     * {@code ReplicationFollower} alone can't catch up (its source's WAL no
     * longer holds far-enough-back history; see {@code currentWalRecords()}'s
     * Javadoc). After this call, {@link #lastAppliedSequenceNumber()} is
     * exactly {@code watermark + 1}, so a {@code ReplicationFollower}
     * started immediately afterward resumes from precisely where this
     * snapshot left off — no gap, no re-fetching what was just loaded.
     *
     * <p><b>Crash safety comes from reusing {@link SSTableWriter#write}
     * unchanged</b>: the same temp-file-then-{@code force()}-then-atomic-rename
     * sequence a normal flush uses. A crash any time before this method
     * returns leaves no partial SSTable visible — either the rename
     * completed and the whole snapshot is durably there, or it didn't and
     * nothing changed. There is deliberately no partial/incremental variant:
     * a caller must have the <em>entire</em> snapshot in hand (fully
     * received from its network source) before calling this at all — see
     * {@code SnapshotClient}, which never calls this with a partially
     * received transfer.
     *
     * @throws IllegalStateException if this store is not currently empty —
     *         bootstrap is only meaningful for a brand-new node; loading a
     *         snapshot on top of existing data would silently discard it
     */
    public void loadSnapshot(Map<String, byte[]> keyValues, long watermark) {
        Objects.requireNonNull(keyValues, "keyValues must not be null");
        if (watermark < 0) {
            throw new IllegalArgumentException("watermark must not be negative");
        }

        stateLock.writeLock().lock();
        try {
            if (!active.isEmpty() || frozen != null || !sstables.isEmpty()) {
                throw new IllegalStateException("loadSnapshot requires an empty store — this one already has data");
            }
            if (!keyValues.isEmpty()) {
                SortedMap<String, StoredEntry> entries = new TreeMap<>();
                for (Map.Entry<String, byte[]> entry : keyValues.entrySet()) {
                    entries.put(entry.getKey(), new StoredEntry.Value(entry.getValue()));
                }
                Path finalFile = dataDirectory.resolve(sstableFileName(watermark));
                try {
                    SSTableWriter.write(dataDirectory.resolve(FLUSH_TEMP_FILE_NAME), finalFile, entries, watermark);
                    sstables = List.of(SSTableReader.open(finalFile));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            wal.ensureNextSequenceNumberAtLeast(watermark + 1);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Registers {@code listener} to be called with every record this store
     * durably applies from now on (not retroactively — a new listener does
     * not get replayed history; pair this with {@link #lastAppliedSequenceNumber()}
     * to know where "now" started). Intended for one purpose: a replication
     * component on a leader forwarding its write stream to followers.
     */
    public void addReplicationListener(Consumer<WalRecord> listener) {
        replicationListeners.add(Objects.requireNonNull(listener, "listener must not be null"));
    }

    public void removeReplicationListener(Consumer<WalRecord> listener) {
        replicationListeners.remove(listener);
    }

    private void notifyReplicationListeners(WalRecord record) {
        for (Consumer<WalRecord> listener : replicationListeners) {
            try {
                listener.accept(record);
            } catch (RuntimeException e) {
                log.warn("replication listener threw while notified of {}; the write itself already succeeded", record, e);
            }
        }
    }

    /** Must be called only while holding {@link #stateLock} (either lock mode). */
    private Optional<StoredEntry> lookupMemTablesLocked(String key) {
        Optional<StoredEntry> hit = active.get(key);
        if (hit.isPresent()) {
            return hit;
        }
        if (frozen != null) {
            return frozen.get(key);
        }
        return Optional.empty();
    }

    private static Optional<byte[]> toValue(StoredEntry entry) {
        return switch (entry) {
            case StoredEntry.Value v -> Optional.of(v.bytes());
            case StoredEntry.Tombstone t -> Optional.empty();
        };
    }

    /** Lock-free: scans a fixed snapshot taken earlier under {@link #stateLock}. */
    private static Optional<byte[]> scanSstables(List<SSTableReader> readers, String key) {
        for (SSTableReader reader : readers) {
            Optional<StoredEntry> hit;
            try {
                hit = reader.get(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (hit.isPresent()) {
                return toValue(hit.get());
            }
        }
        return Optional.empty();
    }

    /**
     * Every key currently live (not deleted) in this store — added in Phase
     * 7 for partition rebalancing, which needs to enumerate a node's actual
     * stored keys rather than only ever looking one up by name. Exposed
     * beyond {@link KeyValueStore} the same way {@link #flush()} is.
     *
     * <p>Snapshots {@code active}/{@code frozen}/{@code sstables} together
     * under {@link #stateLock}'s read lock (the same discipline {@link #get}
     * uses), then resolves each key against whichever generation is newest
     * for it — {@code active}, then {@code frozen}, then each SSTable
     * newest-to-oldest. A key already seen in a newer generation is never
     * reconsidered from an older one, so a value that was later overwritten
     * or deleted is never returned as if it still were live.
     *
     * <p><b>{@code active}/{@code frozen} are collected while still holding
     * the read lock; only the SSTable scan runs lock-free afterward.</b> An
     * earlier version collected all three lock-free, on the same "snapshot
     * the reference under lock, read from it after unlocking" pattern
     * {@link #get} safely uses for {@code sstables} — safe there because an
     * {@link SSTableReader} is immutable once created, but genuinely unsafe
     * for {@code active}, which is exactly the {@link MemTable} concurrent
     * {@code put}/{@code delete} calls keep mutating. Reading a {@code
     * TreeMap} while another thread concurrently inserts into it is
     * undefined behavior in general and threw a real, reproducible {@code
     * ConcurrentModificationException} under sustained concurrent load — see
     * PROGRESS.md's Phase 12 section for how this was found and confirmed
     * fixed. {@code frozen}, like {@code active}, is included in this same
     * locked step even though it happens to already be immutable once set
     * (never written to again) — doing so keeps the two MemTable tiers
     * handled identically rather than relying on a fact about one of them
     * that isn't true of the other.
     *
     * <p>Not cheap: unlike a point lookup, this reads every SSTable in full
     * ({@link SSTableReader#scanAll()}) rather than stopping early. Meant for
     * administrative/rebalancing use, not the request hot path.
     */
    public Set<String> keys() {
        Set<String> seen = new HashSet<>();
        Set<String> liveKeys = new LinkedHashSet<>();
        List<SSTableReader> sstablesSnapshot;

        stateLock.readLock().lock();
        try {
            collectLiveKeys(active.entries(), seen, liveKeys);
            if (frozen != null) {
                collectLiveKeys(frozen.entries(), seen, liveKeys);
            }
            sstablesSnapshot = sstables;
            acquireAll(sstablesSnapshot);
        } finally {
            stateLock.readLock().unlock();
        }

        try {
            for (SSTableReader reader : sstablesSnapshot) {
                List<Map.Entry<String, StoredEntry>> records;
                try {
                    records = reader.scanAll();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                for (Map.Entry<String, StoredEntry> entry : records) {
                    if (seen.add(entry.getKey()) && entry.getValue() instanceof StoredEntry.Value) {
                        liveKeys.add(entry.getKey());
                    }
                }
            }
        } finally {
            releaseAll(sstablesSnapshot);
        }
        return liveKeys;
    }

    private static void acquireAll(List<SSTableReader> readers) {
        for (SSTableReader reader : readers) {
            reader.acquire();
        }
    }

    private static void releaseAll(List<SSTableReader> readers) {
        for (SSTableReader reader : readers) {
            reader.release();
        }
    }

    private static void collectLiveKeys(Map<String, StoredEntry> generation, Set<String> seen, Set<String> liveKeys) {
        for (Map.Entry<String, StoredEntry> entry : generation.entrySet()) {
            if (seen.add(entry.getKey()) && entry.getValue() instanceof StoredEntry.Value) {
                liveKeys.add(entry.getKey());
            }
        }
    }

    /**
     * Explicitly triggers a flush, bypassing the size threshold, so tests can
     * force one deterministically. A no-op if a flush is already in progress
     * or there is nothing to flush.
     */
    public void flush() {
        FlushJob job;
        stateLock.writeLock().lock();
        try {
            job = beginFreezeLocked();
        } finally {
            stateLock.writeLock().unlock();
        }
        if (job != null) {
            completeFlush(job);
        }
    }

    /** Must be called only while holding {@link #stateLock}'s write lock. */
    private FlushJob maybeBeginFreezeLocked() {
        if (active.approximateSizeInBytes() < flushThresholdBytes) {
            return null;
        }
        return beginFreezeLocked();
    }

    /** Must be called only while holding {@link #stateLock}'s write lock. */
    private FlushJob beginFreezeLocked() {
        if (frozen != null || active.isEmpty()) {
            return null; // a flush is already in progress, or nothing to flush; defer
        }
        MemTable toFlush = active;
        frozen = toFlush;
        active = new MemTable();
        return new FlushJob(toFlush, toFlush.maxSequenceNumber());
    }

    /**
     * Runs the slow part of a flush: entirely lock-free except for the final,
     * brief registration step. See the class Javadoc for why this ordering —
     * write, force, rename, truncate WAL, only then register — is what makes
     * flush crash-safe and never blocks concurrent readers/writers on disk I/O.
     */
    private void completeFlush(FlushJob job) {
        int sstableCountAfterFlush;
        try {
            Path tempFile = dataDirectory.resolve(FLUSH_TEMP_FILE_NAME);
            Path finalFile = dataDirectory.resolve(sstableFileName(job.watermark()));
            SSTableWriter.write(tempFile, finalFile, job.memTableToFlush().entries(), job.watermark());

            wal.truncateUpTo(job.watermark());

            SSTableReader newReader = SSTableReader.open(finalFile);
            stateLock.writeLock().lock();
            try {
                List<SSTableReader> updated = new ArrayList<>(sstables.size() + 1);
                updated.add(newReader);
                updated.addAll(sstables);
                sstables = List.copyOf(updated);
                sstableCountAfterFlush = sstables.size();
                frozen = null;
            } finally {
                stateLock.writeLock().unlock();
            }
        } catch (IOException e) {
            // frozen deliberately stays set: a failed flush is a serious, propagated
            // error, and retrying automatically is out of scope for this phase (see
            // PROGRESS.md limitations) — this instance will not attempt another flush
            // until restarted, but no data is at risk: active/frozen/WAL remain fully
            // consistent, and a fresh restart's recovery can flush again from scratch.
            throw new UncheckedIOException(e);
        }

        if (sstableCountAfterFlush >= compactionTriggerCount && compactionInProgress.compareAndSet(false, true)) {
            try {
                compactNow();
            } finally {
                compactionInProgress.set(false);
            }
        }
    }

    /**
     * Phase 13: merges every currently-live SSTable into one. Runs
     * synchronously on the calling thread (the thread that just completed a
     * flush) — same "inline but off {@link #stateLock} for the slow part"
     * shape as {@link #completeFlush} itself. Only ever entered with
     * {@link #compactionInProgress} already held, so at most one compaction
     * runs at a time; {@code stateLock} still separately protects
     * {@link #sstables} against concurrent readers/writers, exactly as it
     * does for a flush.
     *
     * <p>Because this always compacts <em>every</em> live table, it is
     * always safe to drop tombstones with no surviving older generation
     * left to shadow — see {@link Compactor}'s Javadoc.
     *
     * <p>The merge/read phase ({@link Compactor#compact}, which calls
     * {@link SSTableReader#scanAll()} on each input) runs lock-free, on a
     * snapshot acquired the same way {@link #get} protects its own
     * lock-free scan: {@code acquire()} each input reader while still
     * holding {@code stateLock}'s read lock, then {@code release()} them
     * once the merge is done. Only the final swap — installing the merged
     * reader (if any) and retiring the compacted-away inputs — takes the
     * write lock, briefly.
     *
     * <p>A flush can complete concurrently with this method's lock-free
     * merge phase (nothing stops it — flushes are never blocked on
     * compaction). Any such newly-added reader appears at the <em>front</em>
     * of {@code sstables} by the time the swap runs (flushes always
     * prepend), so the swap identifies exactly which prefix of the current
     * list is "new since this compaction's snapshot was taken" and keeps
     * it untouched, replacing only the (still-contiguous, still
     * same-relative-order) suffix this compaction actually read.
     */
    private void compactNow() {
        List<SSTableReader> snapshot;
        stateLock.readLock().lock();
        try {
            snapshot = sstables;
            acquireAll(snapshot);
        } finally {
            stateLock.readLock().unlock();
        }

        Optional<Path> writtenFile;
        try {
            Path tempFile = dataDirectory.resolve(COMPACTION_TEMP_FILE_NAME);
            Path finalFile = dataDirectory.resolve(compactedSstableFileName());
            try {
                writtenFile = Compactor.compact(snapshot, tempFile, finalFile, true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } finally {
            releaseAll(snapshot);
        }

        SSTableReader mergedReader = null;
        boolean swapped = false;
        try {
            if (writtenFile.isPresent()) {
                mergedReader = SSTableReader.open(writtenFile.get());
            }

            stateLock.writeLock().lock();
            try {
                List<SSTableReader> current = sstables;
                int newlyAddedCount = current.size() - snapshot.size();
                List<SSTableReader> newlyAdded = current.subList(0, newlyAddedCount);

                List<SSTableReader> updated = new ArrayList<>(newlyAdded.size() + 1);
                updated.addAll(newlyAdded);
                if (mergedReader != null) {
                    updated.add(mergedReader);
                }
                sstables = List.copyOf(updated);
            } finally {
                stateLock.writeLock().unlock();
            }
            swapped = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // Old inputs are only ever retired (closed + deleted) once the swap
            // above has actually published their replacement (or published the
            // legitimate "nothing survived" empty state) into `sstables` — never
            // on a failure path. Retiring them on a failed swap would delete the
            // only copy of data that a broken or unopenable merged file was
            // supposed to replace; this ordering is the one genuine correctness
            // requirement compaction adds beyond what flush already guarantees.
            if (swapped) {
                for (SSTableReader oldReader : snapshot) {
                    oldReader.retire();
                }
            } else if (mergedReader != null) {
                // Opened successfully but the swap never completed — never
                // published, so retire it here to avoid leaking the file; the
                // old inputs are untouched and remain the live, correct data.
                mergedReader.retire();
            }
        }
    }

    private String compactedSstableFileName() {
        // "-c" never appears in a flush-produced name (see `sstableFileName`),
        // so a compacted output's filename can never collide with one, and a
        // flush and a compaction can never race to create the same path.
        return String.format("sstable-c%019d-%d.sst", System.currentTimeMillis(), compactionCounter.incrementAndGet());
    }

    /**
     * Explicitly triggers a full compaction, bypassing the SSTable-count
     * threshold, so tests can force one deterministically. A no-op if a
     * compaction is already in progress or there are fewer than two
     * SSTables (nothing meaningful to merge).
     */
    public void compact() {
        if (sstables.size() < 2) {
            return;
        }
        if (compactionInProgress.compareAndSet(false, true)) {
            try {
                compactNow();
            } finally {
                compactionInProgress.set(false);
            }
        }
    }

    @Override
    public void close() throws IOException {
        stateLock.writeLock().lock();
        try {
            IOException failure = null;
            try {
                wal.close();
            } catch (IOException e) {
                failure = e;
            }
            for (SSTableReader reader : sstables) {
                try {
                    reader.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private static String sstableFileName(long maxSequenceNumber) {
        return String.format("sstable-%019d.sst", maxSequenceNumber);
    }

    private static List<SSTableReader> discoverSSTables(Path dataDirectory) throws IOException {
        List<SSTableReader> readers = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDirectory, "sstable-*.sst")) {
            for (Path path : stream) {
                readers.add(SSTableReader.open(path));
            }
        } catch (IOException | RuntimeException e) {
            closeAll(readers, e);
            throw e;
        }
        readers.sort(Comparator.comparingLong(SSTableReader::maxSequenceNumber).reversed());
        return readers;
    }

    private static void closeAll(List<SSTableReader> readers, Exception primary) {
        for (SSTableReader reader : readers) {
            try {
                reader.close();
            } catch (IOException suppressed) {
                primary.addSuppressed(suppressed);
            }
        }
    }

    /** The MemTable being flushed, and the WAL watermark it represents. */
    private record FlushJob(MemTable memTableToFlush, long watermark) {
    }

    /**
     * A point-in-time snapshot of this store's own storage-engine metrics —
     * the Phase 14 "final hardening" observability addition: real numbers
     * this store already tracks internally, exposed for external inspection
     * (see {@code docs/OPERATIONS.md}) rather than left only as private
     * fields useful solely to this class's own logic.
     */
    public record StoreStatus(
            int sstableCount,
            long totalSstableBytes,
            long activeMemTableSizeBytes,
            boolean flushInProgress,
            long lastAppliedSequenceNumber) {
    }

    /** See {@link StoreStatus}. Cheap: sums already-known file sizes, does not touch disk. */
    public StoreStatus status() {
        stateLock.readLock().lock();
        try {
            long totalSstableBytes = 0;
            for (SSTableReader reader : sstables) {
                totalSstableBytes += reader.fileSizeBytes();
            }
            return new StoreStatus(sstables.size(), totalSstableBytes, active.approximateSizeInBytes(),
                    frozen != null, wal.nextSequenceNumber());
        } finally {
            stateLock.readLock().unlock();
        }
    }
}
