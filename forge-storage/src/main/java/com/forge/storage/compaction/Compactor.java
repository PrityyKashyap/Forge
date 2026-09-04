package com.forge.storage.compaction;

import com.forge.storage.memtable.StoredEntry;
import com.forge.storage.sstable.SSTableReader;
import com.forge.storage.sstable.SSTableWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Phase 13: merges several SSTables into one, keeping only the newest
 * surviving version of each key. A stateless utility, same shape as
 * {@link SSTableWriter} — takes already-open readers and output paths, does
 * the merge, and reuses {@link SSTableWriter#write} unchanged for the
 * output, so the result inherits the exact same crash-safety guarantee
 * (temp file, {@code force(true)}, atomic rename) that every other durable
 * file in this project already has.
 *
 * <h2>Strategy: full, size-triggered compaction — not leveled</h2>
 * {@link com.forge.storage.ConcurrentLsmKeyValueStore} always compacts
 * <em>every</em> currently-live SSTable into exactly one new table, once
 * the table count crosses a threshold — not a leveled scheme with
 * partial/tiered merges. Chosen for this codebase's current scale: it's the
 * simplest strategy that is still genuinely correct and effective (it
 * bounds SSTable count and read amplification, and reclaims space from
 * overwritten/deleted keys), and — critically — it sidesteps a real
 * correctness hazard leveled/partial compaction would introduce: a
 * tombstone can only ever be safely dropped once every older generation
 * that might still hold a stale value for that key has also been folded
 * in, which is automatically true for a full compaction (there is no older
 * generation left outside it) and would otherwise need its own
 * "does an even older table survive this compaction?" bookkeeping. A
 * leveled scheme that avoids rewriting the whole dataset on every
 * compaction is a natural, larger follow-up — see PROGRESS.md's Phase 13
 * known limitations.
 *
 * @see com.forge.storage.ConcurrentLsmKeyValueStore for the trigger policy,
 *      the locking discipline around a compaction's lock-free merge/read
 *      phase, and how compacted-away input files are safely retired while
 *      the store stays live.
 */
public final class Compactor {

    private Compactor() {
    }

    /**
     * @param inputs      the tables to merge, newest-first (matching
     *                    {@code ConcurrentLsmKeyValueStore.sstables}'s own
     *                    ordering) — must not be concurrently retired while
     *                    this call is in flight; see the caller's own
     *                    acquire/release discipline
     * @param tempFile    scratch path for the merged output, same
     *                    convention as {@link SSTableWriter#write}
     * @param finalFile   the merged output's permanent path — must be a
     *                    filename never used by any existing SSTable (see
     *                    {@code ConcurrentLsmKeyValueStore}'s naming scheme
     *                    for compacted output, which never collides with a
     *                    flush-produced name)
     * @param dropTombstones whether a tombstone with no surviving older
     *                    generation behind it may be dropped entirely —
     *                    only ever safe when {@code inputs} is <b>every</b>
     *                    currently-live SSTable (a full compaction); see
     *                    this class's Javadoc
     * @return the path written to, or empty if the merge produced no live
     *         entries at all (every key present was tombstoned and
     *         {@code dropTombstones} was true) — in which case no file is
     *         written and the caller should simply drop all inputs with no
     *         replacement
     */
    public static Optional<Path> compact(List<SSTableReader> inputs, Path tempFile, Path finalFile,
            boolean dropTombstones) throws IOException {
        if (inputs.isEmpty()) {
            return Optional.empty();
        }

        TreeMap<String, StoredEntry> merged = new TreeMap<>();
        long maxSequenceNumber = 0;
        // Oldest to newest, so a plain map.put lets each newer generation's
        // entry naturally overwrite whatever an older one already placed —
        // the same "newest wins" rule ConcurrentLsmKeyValueStore.keys() and
        // scanSstables() already apply, just expressed as ordered inserts
        // instead of a "first one seen wins" scan.
        for (int i = inputs.size() - 1; i >= 0; i--) {
            SSTableReader reader = inputs.get(i);
            maxSequenceNumber = Math.max(maxSequenceNumber, reader.maxSequenceNumber());
            for (Map.Entry<String, StoredEntry> entry : reader.scanAll()) {
                merged.put(entry.getKey(), entry.getValue());
            }
        }

        if (dropTombstones) {
            Iterator<Map.Entry<String, StoredEntry>> it = merged.entrySet().iterator();
            while (it.hasNext()) {
                if (it.next().getValue() instanceof StoredEntry.Tombstone) {
                    it.remove();
                }
            }
        }

        if (merged.isEmpty()) {
            return Optional.empty();
        }
        SSTableWriter.write(tempFile, finalFile, merged, maxSequenceNumber);
        return Optional.of(finalFile);
    }
}
