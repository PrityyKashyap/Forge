package com.forge.cluster.recovery;

import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Phase 15: rebuilds a node's local store from scratch via a full snapshot,
 * for the one case Phase 9's ordinary WAL-based catch-up cannot safely
 * cover — a node whose <em>own local data</em> may have diverged from the
 * cluster's authoritative history, not merely fallen behind it.
 *
 * <h2>When this is needed, precisely</h2>
 * {@code com.forge.cluster.leadership.SequenceEpochs}' term-banded sequence
 * numbering makes cross-term replication ordering safe, but it does not —
 * and cannot — erase locally-applied writes a node accepted while it was
 * (or believed itself to be) leader, then never replicated before a term
 * change moved on without them. Those keys' values remain visible to a
 * local {@code GET} on that node indefinitely; incremental
 * {@code ReplicationFollower} catch-up only ever appends forward from a
 * sequence number, it has no way to retroactively correct a value already
 * applied under an abandoned term. A rejoining node whose own data implies
 * an older term than the cluster's current one is therefore treated,
 * conservatively, as potentially divergent and fully re-synced here —
 * discarding its local state — rather than risk ever silently serving it.
 *
 * <p>This is a deliberate, disclosed simplification: a plain follower that
 * never originated a write (never was leader) never actually diverges, so
 * always resyncing on every observed term change is more conservative
 * (and more expensive) than strictly necessary. Distinguishing "was I ever
 * leader in the old term" would need persistent state Phase 14 does not
 * keep (see {@code RaftNode}'s Javadoc) — so this phase accepts the extra
 * cost of an occasionally-unnecessary full resync in exchange for a
 * genuinely simple, easy-to-verify safety argument. See PROGRESS.md's
 * Phase 15 known limitations.
 *
 * <h2>What this does</h2>
 * Closes the stale store, deletes its entire on-disk data directory, opens
 * a brand-new empty store at the same path, and loads a complete snapshot
 * from a live {@link SnapshotServer} via {@link SnapshotClient} — reusing
 * Phase 10's transfer mechanism, and its own crash-safety guarantee
 * unchanged: if the transfer fails partway, nothing is loaded and the
 * caller may simply retry (see {@link SnapshotClient}'s Javadoc). The
 * <em>directory deletion</em> step, unlike the snapshot transfer, is not
 * itself crash-atomic — a crash between deleting the old directory and
 * completing the new snapshot load would leave this node with no usable
 * local data at all, recoverable only by retrying this same resync once
 * the node is back up. Stated plainly as this method's one real limitation
 * beyond what {@link SnapshotClient} already discloses.
 */
public final class StaleReplicaRecovery {

    private static final Logger log = LoggerFactory.getLogger(StaleReplicaRecovery.class);

    private StaleReplicaRecovery() {
    }

    /**
     * @param staleStore the node's current store; closed by this call regardless of outcome
     * @param dataDirectory the same directory {@code staleStore} was opened on — its entire
     *                      contents are deleted
     * @param flushThresholdBytes the flush threshold for the freshly (re)opened store
     * @return a brand-new store at {@code dataDirectory}, already fully loaded from the snapshot
     * @throws IOException if closing, wiping, reopening, or the snapshot transfer itself fails —
     *                      in every failure case, {@code dataDirectory} is left either fully wiped
     *                      (safe to retry from scratch) or, if the wipe itself failed, unchanged
     */
    public static ConcurrentLsmKeyValueStore resyncFromSnapshot(ConcurrentLsmKeyValueStore staleStore,
            Path dataDirectory, long flushThresholdBytes, String snapshotHost, int snapshotPort) throws IOException {
        Objects.requireNonNull(staleStore, "staleStore must not be null");
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(snapshotHost, "snapshotHost must not be null");

        try {
            staleStore.close();
        } catch (IOException e) {
            log.warn("failed to cleanly close the stale store at {} before resync; continuing anyway", dataDirectory, e);
        }

        deleteDirectoryContents(dataDirectory);
        log.info("wiped local data at {} ahead of a full snapshot resync from {}:{}", dataDirectory, snapshotHost, snapshotPort);

        ConcurrentLsmKeyValueStore fresh = new ConcurrentLsmKeyValueStore(dataDirectory, flushThresholdBytes);
        try {
            SnapshotClient.fetchAndLoad(snapshotHost, snapshotPort, fresh);
        } catch (IOException e) {
            fresh.close();
            throw e;
        }
        log.info("resync complete: {} now holds {} keys", dataDirectory, fresh.keys().size());
        return fresh;
    }

    private static void deleteDirectoryContents(Path dataDirectory) throws IOException {
        if (!Files.exists(dataDirectory)) {
            Files.createDirectories(dataDirectory);
            return;
        }
        java.util.List<Path> toDelete;
        try (Stream<Path> walk = Files.walk(dataDirectory)) {
            toDelete = walk.filter(p -> !p.equals(dataDirectory)).sorted(Comparator.reverseOrder()).toList();
        }
        for (Path path : toDelete) {
            Files.delete(path);
        }
    }
}
