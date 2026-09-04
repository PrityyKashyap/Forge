package com.forge.cluster;

import com.forge.storage.ConcurrentLsmKeyValueStore;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Moves data between nodes after a {@link ClusterTopology} change — the
 * "partition movement support" DESIGN.md's Phase 7 contract calls for.
 *
 * <p>There is no replication yet (that's Phase 9), so this is a direct
 * copy from the losing node to the gaining node, not a replica-based
 * bootstrap — an explicit, honest scope boundary for a first partitioning
 * phase; see PROGRESS.md.
 *
 * <p><b>Per-key safety, not whole-migration atomicity</b>: each key is
 * written to its new owner and only then deleted from the old one, so a
 * crash or network failure mid-migration can leave a key temporarily
 * duplicated on both nodes — never lost. A failed {@link #migrate} call can
 * simply be retried: every key it already finished moving is already gone
 * from the source (so a re-scan of {@link ConcurrentLsmKeyValueStore#keys()}
 * naturally won't revisit it), making the whole operation idempotent to
 * retry, even though it isn't a single atomic transaction.
 *
 * <p><b>Required call order — update ownership before migrating</b>:
 * {@code sourceNode}'s {@code ForgeServer} must already be rejecting (via
 * its ownership predicate) any key {@code newTopology} no longer assigns to
 * it <em>before</em> {@link #migrate} is called. If it isn't, a client could
 * still write a "soon to be foreign" key to the source in the gap between
 * this method's {@code keys()} snapshot and its completion, and that write
 * would silently not be included in this pass (only a subsequent {@code
 * migrate} call would catch it). This method has no way to enforce that
 * ordering itself — it's a precondition on how it's called, not something
 * it can check.
 */
public final class PartitionRebalancer {

    private PartitionRebalancer() {
    }

    public record MigrationResult(Set<String> keysMoved) {
    }

    /**
     * Moves every key {@code sourceStore} currently holds that {@code newTopology}
     * assigns to a different node than {@code sourceNode}, via {@code destinationRouter}
     * (expected to be a {@link PartitionedForgeClient} built from {@code newTopology},
     * so each moved key lands wherever the new topology actually says it belongs —
     * which need not be a single node if {@code sourceNode} is shedding keys to
     * several gainers at once).
     *
     * @throws IOException on the first failed network call; already-moved keys stay
     *                      moved (see class Javadoc on retrying)
     */
    public static MigrationResult migrate(NodeId sourceNode, ConcurrentLsmKeyValueStore sourceStore,
            ClusterTopology newTopology, PartitionedForgeClient destinationRouter) throws IOException {
        Set<String> moved = new LinkedHashSet<>();
        for (String key : sourceStore.keys()) {
            if (newTopology.ownerOf(key).equals(sourceNode)) {
                continue; // still ours under the new topology
            }
            Optional<byte[]> value = sourceStore.get(key);
            if (value.isEmpty()) {
                continue; // deleted locally between the keys() snapshot and this get() -- nothing left to move
            }
            destinationRouter.put(key, value.get()); // written to the new owner before...
            sourceStore.delete(key);                  // ...it's removed from here — never the other order
            moved.add(key);
        }
        return new MigrationResult(moved);
    }
}
