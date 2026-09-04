package com.forge.cluster.leadership;

/**
 * Phase 15: maps a Raft term to a disjoint "band" of WAL sequence numbers,
 * so that writes originated by leaders of different terms can never
 * collide in sequence-number space.
 *
 * <h2>The problem this solves</h2>
 * Phase 9's replication has no epoch/term concept at all — it is a flat,
 * ever-increasing per-store sequence number space. Consider: leader A has
 * locally applied through sequence 100, but only replicated through 98
 * before crashing. Follower B (at 98) is elected the new leader and, using
 * Phase 9's ordinary {@code put()} path unchanged, would next assign
 * sequence <b>99</b> to its first new write — colliding with A's own,
 * different, never-replicated write that already used sequence 99. Two
 * different logical writes would then share one sequence number, an
 * ambiguity nothing downstream (recovery, replication catch-up) can
 * resolve correctly.
 *
 * <h2>The fix, and why it needs no new wire/log format</h2>
 * Every term T is assigned the sequence range {@code [bandStart(T), bandStart(T+1))}.
 * The instant a node is confirmed as leader for term T (see
 * {@code PartitionLeadership}), it ratchets its own store's next-sequence-number
 * forward to at least {@code bandStart(T)} via
 * {@link com.forge.storage.ConcurrentLsmKeyValueStore#ensureNextSequenceNumberAtLeast}
 * — a pure ratchet (never decreases), already used since Phase 3/10 to align a
 * store's numbering with a discovered watermark or loaded snapshot. Every
 * node that learns "the current leader is for term T" (leader or follower)
 * performs the exact same ratchet before participating in that term's
 * replication stream, so every replica derives the identical band boundary
 * from the one value Raft already guarantees is agreed-upon: the term.
 * Because term always increases and bands never overlap, a write from an
 * old, superseded term can never be mistaken for — or collide with — a
 * write from a newer one; {@code WriteAheadLog.appendReplicated}'s existing,
 * unmodified gap-detection logic (reject anything at or below the current
 * next-sequence-number as already-applied) already does exactly the right
 * thing once both sides have independently ratcheted into the new band.
 * No change to {@code WalRecord}, {@code ReplicationWireFormat}, or
 * {@code ConcurrentLsmKeyValueStore.applyReplicated} was needed.
 *
 * <h2>What this does <em>not</em> fix, stated plainly</h2>
 * A′s own two writes at (old) sequence 99 and 100 — the ones it never
 * replicated before crashing — are not erased from A's local key-value
 * data by this scheme; only the WAL's own future numbering moves on. If A
 * later rejoins, its local store can still hold values a majority of the
 * cluster never saw. This is exactly why a rejoining node whose own
 * implied term is behind the cluster's current term performs a full
 * snapshot resync (see {@code StaleReplicaRecovery}) rather than trying to
 * incrementally reconcile — discarding any such orphaned local state
 * rather than risk silently serving it forever. See
 * {@code docs/FAILURE_MODEL.md} and {@code docs/CONSISTENCY.md}.
 *
 * <h2>Sizing</h2>
 * {@code BAND_SIZE = 1,000,000,000} — comfortably larger than any realistic
 * number of writes a single term would see in this project's tests/demos,
 * while leaving room for roughly 9.2 billion terms before a 64-bit sequence
 * number could overflow. An unbenchmarked, deliberately generous constant,
 * same disclosure as {@code ConcurrentLsmKeyValueStore}'s own
 * {@code DEFAULT_FLUSH_THRESHOLD_BYTES}.
 */
public final class SequenceEpochs {

    public static final long BAND_SIZE = 1_000_000_000L;

    private SequenceEpochs() {
    }

    /**
     * The first sequence number term {@code term} is allowed to use.
     * Saturates at {@link Long#MAX_VALUE} rather than overflowing into a
     * negative number for a term so large it would never occur in practice
     * (see class Javadoc's sizing note) — a defensive ceiling, not an
     * expected code path.
     */
    public static long bandStart(long term) {
        if (term < 0) {
            throw new IllegalArgumentException("term must not be negative: " + term);
        }
        if (term > Long.MAX_VALUE / BAND_SIZE) {
            return Long.MAX_VALUE;
        }
        return term * BAND_SIZE;
    }

    /** The term whose band contains {@code sequenceNumber} — the inverse of {@link #bandStart}. */
    public static long impliedTerm(long sequenceNumber) {
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("sequenceNumber must not be negative: " + sequenceNumber);
        }
        return sequenceNumber / BAND_SIZE;
    }
}
