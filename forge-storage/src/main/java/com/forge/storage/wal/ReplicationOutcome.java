package com.forge.storage.wal;

/** What happened when a replicated record (one carrying its own, externally-assigned sequence number) was offered to a WAL. */
public enum ReplicationOutcome {
    /** The record's sequence number was exactly the next expected one; it was appended and forced to disk. */
    APPLIED,
    /**
     * The record's sequence number is less than the next expected one — this
     * exact entry (or a later one covering it) was already durably applied.
     * Applying it again would violate the WAL's strictly-increasing sequence
     * invariant, so it's safely ignored instead: replication's idempotency
     * guarantee for duplicate or replayed entries.
     */
    ALREADY_APPLIED,
    /**
     * The record's sequence number is greater than the next expected one —
     * one or more earlier entries are missing. Applying it anyway would
     * create a permanent gap the recovery scan could never fill in, so it's
     * refused; the caller must catch up from the next expected sequence
     * number first (see {@code ConcurrentLsmKeyValueStore#lastAppliedSequenceNumber()}).
     */
    GAP_DETECTED
}
