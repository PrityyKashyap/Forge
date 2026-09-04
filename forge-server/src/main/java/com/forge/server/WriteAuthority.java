package com.forge.server;

import java.util.Optional;

/**
 * Phase 15: the fencing gate a {@link ForgeServer} consults before letting
 * a PUT/DELETE actually touch its store — separate from and additional to
 * Phase 7's {@code ownershipPredicate}, which answers a different question
 * ("do I serve this key's partition at all") than this answers ("am I
 * currently the fenced, Raft-confirmed leader allowed to accept writes for
 * it, right now").
 *
 * <p>Deliberately declared here in {@code forge-server}, not
 * {@code forge-cluster}, even though its only real implementation
 * ({@code com.forge.cluster.leadership.PartitionLeadership}) lives in
 * {@code forge-cluster} — {@code forge-cluster} already depends on
 * {@code forge-server}, so the dependency has to point this direction to
 * avoid a cycle. This is the same reason {@link java.util.function.Predicate}
 * (not a {@code forge-cluster} type) is what {@code ForgeServer}'s existing
 * ownership gate already uses.
 *
 * <p>{@link #NONE} is the default every pre-Phase-15 constructor implicitly
 * uses — always allows writes, so a caller that never heard of consensus
 * gets byte-for-byte the same behavior as before this interface existed.
 */
public interface WriteAuthority {

    /**
     * True if a write should be allowed to reach the store right now. Must
     * be safe to call on every single PUT/DELETE (cheap, no blocking I/O) —
     * {@code ConnectionHandler} calls this synchronously on the request
     * path, not just once at connection setup.
     */
    boolean canAcceptWrites();

    /** This node's current understanding of the leadership term/epoch, for a rejection's diagnostic message. */
    long currentTerm();

    /** A human-readable hint at who the current leader is, if known — {@code Optional.empty()} if not. */
    Optional<String> currentLeaderHint();

    /** Always allows writes — the pre-Phase-15 behavior, and the default for every constructor that doesn't ask for fencing. */
    WriteAuthority NONE = new WriteAuthority() {
        @Override
        public boolean canAcceptWrites() {
            return true;
        }

        @Override
        public long currentTerm() {
            return 0;
        }

        @Override
        public Optional<String> currentLeaderHint() {
            return Optional.empty();
        }
    };
}
