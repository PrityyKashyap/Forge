package com.forge.cluster.membership;

/**
 * A node's liveness state as seen by one {@link FailureDetector} instance.
 * Per DESIGN.md's Phase 8 contract, this view is <b>local and eventually
 * consistent</b> — different nodes running their own detector may
 * transiently disagree about the same peer's state; there is no
 * cluster-wide agreement protocol here (that would be a consensus concern,
 * Phase 14).
 */
public enum NodeState {
    /** A heartbeat was received within {@code suspectTimeout}. */
    ALIVE,
    /** No heartbeat for at least {@code suspectTimeout} but less than {@code deadTimeout}. */
    SUSPECT,
    /** No heartbeat for at least {@code deadTimeout}. */
    DEAD
}
