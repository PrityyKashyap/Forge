package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;

import java.io.IOException;

/**
 * The one seam through which {@link RaftNode} — deliberately I/O-free and
 * synchronously testable with a fake {@link java.time.Clock}, per its class
 * Javadoc — durably persists {@code currentTerm}/{@code votedFor} without
 * doing any file I/O itself. {@link RaftNode} calls {@link
 * #onPersistentStateChanged} synchronously, <em>before</em> mutating its own
 * in-memory fields, every time {@code currentTerm} or {@code votedFor}
 * changes; if it throws, the in-memory fields are left completely untouched
 * and the exception propagates out of whatever {@code RaftNode} method
 * triggered the change (see that method's Javadoc for what its caller does
 * with the failure — uniformly: treat it exactly like a dropped RPC, never
 * fabricate success).
 *
 * <p>Tests use {@link #NONE} (matching every pre-existing {@code
 * RaftNodeTest} case — no persistence, no behavior change). Production
 * wiring is {@link RaftPersistentState#save}, whose signature already
 * matches this interface exactly.
 */
@FunctionalInterface
public interface RaftPersistenceListener {

    void onPersistentStateChanged(long currentTerm, NodeId votedFor) throws IOException;

    RaftPersistenceListener NONE = (term, votedFor) -> { };
}
