package com.forge.cluster.consensus;

import java.util.Objects;

/**
 * One entry in a {@link RaftNode}'s replicated log. {@code command} is
 * opaque to Raft itself — in this phase's scope it only ever carries
 * {@link #NO_OP}, appended once by a freshly-elected leader (standard Raft
 * practice: committing a current-term entry quickly is what lets a new
 * leader safely commit any older-term entries it inherited, and — in this
 * codebase's integration — a committed no-op is exactly the signal
 * {@code RaftCluster} uses to confirm "this leadership is real, a majority
 * has acknowledged it," before treating {@code selfId} as the current
 * partition leader for Phase 9 replication purposes). Carrying real KV
 * commands through this log is explicitly out of scope — see
 * {@code RaftNode}'s class Javadoc on why Phase 9's replication remains the
 * data plane.
 */
public record LogEntry(long term, String command) {

    public static final String NO_OP = "no-op";

    public LogEntry {
        Objects.requireNonNull(command, "command must not be null");
    }
}
