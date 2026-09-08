package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Post-Phase-15 audit addition: durable storage for exactly the two fields
 * the Raft paper calls "persistent state" for safety — {@code currentTerm}
 * and {@code votedFor} (§5.6's crash-recovery requirement; not the log —
 * see below for why that's a deliberate, disclosed scope boundary).
 *
 * <h2>The safety property this closes</h2>
 * Without this, a node that crashes and restarts mid-term forgets it already
 * voted, and could grant a second, contradictory vote in a term it already
 * voted in — the exact gap {@code RaftNode}'s class Javadoc disclosed since
 * Phase 14 ("Known limitations", PROGRESS.md). Persisting {@code
 * currentTerm}/{@code votedFor} before a node ever responds to an RPC that
 * changed them closes it: a restarted node reloads exactly what it last
 * durably knew, so it can never grant a second vote for a term it already
 * voted in, even across a crash.
 *
 * <h2>Why the log is deliberately NOT persisted here</h2>
 * This project's Raft log (see {@code RaftNode}'s class Javadoc) carries
 * nothing but one no-op "I am leader for this term" marker per election —
 * never real KV data (that remains Phase 9's separate, already-durable
 * replication path). Losing the log on restart costs nothing safety-wise:
 * a restarted node rejoins with an empty log and its persisted {@code
 * currentTerm}, and the existing {@code AppendEntries} consistency check
 * (a log-length mismatch against {@code prevLogIndex}) makes it re-adopt
 * the current leader's log from scratch via the same {@code nextIndex}
 * back-off mechanism already tested for ordinary lagging followers — no new
 * mechanism needed <em>as a follower</em>. Persisting the log too would be
 * the theoretically complete answer, but would require durable storage
 * keyed to log mutation/truncation on every AppendEntries, a materially
 * bigger change for a log that, in this project, never holds anything
 * worth recovering. Recorded here as a scope boundary, not glossed over.
 *
 * <h2>A real, disclosed liveness consequence: the restarted node can't itself lead again — in the worst case, not ever</h2>
 * Because the restarted node's log is empty while an active peer's log is
 * not, Raft's own leader-election safety rule (candidate's log must be at
 * least as up to date as the voter's) correctly refuses to elect it —
 * regardless of how high its persisted {@code currentTerm} climbs, since
 * the up-to-date check is independent of term. This is <em>correct</em>
 * Raft behavior, not a bug in this implementation, but it has a sharp edge
 * this project's own {@code ChaosScenarioTest} Scenario K already
 * discovered in a different guise: in a cluster no larger than the bare
 * minimum quorum (a 2-node cluster, where losing one node already loses
 * majority), a restarted node with the "stale-looking" empty log and its
 * one remaining peer can deadlock permanently — the peer alone can never
 * reach a 2-node majority, and the restarted node can never win a vote
 * against the peer's non-empty log. Neither side can ever become leader
 * again. With 3 or more nodes this isn't fatal: the surviving majority
 * (2 of 3, e.g.) elects a leader entirely without the restarted node's
 * vote, and that leader's own {@code AppendEntries} eventually repairs the
 * restarted node's log via the ordinary follower path — it just can never
 * be the one to initiate that recovery by winning an election itself.
 * {@code RaftClusterPersistenceIntegrationTest} proves the healthy (3-node)
 * case explicitly and documents this exact 2-node degenerate case in its
 * own Javadoc, matching Scenario K's precedent instead of writing a test
 * that (correctly) intermittently fails against an actually-impossible
 * expectation.
 *
 * <h2>Format and crash safety</h2>
 * Mirrors {@code SSTableWriter}/{@code WriteAheadLog}'s established
 * discipline exactly: write to a temp file, {@code force(true)}, then an
 * atomic rename into place — a crash at any point before the rename leaves
 * the previous durable state (or no file, on first-ever startup) completely
 * intact. A CRC32 over the payload detects torn/corrupt reads; on mismatch,
 * {@link #load} throws rather than silently falling back to {@code term=0}
 * — a fabricated "never voted" default would be less safe than refusing to
 * start, since it could reintroduce exactly the double-vote risk this class
 * exists to prevent.
 */
public final class RaftPersistentState {

    private static final byte FORMAT_VERSION = 1;

    private final Path file;
    private long currentTerm;
    private NodeId votedFor;

    private RaftPersistentState(Path file, long currentTerm, NodeId votedFor) {
        this.file = file;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    /**
     * Loads persisted state from {@code file}, or a fresh {@code term=0,
     * votedFor=null} state if {@code file} does not exist yet (first-ever
     * startup for this node — safe, since a node with no prior vote history
     * has nothing to forget).
     *
     * @throws IOException if {@code file} exists but is truncated or its
     *                      checksum does not match — a genuinely corrupt or
     *                      torn write, deliberately not papered over
     */
    public static RaftPersistentState load(Path file) throws IOException {
        Objects.requireNonNull(file, "file must not be null");
        if (!Files.exists(file)) {
            return new RaftPersistentState(file, 0L, null);
        }
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length < 1 + 8 + 1 + 8) {
            throw new IOException("corrupt Raft persistent-state file (too short): " + file);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte version = buffer.get();
        if (version != FORMAT_VERSION) {
            throw new IOException("unsupported Raft persistent-state format version " + version + " in " + file);
        }
        long term = buffer.getLong();
        boolean hasVotedFor = buffer.get() != 0;
        NodeId votedFor = null;
        if (hasVotedFor) {
            int length = buffer.getInt();
            if (length < 0 || length > buffer.remaining() - 8) {
                throw new IOException("corrupt Raft persistent-state file (bad votedFor length): " + file);
            }
            byte[] idBytes = new byte[length];
            buffer.get(idBytes);
            votedFor = new NodeId(new String(idBytes, StandardCharsets.UTF_8));
        }
        long expectedChecksum = buffer.getLong();
        long actualChecksum = checksum(bytes, bytes.length - 8);
        if (expectedChecksum != actualChecksum) {
            throw new IOException("corrupt Raft persistent-state file (checksum mismatch): " + file);
        }
        return new RaftPersistentState(file, term, votedFor);
    }

    /**
     * Durably persists {@code term}/{@code votedFor}, replacing whatever was
     * there before. Returns only after the data is on disk (temp file
     * written, forced, and atomically renamed into place) — a caller that
     * calls this before responding to an RPC gets the paper's exact
     * ordering guarantee.
     */
    public synchronized void save(long term, NodeId votedFor) throws IOException {
        byte[] idBytes = votedFor == null ? null : votedFor.value().getBytes(StandardCharsets.UTF_8);
        int size = 1 + 8 + 1 + (idBytes == null ? 0 : 4 + idBytes.length) + 8;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(FORMAT_VERSION);
        buffer.putLong(term);
        buffer.put((byte) (idBytes == null ? 0 : 1));
        if (idBytes != null) {
            buffer.putInt(idBytes.length);
            buffer.put(idBytes);
        }
        long crc = checksum(buffer.array(), size - 8);
        buffer.putLong(crc);
        buffer.flip();

        Path tempFile = file.resolveSibling(file.getFileName().toString() + ".tmp");
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (FileChannel channel = FileChannel.open(tempFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            channel.write(buffer);
            channel.force(true);
        }
        Files.move(tempFile, file, StandardCopyOption.ATOMIC_MOVE);

        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    private static long checksum(byte[] bytes, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(bytes, 0, length);
        return crc32.getValue();
    }

    public long currentTerm() {
        return currentTerm;
    }

    public Optional<NodeId> votedFor() {
        return Optional.ofNullable(votedFor);
    }
}
