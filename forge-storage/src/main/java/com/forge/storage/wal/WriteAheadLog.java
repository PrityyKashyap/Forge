package com.forge.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * A durable, append-only, checksummed write-ahead log.
 *
 * <p>Every record has the identical physical shape, whether it logs a PUT or
 * a DELETE:
 *
 * <pre>
 *   [int32  bodyLength]        length of everything below, in bytes
 *   [int64  sequenceNumber]    strictly increasing, starts at 1, never resets
 *   [int8   opType]            1 = PUT, 2 = DELETE
 *   [int32  keyLength][keyBytes]      keyBytes = key encoded as UTF-8
 *   [int32  valueLength][valueBytes]  raw bytes as-is; 0 length, no bytes, for DELETE
 *   [int32  crc32]             CRC32 over sequenceNumber..valueBytes
 * </pre>
 *
 * <p><b>Durability contract:</b> {@link #appendPut} and {@link #appendDelete}
 * do not return until the record has been written and {@link FileChannel#force}d
 * to the underlying storage device. A successful return is the durability
 * boundary — see DESIGN.md &sect;2 and &sect;11.
 *
 * <p><b>Recovery contract:</b> {@link #open} scans the file from the start,
 * decoding only complete, checksum-valid, strictly-ordered records. The first
 * record that is torn (incomplete), malformed (an impossible length), or
 * fails its checksum stops the scan; that record and everything physically
 * after it in the file is discarded — the file is truncated to the byte
 * offset immediately before it, so a subsequent append never lands after
 * undiscovered garbage.
 *
 * <p><b>Thread safety (Phase 4):</b> every method that mutates state
 * ({@link #appendPut}, {@link #appendDelete}, {@link #appendReplicated},
 * {@link #truncateAll}, {@link #truncateUpTo}, {@link #ensureNextSequenceNumberAtLeast},
 * {@link #close}) is {@code synchronized}, so this class is safe to call
 * concurrently on its own terms. That said, this is defensive hardening,
 * not the primary mechanism relied on for correctness: {@link com.forge.storage.ConcurrentLsmKeyValueStore}
 * already serializes all writers through its own lock before ever reaching
 * this class, so these calls are never actually contended in practice.
 * Making the WAL correctly self-defending independent of caller discipline
 * is still worthwhile — it has multiple callers ({@link com.forge.storage.DurableKeyValueStore},
 * {@link com.forge.storage.LsmKeyValueStore}, and {@link com.forge.storage.ConcurrentLsmKeyValueStore})
 * with different threading models, and the cost is negligible next to the
 * millisecond-scale {@code fsync} every call already pays. <b>This does
 * not, by itself, make {@code DurableKeyValueStore} or {@code LsmKeyValueStore}
 * safe for direct concurrent use</b> — their own in-memory structures
 * ({@code HashMap}, {@code TreeMap}) remain unsynchronized; only
 * {@code ConcurrentLsmKeyValueStore} provides that guarantee end to end.
 * {@link #recoveredRecords()} needs no synchronization: it returns an
 * already-computed, never-mutated-after-construction list.
 */
public final class WriteAheadLog implements Closeable {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;

    private static final int LENGTH_PREFIX_BYTES = 4;
    private static final int SEQUENCE_NUMBER_BYTES = 8;
    private static final int OP_TYPE_BYTES = 1;
    private static final int KEY_LENGTH_BYTES = 4;
    private static final int VALUE_LENGTH_BYTES = 4;
    private static final int CHECKSUM_BYTES = 4;

    /** Bytes of body overhead with an empty key and an empty value: seq + opType + keyLen + valueLen + checksum. */
    private static final int FIXED_OVERHEAD =
            SEQUENCE_NUMBER_BYTES + OP_TYPE_BYTES + KEY_LENGTH_BYTES + VALUE_LENGTH_BYTES + CHECKSUM_BYTES;

    private static final int MIN_BODY_LENGTH = FIXED_OVERHEAD;

    /**
     * Defensive ceiling on a single record's body length. Rejecting anything
     * beyond this — before ever allocating a buffer for it — is what makes a
     * corrupted length field unable to trigger an uncontrolled memory
     * allocation, regardless of what the rest of the file happens to contain.
     */
    private static final int MAX_BODY_LENGTH = 64 * 1024 * 1024; // 64 MiB

    private static final byte[] EMPTY_VALUE = new byte[0];

    private final Path file;
    private FileChannel channel;
    private final List<WalRecord> recovered;
    private long nextSequenceNumber;

    private WriteAheadLog(Path file, FileChannel channel, List<WalRecord> recovered, long nextSequenceNumber) {
        this.file = file;
        this.channel = channel;
        this.recovered = recovered;
        this.nextSequenceNumber = nextSequenceNumber;
    }

    /**
     * Opens (creating if necessary) the WAL file at {@code file}, recovers
     * every valid record from it, and physically truncates any torn/corrupt
     * tail before returning. The returned log is immediately ready to accept
     * new appends, which land after the last valid record.
     */
    public static WriteAheadLog open(Path file) throws IOException {
        FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            RecoveryResult result = recover(channel);
            channel.truncate(result.validLength());
            channel.position(result.validLength());
            return new WriteAheadLog(file, channel, result.records(), result.nextSequenceNumber());
        } catch (IOException | RuntimeException e) {
            try {
                channel.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /** The valid records found when this log was opened, in on-disk order. */
    public List<WalRecord> recoveredRecords() {
        return recovered;
    }

    /**
     * Every record currently in this WAL, re-scanned fresh right now —
     * unlike {@link #recoveredRecords()}, which is a fixed snapshot from
     * whenever this log was opened and never changes afterward. Added in
     * Phase 9 for replication catch-up: a leader sends a newly-connected
     * follower everything currently here with a sequence number at or past
     * the follower's own watermark. Only covers records not yet truncated
     * away by a flush ({@link #truncateUpTo})/{@link #truncateAll()} — a
     * follower behind further than that needs a full bootstrap (Phase 10),
     * not this.
     */
    public synchronized List<WalRecord> currentRecords() throws IOException {
        return recover(channel).records();
    }

    /**
     * Ensures the next appended record's sequence number is at least
     * {@code minimum}, raising it if necessary; never lowers it.
     *
     * <p>This exists to close a real gap: {@link #truncateAll()} preserves
     * {@code nextSequenceNumber} only for the lifetime of the
     * {@code WriteAheadLog} instance that called it. Once that instance is
     * closed, the file it truncated to empty carries no durable record of
     * what was discarded — a fresh {@link #open} on that same file sees an
     * empty file and, correctly from its own narrow point of view, starts
     * counting from 1 again. Left uncorrected, this reuses a sequence number
     * an SSTable has already claimed, and the very next restart's
     * "skip records already covered by the newest SSTable" recovery rule
     * then silently treats that *new*, already-acknowledged write as
     * redundant and drops it — a real, if narrow, path to data loss.
     * {@code LsmKeyValueStore} is the only class that knows the SSTable
     * watermark, so it is the one that must call this after opening.
     */
    public synchronized void ensureNextSequenceNumberAtLeast(long minimum) {
        nextSequenceNumber = Math.max(nextSequenceNumber, minimum);
    }

    /**
     * Discards every record currently in this WAL, resetting it to empty.
     * {@link #nextSequenceNumber} is preserved unchanged, so the next append
     * continues the same monotonic sequence rather than restarting at 1.
     *
     * <p>Phase 3 only ever calls this immediately after a synchronous MemTable
     * flush has been durably written to a new SSTable (fsynced and atomically
     * renamed) — at that exact instant, every record in this WAL is already
     * redundant with that SSTable, so discarding everything is correct, not
     * just convenient. This relies on flushes being synchronous and
     * single-threaded: nothing else could have appended a genuinely newer
     * record while the flush was in progress. A concurrent writer (Phase 4)
     * would break that assumption and require preserving a newer tail
     * instead of clearing unconditionally — this method's contract would need
     * to become "discard records up to a watermark," not "discard everything."
     * See {@link #truncateUpTo(long)}, added in Phase 4 for exactly that case;
     * this method is unchanged and still used by the single-threaded engines.
     */
    public synchronized void truncateAll() throws IOException {
        channel.truncate(0);
        channel.position(0);
        channel.force(true);
    }

    /**
     * Rewrites this WAL so that only records with {@code sequenceNumber() >
     * watermark} remain. Unlike {@link #truncateAll()}, this preserves any
     * record appended after the watermark was determined — required once a
     * flush's disk I/O can run concurrently with ordinary writes (Phase 4): a
     * write can land in the active MemTable while an older, frozen MemTable's
     * flush is still in progress, and that write's WAL record must survive
     * this call, not be discarded along with the ones the flush already
     * covers.
     *
     * <p>Uses the same crash-safe technique as SSTable creation: the
     * surviving records are re-encoded (with their original sequence numbers
     * preserved, not reassigned) to a temp file, {@code force(true)}d, then
     * atomically renamed over this WAL's own path. A crash before the rename
     * leaves the original, longer WAL completely untouched — recovery
     * replays it and correctly treats the still-present, already-flushed
     * records as redundant, per the existing watermark-skip rule. A crash
     * after the rename leaves the new, shorter WAL as the durable truth.
     * Because a plain rename does not affect this instance's already-open
     * file handle, the channel is closed and reopened against the
     * (now-renamed) path afterward.
     */
    public synchronized void truncateUpTo(long watermark) throws IOException {
        RecoveryResult result = recover(channel);
        List<WalRecord> toKeep = new ArrayList<>();
        for (WalRecord record : result.records()) {
            if (record.sequenceNumber() > watermark) {
                toKeep.add(record);
            }
        }

        Path tempFile = file.resolveSibling(file.getFileName().toString() + ".rewrite.tmp");
        try (FileChannel tempChannel = FileChannel.open(tempFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (WalRecord record : toKeep) {
                ByteBuffer buf = switch (record) {
                    case WalRecord.Put put -> encodeRecord(put.sequenceNumber(), OP_PUT,
                            put.key().getBytes(StandardCharsets.UTF_8), put.value());
                    case WalRecord.Delete delete -> encodeRecord(delete.sequenceNumber(), OP_DELETE,
                            delete.key().getBytes(StandardCharsets.UTF_8), EMPTY_VALUE);
                };
                while (buf.hasRemaining()) {
                    tempChannel.write(buf);
                }
            }
            tempChannel.force(true);
        }

        Files.move(tempFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        channel.close();
        channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.position(channel.size());
    }

    /**
     * Appends a durable PUT record. Does not return until the record has
     * been written and forced to disk.
     *
     * @return the sequence number assigned to this record
     */
    public synchronized long appendPut(String key, byte[] value) throws IOException {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        return appendRecord(OP_PUT, key, value);
    }

    /**
     * Appends a durable DELETE record. Does not return until the record has
     * been written and forced to disk.
     *
     * @return the sequence number assigned to this record
     */
    public synchronized long appendDelete(String key) throws IOException {
        Objects.requireNonNull(key, "key must not be null");
        return appendRecord(OP_DELETE, key, EMPTY_VALUE);
    }

    /**
     * The sequence number that would be assigned to the <em>next</em> record
     * appended via {@link #appendPut}/{@link #appendDelete} — equivalently,
     * one past the last record actually durable in this log. Added in Phase
     * 9 so a replication follower can tell a leader exactly where its own
     * log has gotten to ("send me starting from here"), without exposing
     * anything about this log's internals beyond that one number.
     */
    public synchronized long nextSequenceNumber() {
        return nextSequenceNumber;
    }

    /**
     * Appends {@code record} using <b>its own</b> sequence number rather
     * than assigning a fresh one — the replication path: a follower applies
     * a leader's WAL records verbatim, in the leader's order, under the
     * leader's numbering, never generating its own. See
     * {@link ReplicationOutcome} for what each result means and why.
     *
     * <p>Deliberately refuses to create a gap (see {@link ReplicationOutcome#GAP_DETECTED})
     * rather than silently accepting an out-of-order record — DESIGN.md's
     * Phase 9 contract requires followers to apply entries in exactly the
     * leader's order, with no permanent gaps; accepting {@code record} here
     * despite a gap would durably violate that the moment it's forced to
     * disk, and no later record could ever retroactively fill the hole.
     */
    public synchronized ReplicationOutcome appendReplicated(WalRecord record) throws IOException {
        Objects.requireNonNull(record, "record must not be null");
        long seq = record.sequenceNumber();
        if (seq < nextSequenceNumber) {
            return ReplicationOutcome.ALREADY_APPLIED;
        }
        if (seq > nextSequenceNumber) {
            return ReplicationOutcome.GAP_DETECTED;
        }

        ByteBuffer buf = switch (record) {
            case WalRecord.Put put -> encodeRecord(seq, OP_PUT, put.key().getBytes(StandardCharsets.UTF_8), put.value());
            case WalRecord.Delete delete -> encodeRecord(seq, OP_DELETE, delete.key().getBytes(StandardCharsets.UTF_8), EMPTY_VALUE);
        };
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        channel.force(true);

        nextSequenceNumber = seq + 1;
        return ReplicationOutcome.APPLIED;
    }

    private long appendRecord(byte opType, String key, byte[] value) throws IOException {
        long seq = nextSequenceNumber;
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = encodeRecord(seq, opType, keyBytes, value);

        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        channel.force(true);

        nextSequenceNumber = seq + 1;
        return seq;
    }

    /** Encodes one record, including its checksum, ready to write. Shared by {@link #appendRecord} and {@link #truncateUpTo}. */
    private static ByteBuffer encodeRecord(long seq, byte opType, byte[] keyBytes, byte[] value) {
        int bodyLength = FIXED_OVERHEAD + keyBytes.length + value.length;
        ByteBuffer buf = ByteBuffer.allocate(LENGTH_PREFIX_BYTES + bodyLength);
        buf.putInt(bodyLength);
        buf.putLong(seq);
        buf.put(opType);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(value.length);
        buf.put(value);

        int contentLength = buf.position() - LENGTH_PREFIX_BYTES;
        CRC32 crc = new CRC32();
        crc.update(buf.array(), LENGTH_PREFIX_BYTES, contentLength);
        buf.putInt((int) crc.getValue());

        buf.flip();
        return buf;
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    // --- recovery ------------------------------------------------------

    private record RecoveryResult(List<WalRecord> records, long validLength, long nextSequenceNumber) {
    }

    private static RecoveryResult recover(FileChannel channel) throws IOException {
        long fileSize = channel.size();
        long pos = 0;
        long lastSequenceNumber = 0;
        List<WalRecord> records = new ArrayList<>();

        while (true) {
            long remaining = fileSize - pos;
            if (remaining < LENGTH_PREFIX_BYTES) {
                break; // not even a full length prefix left: torn
            }

            ByteBuffer lengthBuf = ByteBuffer.allocate(LENGTH_PREFIX_BYTES);
            if (!readFully(channel, lengthBuf, pos)) {
                break;
            }
            int bodyLength = lengthBuf.flip().getInt();

            // Validate the declared length BEFORE allocating anything sized by it.
            if (bodyLength < MIN_BODY_LENGTH || bodyLength > MAX_BODY_LENGTH) {
                break; // negative, impossibly small, or implausibly large: corrupt
            }

            long bodyStart = pos + LENGTH_PREFIX_BYTES;
            long remainingAfterPrefix = fileSize - bodyStart;
            if (bodyLength > remainingAfterPrefix) {
                break; // declared length exceeds what's actually left in the file: torn
            }

            // Safe to allocate now: bodyLength is bounded by MAX_BODY_LENGTH and
            // confirmed to fit within the file's actual remaining bytes.
            ByteBuffer body = ByteBuffer.allocate(bodyLength);
            if (!readFully(channel, body, bodyStart)) {
                break; // defensive; shouldn't happen given the checks above
            }
            body.flip();

            long seq = body.getLong();
            byte opType = body.get();
            int keyLength = body.getInt();

            int maxPossibleKeyLength = bodyLength - FIXED_OVERHEAD;
            if (keyLength < 0 || keyLength > maxPossibleKeyLength) {
                break; // impossible key length: corrupt
            }
            byte[] keyBytes = new byte[keyLength];
            body.get(keyBytes);

            int valueLength = body.getInt();
            int expectedValueLength = bodyLength - FIXED_OVERHEAD - keyLength;
            if (valueLength != expectedValueLength) {
                break; // key/value lengths inconsistent with the declared body length: corrupt
            }
            byte[] valueBytes = new byte[valueLength];
            body.get(valueBytes);

            int contentLength = bodyLength - CHECKSUM_BYTES;
            CRC32 crc = new CRC32();
            crc.update(body.array(), 0, contentLength);
            int expectedChecksum = (int) crc.getValue();
            int storedChecksum = body.getInt();
            if (storedChecksum != expectedChecksum) {
                break; // content corrupted: torn/bit-flipped write
            }

            if (opType != OP_PUT && opType != OP_DELETE) {
                break; // unrecognized op type: corrupt
            }
            if (seq <= lastSequenceNumber) {
                break; // sequence numbers must strictly increase: corrupt/out of order
            }

            String key = new String(keyBytes, StandardCharsets.UTF_8);
            WalRecord record = (opType == OP_PUT)
                    ? new WalRecord.Put(seq, key, valueBytes)
                    : new WalRecord.Delete(seq, key);
            records.add(record);
            lastSequenceNumber = seq;
            pos = bodyStart + bodyLength;
        }

        return new RecoveryResult(List.copyOf(records), pos, lastSequenceNumber + 1);
    }

    /** Reads until {@code buffer} is full or returns false on premature EOF (a torn read). */
    private static boolean readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long pos = position;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, pos);
            if (n < 0) {
                return false;
            }
            pos += n;
        }
        return true;
    }
}
