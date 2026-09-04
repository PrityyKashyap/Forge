package com.forge.common.protocol;

/**
 * Wire-format constants shared by {@link FrameCodec} on both the encoding
 * and decoding side. Values are part of the wire format itself: changing
 * any of them is a breaking protocol change.
 */
public final class ProtocolConstants {

    private ProtocolConstants() {
    }

    public static final byte OP_PUT = 1;
    public static final byte OP_GET = 2;
    public static final byte OP_DELETE = 3;

    public static final byte STATUS_OK_ABSENT = 0;
    public static final byte STATUS_OK_PRESENT = 1;
    public static final byte STATUS_ERROR = 2;

    public static final byte ERROR_NONE = 0;
    public static final byte ERROR_MALFORMED_REQUEST = 1;
    public static final byte ERROR_UNKNOWN_OPERATION = 2;
    public static final byte ERROR_OVERSIZED_REQUEST = 3;
    public static final byte ERROR_STORAGE_ERROR = 4;
    public static final byte ERROR_INTERNAL_ERROR = 5;
    /** Added in Phase 7: this node does not own the requested key under its current partition map. */
    public static final byte ERROR_NOT_OWNER = 6;
    /**
     * Added in Phase 15: this node is not currently the fenced, Raft-confirmed
     * authoritative leader for this key's partition — it was either never
     * leader, or was leader in an older term and has since been (or must
     * now consider itself) superseded. The write was rejected before ever
     * touching the store. See {@code docs/CONSISTENCY.md} §5 and
     * {@code ConnectionHandler}'s message format for this error (which
     * carries a machine-parseable {@code term=}/{@code leader=} hint in its
     * free-text message, documented there and consumed by
     * {@code PartitionedForgeClient}'s retry logic).
     */
    public static final byte ERROR_NOT_LEADER = 7;

    /** Default cap on a single frame's declared payload length, in bytes. */
    public static final int DEFAULT_MAX_FRAME_LENGTH = 16 * 1024 * 1024;

    /** Default cap on a single key's encoded UTF-8 length, in bytes. */
    public static final int DEFAULT_MAX_KEY_LENGTH = 64 * 1024;
}
