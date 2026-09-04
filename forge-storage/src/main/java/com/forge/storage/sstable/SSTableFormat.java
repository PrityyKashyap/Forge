package com.forge.storage.sstable;

/**
 * Shared binary-format constants for {@link SSTableWriter} and {@link SSTableReader}.
 * Kept in one place so the two never drift apart on what the format actually is.
 *
 * <pre>
 * HEADER (24 bytes)
 *   [int32 magic]              0x46535354 ("FSST")
 *   [int32 formatVersion]      1
 *   [int64 maxSequenceNumber]  highest WAL sequence number reflected by this flush
 *   [int32 entryCount]         number of records that follow (cross-check only)
 *   [int32 headerChecksum]     CRC32 over magic..entryCount
 *
 * RECORDS (entryCount of them, in ascending key order)
 *   [int32 recordLength]              length of everything below
 *   [int8  entryType]                 1 = VALUE, 2 = TOMBSTONE
 *   [int32 keyLength][keyBytes]       key, UTF-8
 *   [int32 valueLength][valueBytes]   present only for VALUE; 0/none for TOMBSTONE
 *   [int32 crc32]                     over entryType..valueBytes
 * </pre>
 */
final class SSTableFormat {

    static final int MAGIC = 0x46535354; // "FSST"
    static final int FORMAT_VERSION = 1;

    static final int MAGIC_BYTES = 4;
    static final int VERSION_BYTES = 4;
    static final int MAX_SEQUENCE_NUMBER_BYTES = 8;
    static final int ENTRY_COUNT_BYTES = 4;
    static final int HEADER_CHECKSUM_BYTES = 4;

    /** Bytes covered by the header checksum: magic..entryCount. */
    static final int HEADER_CHECKSUM_COVERAGE =
            MAGIC_BYTES + VERSION_BYTES + MAX_SEQUENCE_NUMBER_BYTES + ENTRY_COUNT_BYTES;

    static final int HEADER_LENGTH = HEADER_CHECKSUM_COVERAGE + HEADER_CHECKSUM_BYTES;

    static final byte ENTRY_TYPE_VALUE = 1;
    static final byte ENTRY_TYPE_TOMBSTONE = 2;

    static final int RECORD_LENGTH_PREFIX_BYTES = 4;
    static final int ENTRY_TYPE_BYTES = 1;
    static final int KEY_LENGTH_BYTES = 4;
    static final int VALUE_LENGTH_BYTES = 4;
    static final int RECORD_CHECKSUM_BYTES = 4;

    /** Record body overhead with an empty key and an empty value. */
    static final int RECORD_FIXED_OVERHEAD =
            ENTRY_TYPE_BYTES + KEY_LENGTH_BYTES + VALUE_LENGTH_BYTES + RECORD_CHECKSUM_BYTES;

    static final int MIN_RECORD_BODY_LENGTH = RECORD_FIXED_OVERHEAD;

    /** Same defensive ceiling, and for the same reason, as {@code WriteAheadLog}'s MAX_BODY_LENGTH. */
    static final int MAX_RECORD_BODY_LENGTH = 64 * 1024 * 1024; // 64 MiB

    static final byte[] EMPTY_VALUE = new byte[0];

    private SSTableFormat() {
    }
}
