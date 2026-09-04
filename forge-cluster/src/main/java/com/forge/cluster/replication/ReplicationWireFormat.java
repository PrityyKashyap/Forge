package com.forge.cluster.replication;

import com.forge.storage.wal.WalRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The replication stream's wire format — deliberately separate from
 * {@code forge-common}'s client {@code Request}/{@code Response} protocol;
 * see {@code ReplicationServer}'s class Javadoc for why. One frame per
 * {@link WalRecord}:
 *
 * <pre>
 * [int8 opType (1=PUT, 2=DELETE)][int64 sequenceNumber]
 * [int32 keyLength][keyBytes][int32 valueLength][valueBytes]
 * </pre>
 *
 * valueLength is always 0 (no bytes) for DELETE. Length validation follows
 * the same discipline as every other codec in this project: every length is
 * checked against a maximum before it's used to allocate an array.
 */
final class ReplicationWireFormat {

    private static final byte OP_PUT = 1;
    private static final byte OP_DELETE = 2;
    private static final int MAX_KEY_OR_VALUE_LENGTH = 64 * 1024 * 1024; // 64 MiB, matches the WAL's own ceiling

    private ReplicationWireFormat() {
    }

    static void writeRecord(DataOutputStream out, WalRecord record) throws IOException {
        byte[] keyBytes = record.key().getBytes(StandardCharsets.UTF_8);
        switch (record) {
            case WalRecord.Put put -> {
                byte[] value = put.value();
                out.writeByte(OP_PUT);
                out.writeLong(record.sequenceNumber());
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeInt(value.length);
                out.write(value);
            }
            case WalRecord.Delete delete -> {
                out.writeByte(OP_DELETE);
                out.writeLong(record.sequenceNumber());
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeInt(0);
            }
        }
    }

    /** @throws java.io.EOFException if the stream ends before a full record is read (a disconnect) */
    static WalRecord readRecord(DataInputStream in) throws IOException {
        byte opType = in.readByte();
        if (opType != OP_PUT && opType != OP_DELETE) {
            throw new IOException("corrupt replication stream: unrecognized opType " + opType);
        }
        long seq = in.readLong();

        int keyLength = in.readInt();
        if (keyLength < 0 || keyLength > MAX_KEY_OR_VALUE_LENGTH) {
            throw new IOException("corrupt replication stream: invalid key length " + keyLength);
        }
        byte[] keyBytes = new byte[keyLength];
        in.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLength = in.readInt();
        if (valueLength < 0 || valueLength > MAX_KEY_OR_VALUE_LENGTH) {
            throw new IOException("corrupt replication stream: invalid value length " + valueLength);
        }
        byte[] value = new byte[valueLength];
        in.readFully(value);

        return opType == OP_PUT ? new WalRecord.Put(seq, key, value) : new WalRecord.Delete(seq, key);
    }
}
