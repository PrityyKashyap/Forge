package com.forge.storage.bloom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Reads and writes a {@link BloomFilter} as a small sidecar file next to an
 * SSTable — deliberately a separate file, not a new section inside
 * {@code SSTableFormat}, so that Phase 3's already-frozen SSTable record
 * format needs no change at all, and an SSTable written before Phase 13 (or
 * one whose sidecar failed to write, or was deleted) is still fully valid
 * and readable — just without the scan-skipping optimization.
 *
 * <h2>Corruption philosophy — deliberately the opposite of {@code SSTableReader}'s</h2>
 * {@code SSTableReader} treats any corruption as fatal, because an SSTable
 * is the only durable copy of already-acknowledged data. A Bloom filter
 * sidecar is pure optimization, derivable in full from the SSTable it sits
 * next to — losing it can never cause a wrong answer, only a slower one
 * (see {@link BloomFilter}'s class Javadoc on why a false positive is
 * always safe). So {@link #tryLoad} never throws for a missing or corrupt
 * sidecar: it logs and returns {@link Optional#empty()}, and callers treat
 * that identically to "no filter available."
 *
 * <pre>
 * HEADER (24 bytes)
 *   [int32 magic]              0x46424C4D ("FBLM")
 *   [int32 formatVersion]      1
 *   [int32 numHashFunctions]
 *   [int32 numBits]
 *   [int32 numWords]           length of the long[] that follows
 *   [int32 headerChecksum]     CRC32 over magic..numWords
 * BODY
 *   [numWords * int64]         the filter's bit array, one long per word
 *   [int32 bodyChecksum]       CRC32 over the body's raw bytes
 * </pre>
 */
public final class BloomFilterFile {

    private static final Logger log = LoggerFactory.getLogger(BloomFilterFile.class);

    private static final int MAGIC = 0x46424C4D;
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_LENGTH = 4 * 6;

    private BloomFilterFile() {
    }

    /** The conventional sidecar path for a given SSTable file: {@code <sstable>.bloom}. */
    public static Path sidecarPathFor(Path sstableFile) {
        return sstableFile.resolveSibling(sstableFile.getFileName().toString() + ".bloom");
    }

    /**
     * Writes {@code filter} via the same temp-file + {@code force(true)} +
     * atomic-rename discipline every other durable file in this project
     * uses. Callers should treat a thrown {@link IOException} here as
     * non-fatal to whatever larger operation (a flush, a compaction) is
     * writing the SSTable this sidecar belongs to — see this class's
     * Javadoc on why losing a sidecar is never a correctness problem.
     */
    public static void write(Path tempFile, Path finalFile, BloomFilter filter) throws IOException {
        long[] words = filter.wordsView();
        try (FileChannel channel = FileChannel.open(tempFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH);
            header.putInt(MAGIC);
            header.putInt(FORMAT_VERSION);
            header.putInt(filter.numHashFunctions());
            header.putInt(filter.numBits());
            header.putInt(words.length);
            CRC32 headerCrc = new CRC32();
            headerCrc.update(header.array(), 0, HEADER_LENGTH - 4);
            header.putInt((int) headerCrc.getValue());
            header.flip();
            writeFully(channel, header);

            ByteBuffer body = ByteBuffer.allocate(words.length * 8 + 4);
            for (long word : words) {
                body.putLong(word);
            }
            CRC32 bodyCrc = new CRC32();
            bodyCrc.update(body.array(), 0, words.length * 8);
            body.putInt((int) bodyCrc.getValue());
            body.flip();
            writeFully(channel, body);

            channel.force(true);
        }
        Files.move(tempFile, finalFile, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Loads the sidecar for {@code sstableFile}, if present and intact.
     * Never throws — any I/O error, missing file, bad magic/version, or
     * checksum mismatch is logged and treated as "no filter available."
     */
    public static Optional<BloomFilter> tryLoad(Path sstableFile) {
        Path path = sidecarPathFor(sstableFile);
        if (!Files.isRegularFile(path)) {
            return Optional.empty();
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < HEADER_LENGTH) {
                log.warn("ignoring corrupt Bloom filter sidecar (smaller than a header): {}", path);
                return Optional.empty();
            }
            ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH);
            readFully(channel, header, 0);
            header.flip();

            int magic = header.getInt();
            int version = header.getInt();
            int numHashFunctions = header.getInt();
            int numBits = header.getInt();
            int numWords = header.getInt();
            int storedHeaderChecksum = header.getInt();

            if (magic != MAGIC || version != FORMAT_VERSION) {
                log.warn("ignoring unrecognized Bloom filter sidecar (bad magic/version): {}", path);
                return Optional.empty();
            }
            CRC32 headerCrc = new CRC32();
            headerCrc.update(header.array(), 0, HEADER_LENGTH - 4);
            if ((int) headerCrc.getValue() != storedHeaderChecksum) {
                log.warn("ignoring corrupt Bloom filter sidecar (header checksum mismatch): {}", path);
                return Optional.empty();
            }
            if (numWords < 0 || numBits <= 0 || numHashFunctions <= 0
                    || fileSize != HEADER_LENGTH + (long) numWords * 8 + 4) {
                log.warn("ignoring corrupt Bloom filter sidecar (inconsistent lengths): {}", path);
                return Optional.empty();
            }

            ByteBuffer body = ByteBuffer.allocate(numWords * 8 + 4);
            readFully(channel, body, HEADER_LENGTH);
            body.flip();
            long[] words = new long[numWords];
            for (int i = 0; i < numWords; i++) {
                words[i] = body.getLong();
            }
            CRC32 bodyCrc = new CRC32();
            bodyCrc.update(body.array(), 0, numWords * 8);
            int storedBodyChecksum = body.getInt();
            if ((int) bodyCrc.getValue() != storedBodyChecksum) {
                log.warn("ignoring corrupt Bloom filter sidecar (body checksum mismatch): {}", path);
                return Optional.empty();
            }

            return Optional.of(BloomFilter.fromWords(words, numBits, numHashFunctions));
        } catch (IOException e) {
            log.warn("ignoring unreadable Bloom filter sidecar: {}", path, e);
            return Optional.empty();
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long pos = position;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, pos);
            if (n < 0) {
                throw new IOException("unexpected end of file reading Bloom filter sidecar at " + pos);
            }
            pos += n;
        }
    }
}
