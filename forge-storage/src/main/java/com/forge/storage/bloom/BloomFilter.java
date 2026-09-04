package com.forge.storage.bloom;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A standard bit-array Bloom filter over string keys: {@link #mightContain}
 * never returns {@code false} for a key that was actually {@link #add}ed
 * (no false negatives, ever); it may occasionally return {@code true} for a
 * key that was never added (a false positive), at a rate governed by how
 * the filter was sized.
 *
 * <p>This is the load-bearing safety property Phase 13's SSTable integration
 * depends on: {@code SSTableReader.get()} only ever uses a filter to
 * <b>skip</b> a scan when {@code mightContain} says "definitely absent" —
 * since that answer can never be wrong, skipping on it can never turn a
 * present key into an incorrectly-missing one. A false positive only ever
 * costs a wasted scan that would have happened anyway without a filter; it
 * never returns a wrong value. See {@code BloomFilterTest} for a direct,
 * exhaustive test of the no-false-negatives property.
 *
 * <p>Uses the standard Kirsch-Mitzenmacher double-hashing technique
 * ({@code h_i(x) = h1(x) + i * h2(x)}) so only two independent hashes need
 * computing regardless of how many hash functions the filter is sized to
 * use — avoiding a dependency on an external hashing library for something
 * this small and self-contained (FNV-1a, computed twice with different
 * seeds, is the two base hashes).
 */
public final class BloomFilter {

    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    /** Appended to the key bytes before hashing a second, independent time. */
    private static final byte SECOND_HASH_SALT = 0x5A;

    private final long[] bits;
    private final int numBits;
    private final int numHashFunctions;

    BloomFilter(int numBits, int numHashFunctions) {
        if (numBits <= 0) {
            throw new IllegalArgumentException("numBits must be positive");
        }
        if (numHashFunctions <= 0) {
            throw new IllegalArgumentException("numHashFunctions must be positive");
        }
        this.numBits = numBits;
        this.numHashFunctions = numHashFunctions;
        this.bits = new long[(numBits + 63) / 64];
    }

    /**
     * Sizes a new, empty filter for {@code expectedEntries} keys at
     * approximately {@code falsePositiveRate} — the standard optimal-size
     * formulas: {@code m = ceil(-(n * ln p) / (ln 2)^2)} bits and
     * {@code k = round((m / n) * ln 2)} hash functions.
     */
    public static BloomFilter sizedFor(long expectedEntries, double falsePositiveRate) {
        if (falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
            throw new IllegalArgumentException("falsePositiveRate must be in (0, 1): " + falsePositiveRate);
        }
        long n = Math.max(1, expectedEntries);
        double m = Math.ceil(-(n * Math.log(falsePositiveRate)) / (Math.log(2) * Math.log(2)));
        int numBits = (int) Math.min(Math.max(m, 8), Integer.MAX_VALUE - 64);
        int numHashFunctions = (int) Math.max(1, Math.round((numBits / (double) n) * Math.log(2)));
        return new BloomFilter(numBits, numHashFunctions);
    }

    /** Reconstructs a filter from its raw bit words, exactly as {@link #wordsView()} exposed them — used by {@code BloomFilterFile} on load. */
    static BloomFilter fromWords(long[] words, int numBits, int numHashFunctions) {
        BloomFilter filter = new BloomFilter(numBits, numHashFunctions);
        System.arraycopy(words, 0, filter.bits, 0, words.length);
        return filter;
    }

    public void add(String key) {
        Objects.requireNonNull(key, "key must not be null");
        long h1 = fnv1a64(key.getBytes(StandardCharsets.UTF_8), false);
        long h2 = fnv1a64(key.getBytes(StandardCharsets.UTF_8), true);
        for (int i = 0; i < numHashFunctions; i++) {
            setBit(bitIndex(h1, h2, i));
        }
    }

    public boolean mightContain(String key) {
        Objects.requireNonNull(key, "key must not be null");
        long h1 = fnv1a64(key.getBytes(StandardCharsets.UTF_8), false);
        long h2 = fnv1a64(key.getBytes(StandardCharsets.UTF_8), true);
        for (int i = 0; i < numHashFunctions; i++) {
            if (!getBit(bitIndex(h1, h2, i))) {
                return false;
            }
        }
        return true;
    }

    private int bitIndex(long h1, long h2, int i) {
        long combined = h1 + (long) i * h2;
        return (int) Math.floorMod(combined, (long) numBits);
    }

    private void setBit(int index) {
        bits[index >>> 6] |= (1L << (index & 63));
    }

    private boolean getBit(int index) {
        return (bits[index >>> 6] & (1L << (index & 63))) != 0;
    }

    public int numBits() {
        return numBits;
    }

    public int numHashFunctions() {
        return numHashFunctions;
    }

    /** Read-only view of the underlying bit words, for {@code BloomFilterFile} to serialize. Not a defensive copy — caller must not mutate. */
    long[] wordsView() {
        return bits;
    }

    private static long fnv1a64(byte[] data, boolean salted) {
        long hash = FNV_OFFSET_BASIS;
        for (byte b : data) {
            hash ^= (b & 0xffL);
            hash *= FNV_PRIME;
        }
        if (salted) {
            hash ^= (SECOND_HASH_SALT & 0xffL);
            hash *= FNV_PRIME;
        }
        return hash;
    }
}
