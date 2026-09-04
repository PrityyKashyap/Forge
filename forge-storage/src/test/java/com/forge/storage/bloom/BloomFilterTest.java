package com.forge.storage.bloom;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one property that must never fail, exhaustively: every added key
 * always tests as present (no false negatives) — see {@link BloomFilter}'s
 * class Javadoc for why this is the load-bearing correctness guarantee the
 * rest of Phase 13 depends on. False-positive-rate tests are statistical
 * (seeded, so deterministic across runs) and check the rate is in the right
 * ballpark, not exact — a Bloom filter's false-positive rate is inherently
 * probabilistic.
 */
class BloomFilterTest {

    @Test
    void everyAddedKeyAlwaysTestsAsPresent() {
        BloomFilter filter = BloomFilter.sizedFor(1000, 0.01);
        Set<String> added = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            filter.add(key);
            added.add(key);
        }
        for (String key : added) {
            assertTrue(filter.mightContain(key), "no false negatives are ever allowed: " + key);
        }
    }

    @Test
    void emptyFilterClaimsNothingIsPresent() {
        BloomFilter filter = BloomFilter.sizedFor(1000, 0.01);
        for (int i = 0; i < 200; i++) {
            assertFalse(filter.mightContain("never-added-" + i));
        }
    }

    @Test
    void falsePositiveRateIsInTheRightBallpark() {
        double targetRate = 0.01;
        int n = 5000;
        BloomFilter filter = BloomFilter.sizedFor(n, targetRate);
        for (int i = 0; i < n; i++) {
            filter.add("present-" + i);
        }

        Random random = new Random(42); // fixed seed: deterministic across runs
        int trials = 20000;
        int falsePositives = 0;
        for (int i = 0; i < trials; i++) {
            String candidate = "absent-" + random.nextLong();
            if (filter.mightContain(candidate)) {
                falsePositives++;
            }
        }
        double observedRate = falsePositives / (double) trials;
        // Generous tolerance (target * 5) — this is a statistical check on a
        // probabilistic structure, not an exact-value assertion; it exists to
        // catch a badly broken sizing/hashing implementation (e.g. an
        // observed rate of 50%), not to pin down the third decimal place.
        assertTrue(observedRate < targetRate * 5,
                "observed false-positive rate " + observedRate + " far exceeds target " + targetRate);
    }

    @Test
    void sizedForRejectsInvalidFalsePositiveRates() {
        assertFalseCtorThrows(0.0);
        assertFalseCtorThrows(1.0);
        assertFalseCtorThrows(-0.1);
        assertFalseCtorThrows(1.1);
    }

    private static void assertFalseCtorThrows(double rate) {
        try {
            BloomFilter.sizedFor(100, rate);
            throw new AssertionError("expected IllegalArgumentException for rate " + rate);
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    void handlesVerySmallExpectedEntryCounts() {
        BloomFilter filter = BloomFilter.sizedFor(1, 0.01);
        filter.add("only-key");
        assertTrue(filter.mightContain("only-key"));
    }
}
