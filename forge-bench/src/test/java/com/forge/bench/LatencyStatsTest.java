package com.forge.bench;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LatencyStatsTest {

    @Test
    void rejectsEmptyInput() {
        assertThrows(IllegalArgumentException.class, () -> LatencyStats.of(new long[0]));
    }

    @Test
    void singleSampleIsEveryPercentile() {
        LatencyStats stats = LatencyStats.of(new long[]{42});
        assertEquals(1, stats.count());
        assertEquals(42.0, stats.averageNanos());
        assertEquals(42, stats.p50Nanos());
        assertEquals(42, stats.p95Nanos());
        assertEquals(42, stats.p99Nanos());
        assertEquals(42, stats.maxNanos());
    }

    @Test
    void percentilesOnOneThousandSequentialSamplesMatchNearestRankByHand() {
        long[] samples = new long[1000];
        for (int i = 0; i < 1000; i++) {
            samples[i] = i + 1; // 1..1000
        }
        LatencyStats stats = LatencyStats.of(samples);

        assertEquals(1000, stats.count());
        assertEquals(500.5, stats.averageNanos());
        assertEquals(500, stats.p50Nanos());
        assertEquals(950, stats.p95Nanos());
        assertEquals(990, stats.p99Nanos());
        assertEquals(1000, stats.maxNanos());
    }

    @Test
    void percentilesOnFourSamplesMatchNearestRankByHand() {
        // ceil(0.50*4)-1=1 -> 20; ceil(0.95*4)-1=3 -> 40; ceil(0.99*4)-1=3 -> 40
        LatencyStats stats = LatencyStats.of(new long[]{40, 10, 30, 20});

        assertEquals(20, stats.p50Nanos());
        assertEquals(40, stats.p95Nanos());
        assertEquals(40, stats.p99Nanos());
        assertEquals(40, stats.maxNanos());
        assertEquals(25.0, stats.averageNanos());
    }

    @Test
    void unsortedInputIsSortedBeforeComputingPercentiles() {
        LatencyStats ascending = LatencyStats.of(new long[]{1, 2, 3, 4, 5});
        LatencyStats shuffled = LatencyStats.of(new long[]{5, 1, 4, 2, 3});

        assertEquals(ascending.p50Nanos(), shuffled.p50Nanos());
        assertEquals(ascending.p95Nanos(), shuffled.p95Nanos());
        assertEquals(ascending.maxNanos(), shuffled.maxNanos());
    }

    @Test
    void doesNotMutateTheInputArray() {
        long[] input = {5, 1, 4, 2, 3};
        long[] original = input.clone();

        LatencyStats.of(input);

        assertEquals(original.length, input.length);
        for (int i = 0; i < original.length; i++) {
            assertEquals(original[i], input[i], "input array must not be mutated");
        }
    }

    @Test
    void maxEqualsTheLargestSampleEvenWhenItIsNotAPercentileBoundary() {
        LatencyStats stats = LatencyStats.of(new long[]{1, 2, 3, 4, 1_000_000});
        assertEquals(1_000_000, stats.maxNanos());
    }
}
