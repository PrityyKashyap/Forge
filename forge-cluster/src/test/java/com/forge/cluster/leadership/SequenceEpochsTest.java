package com.forge.cluster.leadership;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequenceEpochsTest {

    @Test
    void bandsAreDisjointAndOrderedByTerm() {
        assertEquals(0L, SequenceEpochs.bandStart(0));
        assertEquals(SequenceEpochs.BAND_SIZE, SequenceEpochs.bandStart(1));
        assertEquals(5 * SequenceEpochs.BAND_SIZE, SequenceEpochs.bandStart(5));
        assertTrue(SequenceEpochs.bandStart(1) > SequenceEpochs.bandStart(0));
        assertTrue(SequenceEpochs.bandStart(100) > SequenceEpochs.bandStart(99));
    }

    @Test
    void impliedTermIsTheInverseOfBandStart() {
        for (long term = 0; term < 50; term++) {
            long start = SequenceEpochs.bandStart(term);
            assertEquals(term, SequenceEpochs.impliedTerm(start));
            assertEquals(term, SequenceEpochs.impliedTerm(start + SequenceEpochs.BAND_SIZE - 1));
        }
    }

    @Test
    void aSequenceNumberNeverAppearsToBelongToTwoDifferentTerms() {
        long term5End = SequenceEpochs.bandStart(6) - 1;
        assertEquals(5L, SequenceEpochs.impliedTerm(term5End));
        assertEquals(6L, SequenceEpochs.impliedTerm(term5End + 1));
    }

    @Test
    void negativeTermIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> SequenceEpochs.bandStart(-1));
    }

    @Test
    void negativeSequenceNumberIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> SequenceEpochs.impliedTerm(-1));
    }

    @Test
    void extremeTermSaturatesRatherThanOverflowingNegative() {
        long huge = Long.MAX_VALUE / SequenceEpochs.BAND_SIZE + 1000;
        assertEquals(Long.MAX_VALUE, SequenceEpochs.bandStart(huge));
    }
}
