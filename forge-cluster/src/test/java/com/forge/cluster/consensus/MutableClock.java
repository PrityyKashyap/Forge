package com.forge.cluster.consensus;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * A settable {@link Clock} for deterministic tests — advanced explicitly by
 * the test, never by real wall-clock time. Same role here as
 * {@code com.forge.cluster.membership.MutableClock} plays for
 * {@code FailureDetectorTest}; duplicated rather than shared since both are
 * small, package-private test doubles and the two test packages don't
 * otherwise depend on each other.
 */
final class MutableClock extends Clock {

    private Instant now;
    private final ZoneId zone;

    MutableClock(Instant start) {
        this(start, ZoneOffset.UTC);
    }

    private MutableClock(Instant start, ZoneId zone) {
        this.now = start;
        this.zone = zone;
    }

    void advance(Duration duration) {
        now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId newZone) {
        return new MutableClock(now, newZone);
    }

    @Override
    public Instant instant() {
        return now;
    }
}
