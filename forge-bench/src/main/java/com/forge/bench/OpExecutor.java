package com.forge.bench;

import java.io.Closeable;
import java.io.IOException;

/**
 * Executes one {@link BenchOperation} against a store, however that store is
 * reached — a real network connection ({@code NetworkOpExecutor}) or a
 * direct in-process call ({@code InProcessOpExecutor}, for E4's baseline).
 *
 * <p>{@link #execute} deliberately does no defensive copying, retries, or
 * error translation beyond what the underlying client/store already does —
 * any of that would be measured as part of the operation's latency, which
 * would misrepresent what's actually being benchmarked.
 */
interface OpExecutor extends Closeable {

    void execute(BenchOperation op) throws IOException;
}
