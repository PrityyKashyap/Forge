package com.forge.bench;

/**
 * Tunable parameters for Phase 12's distributed benchmarks
 * ({@link DistributedBenchmarkRunner}) — same rule as {@link BenchmarkConstants}:
 * every value here is chosen and documented, not derived from an external
 * source, and code and BENCHMARKS.md's methodology section must never drift
 * apart.
 *
 * <p>Repeat count and dataset sizes here are deliberately smaller than
 * Phase 6's ({@code BenchmarkConstants.REPEATS} = 3): every one of these
 * experiments first has to populate a real, fsync-bound dataset from
 * scratch (unlike Phase 6's experiments, which mostly reuse a shared
 * populated dataset across repeats), so the same repeat count and dataset
 * sizes would make this phase's suite take many times as long for
 * comparatively little extra statistical confidence. Recorded here
 * explicitly rather than silently — see BENCHMARKS.md §7's methodology note.
 */
final class DistributedBenchmarkConstants {

    private DistributedBenchmarkConstants() {
    }

    static final int VALUE_SIZE_BYTES = 100;
    static final int REPEATS = 2;

    // E12: node scaling
    //
    // Concurrency and total work scale WITH node count (concurrency =
    // E12_CONCURRENCY_PER_NODE * nodeCount; per-thread op counts stay fixed).
    // An earlier version of this experiment held total client concurrency
    // fixed at 4 threads regardless of node count, which meant a 4-node run
    // spread only 4 threads across 4 nodes (~1 thread's worth of load per
    // node) while a 1-node run put all 4 threads against that one node's WAL
    // — the two configurations were not comparable, and the resulting
    // "flat throughput regardless of node count" reading was an artifact of
    // that mismatch, not a finding about FORGE. Scaling concurrency with
    // node count keeps per-node client pressure comparable across
    // configurations, so a throughput difference reflects added node
    // capacity rather than a fixed thread pool being spread thinner. See
    // BENCHMARKS.md's Phase 12 section.
    static final int[] NODE_COUNTS = {1, 2, 4};
    static final int E12_CONCURRENCY_PER_NODE = 4;
    static final int E12_MEASURED_OPS_PER_THREAD = 100;
    static final int E12_WARMUP_OPS_PER_THREAD = 20;

    // E13: replication overhead
    static final int[] FOLLOWER_COUNTS = {0, 1, 2};
    static final int E13_WARMUP_OPS = 50;
    static final int E13_MEASURED_OPS = 200;

    // E14: recovery time vs. data size
    static final long[] E14_DATASET_SIZES = {200, 1000, 3000};

    // E15: replica catch-up time vs. backlog size
    static final long[] E15_BACKLOG_SIZES = {200, 1000, 3000};
}
