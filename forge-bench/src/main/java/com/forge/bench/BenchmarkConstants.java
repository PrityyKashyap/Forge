package com.forge.bench;

/**
 * All tunable benchmark parameters in one place, so BENCHMARKS.md's
 * methodology section and the actual code can never drift apart. Every
 * value here is chosen and documented, not derived from any external
 * source — see BENCHMARKS.md for the rationale behind each one.
 */
final class BenchmarkConstants {

    private BenchmarkConstants() {
    }

    static final int VALUE_SIZE_BYTES = 100;
    static final int REPEATS = 3;

    // E1: sequential baseline (single connection, single thread, one op type at a time)
    static final int E1_WARMUP_OPS = 300;
    static final int E1_MEASURED_OPS = 1000;
    /** Pre-populated dataset E1's and E3's GET experiments read from. */
    static final int GET_DATASET_SIZE = 10_000;

    // E3: concurrency scaling
    static final int[] CONCURRENCY_LEVELS = {1, 2, 4, 8, 16, 32};
    /** Total (not per-thread) measured ops at each concurrency level, split evenly across threads. */
    static final int E3_GET_TOTAL_MEASURED_OPS = 2000;
    static final int E3_GET_TOTAL_WARMUP_OPS = 300;
    /** Smaller than GET's: every PUT is fsynced (see WriteAheadLog), so this bounds total run time. */
    static final int E3_PUT_TOTAL_MEASURED_OPS = 500;
    static final int E3_PUT_TOTAL_WARMUP_OPS = 100;

    // E4: network overhead (in-process vs. loopback TCP), single-threaded
    static final int E4_WARMUP_OPS = 200;
    static final int E4_MEASURED_OPS = 800;

    // E5: mixed read/write workload
    static final int E5_CONCURRENCY = 8;
    static final int E5_TOTAL_MEASURED_OPS = 2000;
    static final int E5_TOTAL_WARMUP_OPS = 400;
    static final double E5_READ_FRACTION = 0.8;
    static final long E5_RANDOM_SEED = 42L;
    static final int E5_DATASET_SIZE = 5_000;
}
