package com.forge.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Deterministic key/value generation, so repeated runs are directly
 * comparable. Values are a single, pre-generated {@code byte[]} reused
 * across every PUT that needs one — content is irrelevant to a
 * throughput/latency measurement, only size is, and generating fresh random
 * bytes per operation would add allocation/RNG cost to the timed path
 * itself (see {@link OpExecutor}'s Javadoc on avoiding contamination).
 */
final class Workload {

    private Workload() {
    }

    static String key(String prefix, long index) {
        return prefix + "-" + String.format("%010d", index);
    }

    /** A fixed-size, deterministic value: byte {@code i} is {@code (byte) (i % 256)}. */
    static byte[] fixedValue(int sizeBytes) {
        byte[] value = new byte[sizeBytes];
        for (int i = 0; i < sizeBytes; i++) {
            value[i] = (byte) (i % 256);
        }
        return value;
    }

    static List<BenchOperation> puts(String keyPrefix, long startIndex, int count, byte[] value) {
        List<BenchOperation> ops = new ArrayList<>(count);
        for (long i = 0; i < count; i++) {
            ops.add(new BenchOperation.Put(key(keyPrefix, startIndex + i), value));
        }
        return ops;
    }

    static List<BenchOperation> deletes(String keyPrefix, long startIndex, int count) {
        List<BenchOperation> ops = new ArrayList<>(count);
        for (long i = 0; i < count; i++) {
            ops.add(new BenchOperation.Delete(key(keyPrefix, startIndex + i)));
        }
        return ops;
    }

    /** Round-robin GETs over an existing dataset of {@code datasetSize} keys, starting at {@code offset}. */
    static List<BenchOperation> getsRoundRobin(String keyPrefix, long datasetSize, long offset, int count) {
        List<BenchOperation> ops = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long index = (offset + i) % datasetSize;
            ops.add(new BenchOperation.Get(key(keyPrefix, index)));
        }
        return ops;
    }

    /**
     * A probabilistic GET/PUT mix over a shared, uniformly-distributed key
     * range: each op independently has probability {@code readFraction} of
     * being a GET, else a PUT — both against the same shared keyspace, so
     * PUTs genuinely modify data GETs may subsequently read. Deterministic
     * per {@code seed} (same seed always produces the same op sequence).
     */
    static List<BenchOperation> mixedReadWrite(String keyPrefix, long datasetSize, double readFraction,
            int count, long seed, byte[] value) {
        Random random = new Random(seed);
        List<BenchOperation> ops = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long index = (long) (random.nextDouble() * datasetSize);
            String k = key(keyPrefix, index);
            ops.add(random.nextDouble() < readFraction ? new BenchOperation.Get(k) : new BenchOperation.Put(k, value));
        }
        return ops;
    }
}
