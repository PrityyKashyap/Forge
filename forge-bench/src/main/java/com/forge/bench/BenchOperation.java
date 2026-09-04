package com.forge.bench;

/** One GET/PUT/DELETE the benchmark harness will issue and time. */
sealed interface BenchOperation permits BenchOperation.Get, BenchOperation.Put, BenchOperation.Delete {

    record Get(String key) implements BenchOperation {
    }

    record Put(String key, byte[] value) implements BenchOperation {
    }

    record Delete(String key) implements BenchOperation {
    }
}
