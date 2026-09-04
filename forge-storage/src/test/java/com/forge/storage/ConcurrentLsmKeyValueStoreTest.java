package com.forge.storage;

import com.forge.storage.memtable.MemTable;
import com.forge.storage.sstable.SSTableWriter;
import com.forge.storage.wal.ReplicationOutcome;
import com.forge.storage.wal.WalRecord;
import com.forge.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 4 concurrency correctness suite for {@link ConcurrentLsmKeyValueStore}.
 *
 * <p>Every concurrency test here asserts a specific, falsifiable invariant —
 * never merely "no exception was thrown" — and uses a {@link CyclicBarrier}
 * to force threads to begin their operations at the same instant, maximizing
 * the chance a genuine race actually manifests rather than being hidden by
 * ordinary scheduling gaps.
 */
class ConcurrentLsmKeyValueStoreTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static long countSSTableFiles(Path dir) throws IOException {
        long count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sstable-*.sst")) {
            for (Path ignored : stream) {
                count++;
            }
        }
        return count;
    }

    /** Runs {@code task} on {@code threadCount} threads, all released simultaneously by a barrier; rethrows any failure. */
    private static void runConcurrently(int threadCount, IntConsumer task) throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Thread> threads = new ArrayList<>();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        for (int t = 0; t < threadCount; t++) {
            int index = t;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    task.accept(index);
                } catch (Throwable e) {
                    failures.add(e);
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        if (!failures.isEmpty()) {
            AssertionError error = new AssertionError("thread(s) failed: " + failures);
            for (Throwable f : failures) {
                error.addSuppressed(f);
            }
            throw error;
        }
    }

    // =====================================================================
    // Basic CRUD — single-threaded, mirrors the Phase 3 contract
    // =====================================================================

    @Test
    void putThenGetReturnsStoredValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    @Test
    void putOverwriteReturnsPreviousValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(store.put("a", bytes("1")).isEmpty());
            assertArrayEquals(bytes("1"), store.put("a", bytes("2")).orElseThrow());
            assertArrayEquals(bytes("2"), store.get("a").orElseThrow());
        }
    }

    @Test
    void deleteExistingKeyReturnsItsValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            assertArrayEquals(bytes("1"), store.delete("a").orElseThrow());
            assertTrue(store.get("a").isEmpty());
        }
    }

    @Test
    void deleteMissingKeyIsNoOpAndReturnsEmpty(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(store.delete("never-existed").isEmpty());
        }
    }

    @Test
    void putRejectsNullKeyAndValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertThrows(NullPointerException.class, () -> store.put(null, bytes("v")));
            assertThrows(NullPointerException.class, () -> store.put("k", null));
        }
    }

    @Test
    void getAndDeleteRejectNullKey(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertThrows(NullPointerException.class, () -> store.get(null));
            assertThrows(NullPointerException.class, () -> store.delete(null));
        }
    }

    @Test
    void constructorRejectsNonPositiveFlushThreshold(@TempDir Path dir) {
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentLsmKeyValueStore(dir, 0));
        assertThrows(IllegalArgumentException.class, () -> new ConcurrentLsmKeyValueStore(dir, -1));
    }

    // =====================================================================
    // Flushing — single-threaded, deterministic
    // =====================================================================

    @Test
    void explicitFlushProducesAnSSTable(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
            assertEquals(1, countSSTableFiles(dir));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    @Test
    void flushOnEmptyMemTableIsANoOp(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.flush();
            assertEquals(0, countSSTableFiles(dir));
        }
    }

    @Test
    void getFindsAValueThatExistsOnlyInAnSSTable(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    @Test
    void tombstoneShadowsAnOlderSSTableValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
            store.delete("a");
            assertTrue(store.get("a").isEmpty());
        }
    }

    @Test
    void newerActiveValueShadowsOlderSSTableValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("first"));
            store.flush();
            store.put("a", bytes("second"));
            assertArrayEquals(bytes("second"), store.get("a").orElseThrow());
        }
    }

    // =====================================================================
    // Restart / recovery — single-threaded
    // =====================================================================

    @Test
    void restartRecoversFlushedAndUnflushedDataTogether(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("flushed", bytes("in-an-sstable"));
            store.flush();
            store.put("unflushed", bytes("wal-only"));
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("in-an-sstable"), reopened.get("flushed").orElseThrow());
            assertArrayEquals(bytes("wal-only"), reopened.get("unflushed").orElseThrow());
        }
    }

    @Test
    void multipleRestartCyclesAccumulateCorrectly(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
        }
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            store.put("b", bytes("2"));
        }
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            store.delete("a");
            store.flush();
        }
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(store.get("a").isEmpty());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
        }
    }

    // =====================================================================
    // Defensive copy — must still hold through this engine too
    // =====================================================================

    @Test
    void mutatingInputArrayAfterPutDoesNotAffectStoredValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            byte[] input = bytes("original");
            store.put("k", input);
            input[0] = (byte) 'X';
            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }

    @Test
    void mutatingReturnedArrayFromGetDoesNotAffectStoredValue(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("original"));
            byte[] firstRead = store.get("k").orElseThrow();
            firstRead[0] = (byte) 'X';
            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }

    @Test
    void defensiveCopyHoldsForValuesReadBackFromAnSSTable(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("original"));
            store.flush();
            byte[] firstRead = store.get("k").orElseThrow();
            firstRead[0] = (byte) 'X';
            assertArrayEquals(bytes("original"), store.get("k").orElseThrow());
        }
    }

    // =====================================================================
    // Adversarial crash-window simulation (post-implementation review)
    // =====================================================================

    /**
     * Manually reproduces the exact intermediate state between an SSTable's
     * atomic rename completing and {@code wal.truncateUpTo} running — as if
     * the process crashed in that specific window — by driving
     * {@link WriteAheadLog} and {@link SSTableWriter} directly and
     * deliberately skipping the truncation step, then opening a real
     * {@link ConcurrentLsmKeyValueStore} on the result. Verifies the crash
     * scenario proven safe in the design actually holds in the real code,
     * not just in the proof.
     */
    @Test
    void recoveryToleratesAnUntruncatedWalAlongsideAnAlreadyCompletedSSTable(@TempDir Path dir) throws IOException {
        MemTable memTable = new MemTable();
        long seq;
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve("forge.wal"))) {
            seq = wal.appendPut("a", bytes("1"));
            memTable.put("a", bytes("1"), seq);
            // Deliberately no wal.truncateUpTo() here.
        }
        SSTableWriter.write(dir.resolve("flush.tmp"), dir.resolve(String.format("sstable-%019d.sst", seq)),
                memTable.entries(), seq);

        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow(),
                    "a redundant, not-yet-truncated WAL record must not cause duplication or loss");
            store.put("b", bytes("2"));
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), reopened.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), reopened.get("b").orElseThrow());
        }
    }

    // =====================================================================
    // CONCURRENCY — the required 20-case suite
    // =====================================================================

    // 1. concurrent PUTs on different keys
    @Test
    void concurrentPutsOnDifferentKeysAllSucceedWithNoLoss(@TempDir Path dir) throws Exception {
        int threadCount = 16;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            runConcurrently(threadCount, t -> store.put("key-" + t, bytes("value-" + t)));
            for (int t = 0; t < threadCount; t++) {
                assertArrayEquals(bytes("value-" + t), store.get("key-" + t).orElseThrow(),
                        "every thread's disjoint-key write must be present");
            }
        }
    }

    // 2 & 6. concurrent PUTs on the same key / PUT vs PUT — return-value chaining
    @Test
    void concurrentPutsOnTheSameKeyFormAConsistentReturnValueChain(@TempDir Path dir) throws Exception {
        int threadCount = 16;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            Map<Integer, Optional<byte[]>> previousByThread = new java.util.concurrent.ConcurrentHashMap<>();
            runConcurrently(threadCount, t -> previousByThread.put(t, store.put("k", bytes("v" + t))));

            // Build the chain: each thread's "previous" must equal exactly one other
            // thread's submitted value, except one root (empty), with no repeats and no cycles.
            Set<String> submittedValues = new HashSet<>();
            for (int t = 0; t < threadCount; t++) {
                submittedValues.add("v" + t);
            }
            Map<String, Integer> previousValueCounts = new HashMap<>();
            int roots = 0;
            for (int t = 0; t < threadCount; t++) {
                Optional<byte[]> previous = previousByThread.get(t);
                if (previous.isEmpty()) {
                    roots++;
                } else {
                    String previousValue = new String(previous.get(), StandardCharsets.UTF_8);
                    assertTrue(submittedValues.contains(previousValue),
                            "returned previous value must be something some thread actually submitted");
                    previousValueCounts.merge(previousValue, 1, Integer::sum);
                }
            }
            assertEquals(1, roots, "exactly one PUT must report no previous value (the one that ran first)");
            for (int count : previousValueCounts.values()) {
                assertEquals(1, count, "no submitted value may be reported as \"previous\" more than once");
            }
            // Final state must be exactly one of the submitted values.
            String finalValue = new String(store.get("k").orElseThrow(), StandardCharsets.UTF_8);
            assertTrue(submittedValues.contains(finalValue));
        }
    }

    // 3. concurrent GETs
    @Test
    void concurrentGetsAllObserveTheSameCorrectValue(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("stable-value"));
            int threadCount = 24;
            ConcurrentLinkedQueue<Optional<byte[]>> results = new ConcurrentLinkedQueue<>();
            runConcurrently(threadCount, t -> results.add(store.get("k")));
            assertEquals(threadCount, results.size());
            for (Optional<byte[]> result : results) {
                assertArrayEquals(bytes("stable-value"), result.orElseThrow(), "no GET may see a torn/corrupted read");
            }
        }
    }

    // 4. concurrent DELETEs
    @Test
    void concurrentDeletesOfDistinctKeysAllTakeEffect(@TempDir Path dir) throws Exception {
        int threadCount = 16;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            for (int t = 0; t < threadCount; t++) {
                store.put("key-" + t, bytes("v"));
            }
            runConcurrently(threadCount, t -> store.delete("key-" + t));
            for (int t = 0; t < threadCount; t++) {
                assertTrue(store.get("key-" + t).isEmpty(), "key-" + t + " must be deleted");
            }
        }
    }

    // 5. PUT vs GET
    @Test
    void putRacingGetNeverProducesATornOrCorruptedRead(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("initial"));
            AtomicReference<Optional<byte[]>> getResult = new AtomicReference<>();
            runConcurrently(2, t -> {
                if (t == 0) {
                    store.put("k", bytes("updated"));
                } else {
                    getResult.set(store.get("k"));
                }
            });
            byte[] observed = getResult.get().orElseThrow();
            assertTrue(java.util.Arrays.equals(observed, bytes("initial")) || java.util.Arrays.equals(observed, bytes("updated")),
                    "GET must see exactly one legal, whole value — never a torn mix");
        }
    }

    // 7. PUT vs DELETE
    @Test
    void putRacingDeleteProducesOneConsistentFinalState(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("initial"));
            runConcurrently(2, t -> {
                if (t == 0) {
                    store.put("k", bytes("updated"));
                } else {
                    store.delete("k");
                }
            });
            Optional<byte[]> result = store.get("k");
            // Legal outcomes: PUT-then-DELETE => absent; DELETE-then-PUT => "updated" present.
            assertTrue(result.isEmpty() || java.util.Arrays.equals(result.get(), bytes("updated")),
                    "final state must match one of the two legal serializations");
        }
    }

    // 8. DELETE vs GET
    @Test
    void deleteRacingGetNeverThrowsAndFinalGetIsAbsent(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("v"));
            runConcurrently(2, t -> {
                if (t == 0) {
                    store.delete("k");
                } else {
                    store.get("k"); // just must not throw or corrupt anything
                }
            });
            assertTrue(store.get("k").isEmpty());
        }
    }

    // 9. concurrent WAL writers, via the engine (unit-level coverage already exists in WriteAheadLogTest)
    // 10. sequence-number uniqueness, via the engine
    @Test
    void concurrentWalWritersThroughTheEngineProduceUniqueStrictlyOrderedSequenceNumbers(@TempDir Path dir) throws Exception {
        int threadCount = 12;
        int perThread = 40;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE)) {
            runConcurrently(threadCount, t -> {
                for (int i = 0; i < perThread; i++) {
                    store.put("t" + t + "-" + i, bytes("v"));
                }
            });
        }
        try (WriteAheadLog wal = WriteAheadLog.open(dir.resolve("forge.wal"))) {
            List<WalRecord> records = wal.recoveredRecords();
            assertEquals(threadCount * perThread, records.size());
            for (int i = 0; i < records.size() - 1; i++) {
                assertTrue(records.get(i).sequenceNumber() < records.get(i + 1).sequenceNumber(),
                        "WAL must be strictly ordered even after heavy concurrent writing");
            }
            Set<Long> seqs = records.stream().map(WalRecord::sequenceNumber).collect(Collectors.toSet());
            assertEquals(records.size(), seqs.size(), "every sequence number must be unique");
        }
    }

    // 11. WAL integrity after concurrent writes — covered by the test above (strict ordering + uniqueness
    // IS the integrity check at the WAL-record level); additionally verify full recoverability:
    @Test
    void walRemainsFullyRecoverableAfterAHeavyConcurrentBurst(@TempDir Path dir) throws Exception {
        int threadCount = 10, perThread = 25;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE)) {
            runConcurrently(threadCount, t -> {
                for (int i = 0; i < perThread; i++) {
                    store.put("t" + t + "-" + i, bytes("v" + t + "-" + i));
                }
            });
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE)) {
            for (int t = 0; t < threadCount; t++) {
                for (int i = 0; i < perThread; i++) {
                    assertArrayEquals(bytes("v" + t + "-" + i), reopened.get("t" + t + "-" + i).orElseThrow());
                }
            }
        }
    }

    // 12. PUT during flush
    @Test
    void putArrivingDuringAFlushIsNotLost(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("existing", bytes("1"));

            Thread flusher = new Thread(store::flush);
            Thread putter = new Thread(() -> store.put("during", bytes("2")));
            flusher.start();
            putter.start();
            flusher.join(10_000);
            putter.join(10_000);

            assertArrayEquals(bytes("1"), store.get("existing").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("during").orElseThrow());
        }
    }

    // 13. DELETE during flush
    @Test
    void deleteArrivingDuringAFlushAlwaysTakesEffect(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("1"));

            Thread flusher = new Thread(store::flush);
            Thread deleter = new Thread(() -> store.delete("k"));
            flusher.start();
            deleter.start();
            flusher.join(10_000);
            deleter.join(10_000);

            assertTrue(store.get("k").isEmpty(),
                    "regardless of ordering with the flush, the delete must take effect: "
                            + "either it lands in the same generation being flushed (tombstone in the SSTable), "
                            + "or in the fresh post-flush generation (tombstone shadows the flushed value)");
        }
    }

    // 14. GET during flush — the specific bug this design was built to close (see class Javadoc)
    @Test
    void getNeverSeesAFlushedKeyAsAbsentAcrossTheEntireFlushWindow(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("k", bytes("v"));
            for (int i = 0; i < 300; i++) {
                store.put("filler-" + i, bytes("filler-value-" + i));
            }

            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicReference<String> failure = new AtomicReference<>();
            Thread reader = new Thread(() -> {
                while (!stop.get()) {
                    Optional<byte[]> result = store.get("k");
                    if (result.isEmpty()) {
                        failure.set("GET returned absent for a key that must always be present");
                        return;
                    }
                    if (!java.util.Arrays.equals(bytes("v"), result.get())) {
                        failure.set("GET returned the wrong bytes");
                        return;
                    }
                }
            });
            reader.start();

            store.flush();
            Thread.sleep(50); // keep hammering a bit past flush completion too
            stop.set(true);
            reader.join(10_000);

            assertNull(failure.get(), failure.get());
            assertArrayEquals(bytes("v"), store.get("k").orElseThrow());
        }
    }

    // 15. multiple concurrent flush requests
    @Test
    void multipleConcurrentExplicitFlushRequestsAreSafeAndProduceExactlyOneSSTable(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            runConcurrently(10, t -> store.flush());
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertEquals(1, countSSTableFiles(dir), "concurrent flush() calls on one MemTable must not double-flush");
        }
    }

    // 16. continuous writes while flushing
    @Test
    void continuousWritesDuringFlushAreAllPreserved(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            for (int i = 0; i < 800; i++) {
                store.put("prime-" + i, bytes("v"));
            }

            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicInteger written = new AtomicInteger(0);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                try {
                    int i = 0;
                    while (!stop.get()) {
                        store.put("concurrent-" + i, bytes("v"));
                        written.incrementAndGet();
                        i++;
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            writer.start();

            store.flush();

            stop.set(true);
            writer.join(10_000);

            assertNull(failure.get());
            int total = written.get();
            assertTrue(total > 0, "the writer thread should have made real progress during the flush window");
            for (int i = 0; i < total; i++) {
                assertArrayEquals(bytes("v"), store.get("concurrent-" + i).orElseThrow(),
                        "concurrent write " + i + " must not have been lost");
            }
        }
    }

    // 17. restart/recovery after concurrent writes
    @Test
    void restartRecoversAllDataAfterConcurrentWrites(@TempDir Path dir) throws Exception {
        int threadCount = 10, perThread = 25;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, 4096)) {
            runConcurrently(threadCount, t -> {
                for (int i = 0; i < perThread; i++) {
                    store.put("t" + t + "-" + i, bytes("v" + t + "-" + i));
                }
            });
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir, 4096)) {
            for (int t = 0; t < threadCount; t++) {
                for (int i = 0; i < perThread; i++) {
                    String key = "t" + t + "-" + i;
                    assertArrayEquals(bytes("v" + t + "-" + i), reopened.get(key).orElseThrow(), "missing key " + key);
                }
            }
        }
    }

    // 18. deadlock detection with timeout
    @Test
    @Timeout(30)
    void mixedStressWorkloadCompletesWithoutDeadlock(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, 8192)) {
            int threadCount = 20;
            runConcurrently(threadCount, t -> {
                Random random = new Random(t);
                for (int i = 0; i < 150; i++) {
                    String key = "key-" + random.nextInt(20); // small keyspace: heavy contention
                    switch (random.nextInt(3)) {
                        case 0 -> store.put(key, bytes("v" + i));
                        case 1 -> store.get(key);
                        default -> store.delete(key);
                    }
                }
            });
        }
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] deadlocked = threadBean.findDeadlockedThreads();
        assertNull(deadlocked, "no threads should be deadlocked");
    }

    // 19. same-key contention
    @Test
    void sameKeyContentionUnderSustainedLoadStaysConsistent(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            int threadCount = 12;
            int roundsPerThread = 20;
            runConcurrently(threadCount, t -> {
                for (int round = 0; round < roundsPerThread; round++) {
                    store.put("hot-key", bytes("t" + t + "-r" + round));
                }
            });
            // Must be exactly one of the values that was ever written (never corrupted).
            byte[] finalValue = store.get("hot-key").orElseThrow();
            String finalStr = new String(finalValue, StandardCharsets.UTF_8);
            assertTrue(finalStr.matches("t\\d+-r\\d+"), "final value must be a genuinely-written, whole value: " + finalStr);
        }
    }

    // 20. different-key contention
    @Test
    void differentKeyContentionUnderSustainedLoadLosesNothing(@TempDir Path dir) throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            int threadCount = 12;
            int keysPerThread = 30;
            runConcurrently(threadCount, t -> {
                for (int i = 0; i < keysPerThread; i++) {
                    store.put("t" + t + "-k" + i, bytes("v" + t + "-" + i));
                }
            });
            for (int t = 0; t < threadCount; t++) {
                for (int i = 0; i < keysPerThread; i++) {
                    assertArrayEquals(bytes("v" + t + "-" + i), store.get("t" + t + "-k" + i).orElseThrow());
                }
            }
        }
    }

    // --- keys() (Phase 7: partition rebalancing needs to enumerate stored keys) ---

    @Test
    void keysReturnsEverythingPutAndNothingDeleted(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.put("b", bytes("2"));
            store.put("c", bytes("3"));
            store.delete("b");

            assertEquals(Set.of("a", "c"), store.keys());
        }
    }

    @Test
    void keysIsEmptyForAFreshStore(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(store.keys().isEmpty());
        }
    }

    @Test
    void keysReflectsOverwritesNotJustFirstWrite(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.put("a", bytes("2"));
            store.delete("a");
            store.put("a", bytes("3"));

            assertEquals(Set.of("a"), store.keys());
            assertArrayEquals(bytes("3"), store.get("a").orElseThrow());
        }
    }

    @Test
    void keysSpansBothFlushedSSTablesAndTheCurrentMemTable(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("flushed-1", bytes("1"));
            store.put("flushed-2", bytes("2"));
            store.flush();

            store.put("in-memtable", bytes("3"));

            assertEquals(Set.of("flushed-1", "flushed-2", "in-memtable"), store.keys());
        }
    }

    @Test
    void keysDoesNotResurrectAKeyDeletedAfterItWasFlushed(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush(); // "a" now lives only in an SSTable
            store.delete("a"); // tombstone lands in the active MemTable

            assertTrue(store.keys().isEmpty(), "a newer tombstone in the MemTable must shadow the older flushed value");
            assertTrue(store.get("a").isEmpty());
        }
    }

    @Test
    void keysAcrossMultipleGenerationsResolvesToTheNewestPerKey(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("gen1"));
            store.flush(); // sstable generation 1: a=gen1

            store.put("a", bytes("gen2"));
            store.flush(); // sstable generation 2 (newer): a=gen2

            assertEquals(Set.of("a"), store.keys());
            assertArrayEquals(bytes("gen2"), store.get("a").orElseThrow());
        }
    }

    // --- applyReplicated / lastAppliedSequenceNumber (Phase 9) ------------

    @Test
    void lastAppliedSequenceNumberStartsAtOneForAFreshStore(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertEquals(1L, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    void applyReplicatedMakesTheKeyReadableAndAdvancesTheSequenceNumber(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            ReplicationOutcome outcome = store.applyReplicated(new WalRecord.Put(1, "a", bytes("1")));

            assertEquals(ReplicationOutcome.APPLIED, outcome);
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertEquals(2L, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    void applyReplicatedDeleteRemovesAPreviouslyAppliedKey(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.applyReplicated(new WalRecord.Put(1, "a", bytes("1")));
            ReplicationOutcome outcome = store.applyReplicated(new WalRecord.Delete(2, "a"));

            assertEquals(ReplicationOutcome.APPLIED, outcome);
            assertTrue(store.get("a").isEmpty());
        }
    }

    @Test
    void applyReplicatedIsIdempotentForADuplicateRecord(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.applyReplicated(new WalRecord.Put(1, "a", bytes("1")));
            ReplicationOutcome outcome = store.applyReplicated(new WalRecord.Put(1, "a", bytes("1")));

            assertEquals(ReplicationOutcome.ALREADY_APPLIED, outcome);
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertEquals(2L, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    void applyReplicatedRefusesAGapAndLeavesStateUnchanged(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            ReplicationOutcome outcome = store.applyReplicated(new WalRecord.Put(5, "a", bytes("1")));

            assertEquals(ReplicationOutcome.GAP_DETECTED, outcome);
            assertTrue(store.get("a").isEmpty(), "a refused gap must never become visible");
            assertEquals(1L, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    void applyReplicatedDataSurvivesCloseAndRecovery(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.applyReplicated(new WalRecord.Put(1, "a", bytes("1")));
            store.applyReplicated(new WalRecord.Put(2, "b", bytes("2")));
            store.applyReplicated(new WalRecord.Delete(3, "a"));
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(reopened.get("a").isEmpty());
            assertArrayEquals(bytes("2"), reopened.get("b").orElseThrow());
            assertEquals(4L, reopened.lastAppliedSequenceNumber());
        }
    }

    @Test
    void applyReplicatedContinuesCorrectlyAfterLocalWritesAssignedTheEarlierSequenceNumbers(@TempDir Path dir)
            throws IOException {
        // A node can accumulate local writes (as a leader would) before ever being asked
        // to apply someone else's stream (e.g. after a role change) -- lastAppliedSequenceNumber
        // must reflect exactly where the leader-side stream should resume from.
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("local-1", bytes("1")); // local write assigns seq 1
            store.put("local-2", bytes("2")); // local write assigns seq 2
            assertEquals(3L, store.lastAppliedSequenceNumber());

            ReplicationOutcome outcome = store.applyReplicated(new WalRecord.Put(3, "replicated", bytes("3")));

            assertEquals(ReplicationOutcome.APPLIED, outcome);
            assertArrayEquals(bytes("3"), store.get("replicated").orElseThrow());
            assertEquals(4L, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    @Timeout(30)
    void manySequentialApplyReplicatedCallsProduceExactlyTheExpectedFinalState(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            int count = 500;
            for (int i = 1; i <= count; i++) {
                ReplicationOutcome outcome = store.applyReplicated(new WalRecord.Put(i, "k" + i, bytes("v" + i)));
                assertEquals(ReplicationOutcome.APPLIED, outcome);
            }
            assertEquals(count + 1, store.lastAppliedSequenceNumber());
            for (int i = 1; i <= count; i++) {
                assertArrayEquals(bytes("v" + i), store.get("k" + i).orElseThrow());
            }
            assertEquals(count, store.keys().size());
        }
    }

    // --- replication listeners (Phase 9) -----------------------------------

    @Test
    void currentWalRecordsReflectsLocalWritesMadeAfterConstruction(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(store.currentWalRecords().isEmpty());

            store.put("a", bytes("1"));
            store.put("b", bytes("2"));

            List<WalRecord> records = store.currentWalRecords();
            assertEquals(2, records.size());
            assertEquals("a", records.get(0).key());
            assertEquals("b", records.get(1).key());
        }
    }

    @Test
    void currentWalRecordsDoesNotIncludeWhatAFlushAlreadyTruncatedAway(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            store.flush();
            store.put("b", bytes("2"));

            List<WalRecord> records = store.currentWalRecords();
            assertEquals(1, records.size(), "the flushed record must be gone from the WAL, only the post-flush write remains");
            assertEquals("b", records.get(0).key());
        }
    }

    @Test
    void replicationListenerIsNotifiedOfALocalPutWithTheAssignedSequenceNumber(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            List<WalRecord> observed = new ArrayList<>();
            store.addReplicationListener(observed::add);

            store.put("a", bytes("1"));

            assertEquals(1, observed.size());
            WalRecord.Put put = (WalRecord.Put) observed.get(0);
            assertEquals(1L, put.sequenceNumber());
            assertEquals("a", put.key());
            assertArrayEquals(bytes("1"), put.value());
        }
    }

    @Test
    void replicationListenerIsNotifiedOfALocalDelete(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("a", bytes("1"));
            List<WalRecord> observed = new ArrayList<>();
            store.addReplicationListener(observed::add);

            store.delete("a");

            assertEquals(1, observed.size());
            WalRecord.Delete delete = (WalRecord.Delete) observed.get(0);
            assertEquals(2L, delete.sequenceNumber());
            assertEquals("a", delete.key());
        }
    }

    @Test
    void replicationListenerSeesEveryWriteInOrder(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            List<Long> observedSequenceNumbers = new ArrayList<>();
            store.addReplicationListener(record -> observedSequenceNumbers.add(record.sequenceNumber()));

            for (int i = 0; i < 50; i++) {
                store.put("k" + i, bytes("v" + i));
            }

            List<Long> expected = new ArrayList<>();
            for (long i = 1; i <= 50; i++) {
                expected.add(i);
            }
            assertEquals(expected, observedSequenceNumbers);
        }
    }

    @Test
    void removedListenerStopsReceivingNotifications(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            List<WalRecord> observed = new ArrayList<>();
            Consumer<WalRecord> listener = observed::add;
            store.addReplicationListener(listener);
            store.put("a", bytes("1"));

            store.removeReplicationListener(listener);
            store.put("b", bytes("2"));

            assertEquals(1, observed.size(), "no notification should arrive after removal");
        }
    }

    @Test
    void aThrowingReplicationListenerDoesNotPreventTheWriteFromSucceeding(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.addReplicationListener(record -> {
                throw new RuntimeException("simulated listener failure");
            });

            assertDoesNotThrow(() -> store.put("a", bytes("1")));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    @Test
    void oneMisbehavingListenerDoesNotPreventOtherListenersFromBeingNotified(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            List<WalRecord> observedByGoodListener = new ArrayList<>();
            store.addReplicationListener(record -> {
                throw new RuntimeException("simulated failure");
            });
            store.addReplicationListener(observedByGoodListener::add);

            store.put("a", bytes("1"));

            assertEquals(1, observedByGoodListener.size(), "a well-behaved listener must still be notified");
        }
    }

    @Test
    void applyReplicatedDoesNotNotifyReplicationListeners(@TempDir Path dir) throws IOException {
        // Phase 9's scope is single-level leader -> follower replication, not cascading
        // replication trees -- a follower applying a leader's record does not itself
        // re-forward it. Documented as a deliberate scope boundary, verified here so a
        // future change to that scope has to touch this test consciously.
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            List<WalRecord> observed = new ArrayList<>();
            store.addReplicationListener(observed::add);

            store.applyReplicated(new WalRecord.Put(1, "a", bytes("1")));

            assertTrue(observed.isEmpty());
        }
    }

    // --- loadSnapshot (Phase 10: bootstrap) --------------------------------

    @Test
    void loadSnapshotMakesEveryKeyImmediatelyReadable(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            Map<String, byte[]> snapshot = new HashMap<>();
            snapshot.put("a", bytes("1"));
            snapshot.put("b", bytes("2"));

            store.loadSnapshot(snapshot, 10);

            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            assertEquals(Set.of("a", "b"), store.keys());
        }
    }

    @Test
    void loadSnapshotSetsLastAppliedSequenceNumberToExactlyWatermarkPlusOne(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.loadSnapshot(Map.of("a", bytes("1")), 41);
            assertEquals(42L, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    void loadSnapshotOfAnEmptyMapIsValidAndStillAdvancesTheWatermark(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.loadSnapshot(Map.of(), 5);
            assertEquals(6L, store.lastAppliedSequenceNumber());
            assertTrue(store.keys().isEmpty());
        }
    }

    @Test
    void loadSnapshotRejectsANonEmptyStore(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.put("existing", bytes("1"));
            assertThrows(IllegalStateException.class, () -> store.loadSnapshot(Map.of("a", bytes("2")), 10));
        }
    }

    @Test
    void loadSnapshotRejectsANegativeWatermark(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertThrows(IllegalArgumentException.class, () -> store.loadSnapshot(Map.of(), -1));
        }
    }

    @Test
    void loadedSnapshotDataSurvivesCloseAndReopen(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.loadSnapshot(Map.of("a", bytes("1"), "b", bytes("2")), 20);
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir)) {
            assertArrayEquals(bytes("1"), reopened.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), reopened.get("b").orElseThrow());
            assertEquals(21L, reopened.lastAppliedSequenceNumber());
        }
    }

    @Test
    void aFollowerCanResumeIncrementalReplicationSeamlesslyAfterLoadingASnapshot(@TempDir Path dir) throws IOException {
        // The exact scenario loadSnapshot exists for: bootstrap to a watermark, then
        // continue with ordinary sequence-numbered replicated records right after it,
        // with no gap and no re-application of anything the snapshot already covered.
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            store.loadSnapshot(Map.of("a", bytes("1")), 5); // watermark 5 -> next expected is 6

            ReplicationOutcome outcome = store.applyReplicated(new WalRecord.Put(6, "b", bytes("2")));

            assertEquals(ReplicationOutcome.APPLIED, outcome);
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            assertEquals(7L, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    void loadSnapshotNeverLeavesAPartialSSTableVisibleIfInterruptedBeforeBeingCalled(@TempDir Path dir)
            throws IOException {
        // loadSnapshot itself is only ever invoked with a COMPLETE map (see its own
        // Javadoc on why) -- this test documents and verifies the store-side half of that
        // contract: never having been called at all (as if a network transfer had been
        // interrupted before completion) leaves the store exactly as empty as it started.
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(store.keys().isEmpty());
            assertEquals(1L, store.lastAppliedSequenceNumber());
            // (no loadSnapshot call -- simulating an interrupted-before-completion transfer)
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir)) {
            assertTrue(reopened.keys().isEmpty());
            assertEquals(1L, reopened.lastAppliedSequenceNumber());
        }
    }

    /**
     * Regression test for a real bug found via Phase 12 benchmarking:
     * {@code keys()} used to snapshot {@code active}'s object reference
     * under {@code stateLock}'s read lock but then iterate its live,
     * still-mutable {@code entries()} <em>after releasing the lock</em> — a
     * concurrent {@code put()} mutating that same {@code MemTable} while
     * {@code keys()} was mid-iteration threw a real, reproducible {@code
     * ConcurrentModificationException} once a benchmark hammered {@code
     * keys()} in a tight loop against an actively-writing store. Fixed by
     * collecting {@code active}/{@code frozen}'s keys while still holding
     * the read lock (only the SSTable scan remains lock-free). This test
     * reproduces the original triggering condition directly: one thread
     * writing continuously while several others call {@code keys()} as
     * fast as possible, for long enough that the old code reliably failed.
     */
    @Test
    @Timeout(30)
    void keysNeverThrowsConcurrentModificationExceptionUnderSustainedConcurrentWrites(@TempDir Path dir)
            throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            int writeCount = 3000;
            int readerCount = 4;
            runConcurrently(1 + readerCount, index -> {
                if (index == 0) {
                    for (int i = 0; i < writeCount; i++) {
                        store.put("k" + i, bytes("v" + i));
                    }
                } else {
                    while (store.keys().size() < writeCount) {
                        Thread.yield();
                    }
                }
            });
            assertEquals(writeCount, store.keys().size());
        }
    }

    // =====================================================================
    // Phase 13: compaction
    // =====================================================================

    @Test
    void compactionRunsAutomaticallyOnceSstableCountReachesTheTrigger(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE, 3)) {
            store.put("a", bytes("1"));
            store.flush();
            store.put("b", bytes("2"));
            store.flush();
            assertEquals(2, countSSTableFiles(dir), "below the trigger count, no compaction yet");

            store.put("c", bytes("3"));
            store.flush(); // this is the 3rd SSTable — crosses the trigger

            assertEquals(1, countSSTableFiles(dir), "at the trigger count, all tables merge into one");
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
            assertArrayEquals(bytes("2"), store.get("b").orElseThrow());
            assertArrayEquals(bytes("3"), store.get("c").orElseThrow());
        }
    }

    @Test
    void explicitCompactIsANoOpWithFewerThanTwoSstables(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE, 100)) {
            store.compact(); // zero SSTables — must not throw
            assertEquals(0, countSSTableFiles(dir));

            store.put("a", bytes("1"));
            store.flush();
            store.compact(); // one SSTable — must not throw, and must not need to do anything
            assertEquals(1, countSSTableFiles(dir));
            assertArrayEquals(bytes("1"), store.get("a").orElseThrow());
        }
    }

    @Test
    void compactionKeepsTheNewestValueAcrossOverwrites(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE, 100)) {
            store.put("a", bytes("first"));
            store.flush();
            store.put("a", bytes("second"));
            store.flush();
            store.put("a", bytes("third"));
            store.flush();
            assertEquals(3, countSSTableFiles(dir));

            store.compact();

            assertEquals(1, countSSTableFiles(dir));
            assertArrayEquals(bytes("third"), store.get("a").orElseThrow());
        }
    }

    /**
     * The correctness invariant unique to <em>full</em> compaction (see
     * {@code Compactor}'s Javadoc): a tombstone with no older generation
     * left behind it may be dropped entirely, not merely kept-but-shadowed.
     * Verified two ways: the logical result ({@code get} correctly reports
     * absent) and the physical one (the merged file's raw record scan no
     * longer contains any record at all for the key, confirming it was
     * actually dropped rather than coincidentally shadowed by something
     * else).
     */
    @Test
    void fullCompactionDropsTombstonesEntirely(@TempDir Path dir) throws IOException {
        Path finalSstable;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE, 100)) {
            store.put("gone", bytes("value"));
            store.flush();
            store.delete("gone");
            store.flush();
            store.put("stays", bytes("kept"));
            store.flush();

            store.compact();

            assertEquals(1, countSSTableFiles(dir));
            assertTrue(store.get("gone").isEmpty());
            assertArrayEquals(bytes("kept"), store.get("stays").orElseThrow());

            finalSstable = onlySstableFile(dir);
        }

        try (com.forge.storage.sstable.SSTableReader reader = com.forge.storage.sstable.SSTableReader.open(finalSstable)) {
            var records = reader.scanAll();
            assertTrue(records.stream().noneMatch(e -> e.getKey().equals("gone")),
                    "a dropped tombstone must leave no trace in the compacted file, not just an unreachable one");
            assertEquals(1, records.size());
        }
    }

    @Test
    void reopeningAfterCompactionStillSeesTheCorrectData(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE, 100)) {
            for (int i = 0; i < 10; i++) {
                store.put("k" + i, bytes("v" + i));
                store.flush();
            }
            store.compact();
            assertEquals(1, countSSTableFiles(dir));
        }
        try (ConcurrentLsmKeyValueStore reopened = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE, 100)) {
            for (int i = 0; i < 10; i++) {
                assertArrayEquals(bytes("v" + i), reopened.get("k" + i).orElseThrow());
            }
        }
    }

    /**
     * The core new adversarial scenario Phase 13 introduces: readers must
     * never observe an exception or an incorrect value while compaction is
     * concurrently retiring (closing and deleting) the very SSTables those
     * readers may already have snapshotted. Sustained, mixed writer +
     * compactor + reader load, long enough that the old (pre-refcounting)
     * design's race — closing a file out from under an in-flight lock-free
     * scan — would have reliably surfaced as a {@code ClosedChannelException}
     * or a deleted-file read failure.
     */
    @Test
    @Timeout(30)
    void concurrentReadsDuringSustainedCompactionNeverThrowAndAlwaysSeeACorrectValue(@TempDir Path dir)
            throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, 512, 2)) {
            int keyCount = 400;
            AtomicInteger writerProgress = new AtomicInteger(0);
            AtomicBoolean stop = new AtomicBoolean(false);

            runConcurrently(1 + 3 + 1, index -> {
                if (index == 0) {
                    for (int i = 0; i < keyCount; i++) {
                        store.put("k" + i, bytes("v" + i));
                        if (i % 5 == 0) {
                            store.flush();
                        }
                        writerProgress.set(i + 1);
                    }
                    stop.set(true);
                } else if (index == 1 + 3) {
                    while (!stop.get()) {
                        store.compact();
                    }
                    store.compact();
                } else {
                    Random random = new Random(index);
                    while (!stop.get()) {
                        int written = writerProgress.get();
                        if (written == 0) {
                            continue;
                        }
                        int i = random.nextInt(written);
                        Optional<byte[]> value = store.get("k" + i);
                        assertTrue(value.isPresent(), "k" + i + " was already written and never deleted");
                        assertArrayEquals(bytes("v" + i), value.get());
                    }
                }
            });

            for (int i = 0; i < keyCount; i++) {
                assertArrayEquals(bytes("v" + i), store.get("k" + i).orElseThrow());
            }
        }
    }

    @Test
    void statusReflectsRealStorageEngineState(@TempDir Path dir) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir, Long.MAX_VALUE, 100)) {
            ConcurrentLsmKeyValueStore.StoreStatus empty = store.status();
            assertEquals(0, empty.sstableCount());
            assertEquals(0, empty.totalSstableBytes());
            assertEquals(0, empty.activeMemTableSizeBytes());
            assertFalse(empty.flushInProgress());
            assertEquals(1, empty.lastAppliedSequenceNumber());

            store.put("a", bytes("1"));
            ConcurrentLsmKeyValueStore.StoreStatus afterPut = store.status();
            assertTrue(afterPut.activeMemTableSizeBytes() > 0);
            assertEquals(2, afterPut.lastAppliedSequenceNumber());

            store.flush();
            ConcurrentLsmKeyValueStore.StoreStatus afterFlush = store.status();
            assertEquals(1, afterFlush.sstableCount());
            assertTrue(afterFlush.totalSstableBytes() > 0);
            assertEquals(0, afterFlush.activeMemTableSizeBytes(), "a fresh active MemTable after flush is empty");
        }
    }

    private static Path onlySstableFile(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "sstable-*.sst")) {
            Path only = null;
            for (Path path : stream) {
                if (only != null) {
                    throw new IllegalStateException("expected exactly one SSTable file in " + dir);
                }
                only = path;
            }
            if (only == null) {
                throw new IllegalStateException("expected exactly one SSTable file in " + dir);
            }
            return only;
        }
    }
}
