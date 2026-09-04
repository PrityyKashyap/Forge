package com.forge.tests;

import com.forge.client.ForgeClient;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Many real {@link ForgeClient} connections hitting one real
 * {@link ForgeServer} concurrently, on top of the real
 * {@link ConcurrentLsmKeyValueStore} — the scenario the whole Phase 5
 * connection model (one virtual thread per connection) exists for.
 */
class ConcurrentClientsIntegrationTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @Timeout(60)
    void manyClientsOnSeparateConnectionsCanReadAndWriteConcurrentlyWithoutCorruption(@TempDir Path dataDir)
            throws Exception {
        int clientCount = 32;
        int opsPerClient = 100;

        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
             ForgeServer server = new ForgeServer(store, 0)) {

            ExecutorService pool = Executors.newFixedThreadPool(clientCount);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Void>> futures = new ArrayList<>();

            for (int c = 0; c < clientCount; c++) {
                int clientId = c;
                futures.add(pool.submit(() -> {
                    try (ForgeClient client = ForgeClient.connect("localhost", server.port())) {
                        start.await();
                        for (int i = 0; i < opsPerClient; i++) {
                            String key = "client-" + clientId + "-key-" + i;
                            String value = "client-" + clientId + "-value-" + i;
                            client.put(key, bytes(value));

                            Optional<byte[]> result = client.get(key);
                            assertTrue(result.isPresent(), "expected " + key + " to be present immediately after put");
                            assertArrayEquals(bytes(value), result.get());
                        }
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<Void> f : futures) {
                f.get(50, TimeUnit.SECONDS);
            }
            pool.shutdown();

            // A fresh connection must see every write from every other connection.
            try (ForgeClient verifier = ForgeClient.connect("localhost", server.port())) {
                for (int c = 0; c < clientCount; c++) {
                    for (int i = 0; i < opsPerClient; i++) {
                        String key = "client-" + c + "-key-" + i;
                        String expected = "client-" + c + "-value-" + i;
                        Optional<byte[]> result = verifier.get(key);
                        assertTrue(result.isPresent(), "expected " + key + " to be durably visible");
                        assertArrayEquals(bytes(expected), result.get());
                    }
                }
            }
        }
    }

    @Test
    @Timeout(30)
    void manyClientsForceAFlushDuringConcurrentTrafficWithoutLosingData(@TempDir Path dataDir) throws Exception {
        int clientCount = 8;
        int opsPerClient = 200;
        // Small threshold so flushes actually happen during the test, not just at teardown.
        long tinyFlushThreshold = 8 * 1024;

        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir, tinyFlushThreshold);
             ForgeServer server = new ForgeServer(store, 0)) {

            ExecutorService pool = Executors.newFixedThreadPool(clientCount);
            List<Future<Void>> futures = new ArrayList<>();

            for (int c = 0; c < clientCount; c++) {
                int clientId = c;
                futures.add(pool.submit(() -> {
                    try (ForgeClient client = ForgeClient.connect("localhost", server.port())) {
                        for (int i = 0; i < opsPerClient; i++) {
                            String key = "flush-client-" + clientId + "-key-" + i;
                            client.put(key, bytes("value-" + i));
                        }
                    }
                    return null;
                }));
            }
            for (Future<Void> f : futures) {
                f.get(25, TimeUnit.SECONDS);
            }
            pool.shutdown();

            try (ForgeClient verifier = ForgeClient.connect("localhost", server.port())) {
                for (int c = 0; c < clientCount; c++) {
                    for (int i = 0; i < opsPerClient; i++) {
                        String key = "flush-client-" + c + "-key-" + i;
                        Optional<byte[]> result = verifier.get(key);
                        assertTrue(result.isPresent(), "expected " + key + " to survive concurrent flushing");
                        assertArrayEquals(bytes("value-" + i), result.get());
                    }
                }
            }
        }
    }
}
