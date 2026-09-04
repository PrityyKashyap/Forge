package com.forge.tests;

import com.forge.client.ForgeClient;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 6 correctness requirement: concurrent clients writing to the
 * <b>same</b> keys must not corrupt state. Phase 5's concurrency tests used
 * disjoint per-client key ranges specifically to isolate concurrency-scaling
 * behavior from same-key contention (see {@code ConcurrentClientsIntegrationTest}
 * and {@code ForgeServerTest}'s comments) — this test is the deliberately
 * separate one that targets contention itself.
 *
 * <p>Every write's value is a distinctive, self-describing string
 * ({@code "client-<id>-seq-<n>"}). After the race, each shared key's final
 * value must match that pattern exactly — a torn or mixed value (e.g. bytes
 * from two different writes spliced together) would fail the regex, which a
 * generic "response received OK" check would never catch.
 */
class SharedKeyContentionIntegrationTest {

    private static final int CLIENT_COUNT = 12;
    private static final int WRITES_PER_CLIENT = 150;
    private static final int SHARED_KEY_COUNT = 5;
    private static final Pattern VALID_VALUE = Pattern.compile("^client-(\\d+)-seq-(\\d+)$");

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String sharedKey(int i) {
        return "contended-key-" + i;
    }

    @Test
    @Timeout(60)
    void concurrentWritersToTheSameSmallKeySetNeverProduceATornOrCorruptedValue(@TempDir Path dataDir)
            throws Exception {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
                ForgeServer server = new ForgeServer(store, 0)) {
            String host = "localhost";
            int port = server.port();

            ExecutorService pool = Executors.newFixedThreadPool(CLIENT_COUNT);
            CountDownLatch ready = new CountDownLatch(CLIENT_COUNT);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Void>> futures = new java.util.ArrayList<>();

            for (int c = 0; c < CLIENT_COUNT; c++) {
                int clientId = c;
                futures.add(pool.submit(() -> {
                    try (ForgeClient client = ForgeClient.connect(host, port)) {
                        ready.countDown();
                        go.await();
                        for (int i = 0; i < WRITES_PER_CLIENT; i++) {
                            String key = sharedKey(i % SHARED_KEY_COUNT);
                            String value = "client-" + clientId + "-seq-" + i;
                            client.put(key, bytes(value));
                            // A concurrent read against contended keys must always
                            // succeed and return something well-formed — never throw,
                            // never hang, never return a malformed/partial value.
                            Optional<byte[]> got = client.get(key);
                            assertTrue(got.isPresent(), "contended key must always have some value once written");
                            String observed = new String(got.get(), StandardCharsets.UTF_8);
                            Matcher matcher = VALID_VALUE.matcher(observed);
                            assertTrue(matcher.matches(),
                                    "observed value \"" + observed + "\" for key " + key
                                            + " is not a well-formed write from any client — looks corrupted");
                        }
                    }
                    return null;
                }));
            }

            ready.await();
            go.countDown();
            for (Future<Void> f : futures) {
                f.get(45, TimeUnit.SECONDS);
            }
            pool.shutdown();

            // Final state: every shared key must hold exactly one well-formed,
            // real write — never a blend of two.
            try (ForgeClient verifier = ForgeClient.connect(host, port)) {
                for (int i = 0; i < SHARED_KEY_COUNT; i++) {
                    Optional<byte[]> result = verifier.get(sharedKey(i));
                    assertTrue(result.isPresent(), "shared key " + i + " must be present after the race");
                    String finalValue = new String(result.get(), StandardCharsets.UTF_8);
                    Matcher matcher = VALID_VALUE.matcher(finalValue);
                    assertTrue(matcher.matches(),
                            "final value \"" + finalValue + "\" for shared key " + i + " is not well-formed — looks corrupted");
                    int clientId = Integer.parseInt(matcher.group(1));
                    int seq = Integer.parseInt(matcher.group(2));
                    assertTrue(clientId >= 0 && clientId < CLIENT_COUNT, "client id in final value must be one that actually ran");
                    assertTrue(seq >= 0 && seq < WRITES_PER_CLIENT, "sequence number in final value must be one that actually ran");
                }
            }
        }
    }
}
