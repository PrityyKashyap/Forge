package com.forge.tests;

import com.forge.client.ForgeClient;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 6 correctness requirement: every request receives exactly one
 * response, and each response corresponds to the request that produced it —
 * not a structural given (a buggy connection handler could, in principle,
 * fail to consume a request's bytes fully, or send a response for the wrong
 * request), so this is verified directly at real volume rather than assumed
 * from {@code ConnectionHandler}'s sequential-loop design.
 *
 * <p>Runs a long, deterministic, varied sequence of PUT/GET/DELETE calls on
 * a single connection and checks each response against exactly what that
 * specific request should have produced, given everything before it.
 */
class RequestResponsePairingStressTest {

    private static final int SEQUENCE_LENGTH = 1000;

    private ConcurrentLsmKeyValueStore store;
    private ForgeServer server;
    private ForgeClient client;

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        if (store != null) {
            store.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @Timeout(60)
    void everyResponseOnOneConnectionMatchesItsOwnRequestAcrossAVariedSequence(@TempDir Path dataDir)
            throws IOException {
        store = new ConcurrentLsmKeyValueStore(dataDir);
        server = new ForgeServer(store, 0);
        client = ForgeClient.connect("localhost", server.port());

        for (int i = 0; i < SEQUENCE_LENGTH; i++) {
            String key = "pair-key-" + (i % 37); // deliberately small keyspace: forces overwrites and re-deletes
            String value = "value-for-request-" + i;

            switch (i % 4) {
                case 0 -> {
                    // PUT then immediately GET: response must reflect exactly this PUT.
                    client.put(key, bytes(value));
                    Optional<byte[]> got = client.get(key);
                    assertTrue(got.isPresent(), "request " + i + ": key just PUT must be present");
                    assertArrayEquals(bytes(value), got.get(), "request " + i + ": GET must return this exact PUT's value");
                }
                case 1 -> {
                    // PUT, DELETE, then GET: response must reflect the DELETE, not the PUT.
                    client.put(key, bytes(value));
                    client.delete(key);
                    Optional<byte[]> got = client.get(key);
                    assertFalse(got.isPresent(), "request " + i + ": key just DELETEd must be absent");
                }
                case 2 -> {
                    // Overwrite: two PUTs to the same key, GET must reflect the second, not the first.
                    client.put(key, bytes("stale-" + i));
                    client.put(key, bytes(value));
                    Optional<byte[]> got = client.get(key);
                    assertTrue(got.isPresent());
                    assertArrayEquals(bytes(value), got.get(), "request " + i + ": GET must reflect the overwrite, not the stale value");
                }
                case 3 -> {
                    // GET on a key this iteration never wrote: must not spuriously
                    // return some OTHER request's value (the pairing bug this
                    // whole test targets would most plausibly show up as this).
                    String untouchedKey = "pair-key-untouched-" + i;
                    Optional<byte[]> got = client.get(untouchedKey);
                    assertFalse(got.isPresent(), "request " + i + ": never-written key must be absent, not some other request's value");
                }
                default -> throw new IllegalStateException("unreachable");
            }
        }
    }
}
