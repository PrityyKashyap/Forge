package com.forge.tests;

import com.forge.client.ForgeClient;
import com.forge.client.ForgeServerException;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests of a real {@link ForgeClient} talking to a real
 * {@link ForgeServer} over an actual TCP socket, on top of the real,
 * durable {@link ConcurrentLsmKeyValueStore}. This is the one place in the
 * whole project where both sides of the wire protocol run for real at once.
 */
class ClientServerIntegrationTest {

    private ConcurrentLsmKeyValueStore store;
    private ForgeServer server;
    private ForgeClient client;

    private void startServer(Path dataDir) throws IOException {
        store = new ConcurrentLsmKeyValueStore(dataDir);
        server = new ForgeServer(store, 0);
    }

    private ForgeClient connect() throws IOException {
        return ForgeClient.connect("localhost", server.port());
    }

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
    void putThenGetRoundTripsThroughARealClientAndServer(@TempDir Path dataDir) throws IOException {
        startServer(dataDir);
        client = connect();

        client.put("k", bytes("v"));
        Optional<byte[]> result = client.get("k");

        assertTrue(result.isPresent());
        assertArrayEquals(bytes("v"), result.get());
    }

    @Test
    void getOnAbsentKeyReturnsEmpty(@TempDir Path dataDir) throws IOException {
        startServer(dataDir);
        client = connect();

        assertFalse(client.get("missing").isPresent());
    }

    @Test
    void deleteRemovesAKeyThatWasPreviouslySet(@TempDir Path dataDir) throws IOException {
        startServer(dataDir);
        client = connect();

        client.put("k", bytes("v"));
        client.delete("k");

        assertFalse(client.get("k").isPresent());
    }

    @Test
    void dataWrittenThroughTheClientSurvivesAServerRestartOverTheSameDataDirectory(@TempDir Path dataDir)
            throws IOException {
        startServer(dataDir);
        client = connect();
        client.put("durable-key", bytes("durable-value"));
        client.close();
        client = null;
        server.close();
        store.close();

        startServer(dataDir); // reopens the same on-disk data directory
        client = connect();

        Optional<byte[]> result = client.get("durable-key");
        assertTrue(result.isPresent());
        assertArrayEquals(bytes("durable-value"), result.get());
    }

    @Test
    void secondClientOnASeparateConnectionSeesWritesFromTheFirst(@TempDir Path dataDir) throws IOException {
        startServer(dataDir);
        client = connect();
        client.put("shared-key", bytes("shared-value"));

        try (ForgeClient secondClient = connect()) {
            Optional<byte[]> result = secondClient.get("shared-key");
            assertTrue(result.isPresent());
            assertArrayEquals(bytes("shared-value"), result.get());
        }
    }

    @Test
    void gettingAnAbsentKeyRepeatedlyOnOneConnectionIsStable(@TempDir Path dataDir) throws IOException {
        startServer(dataDir);
        client = connect();

        for (int i = 0; i < 50; i++) {
            assertFalse(client.get("never-set-" + i).isPresent());
        }
    }

    @Test
    void largeValueRoundTrips(@TempDir Path dataDir) throws IOException {
        startServer(dataDir);
        client = connect();

        byte[] large = new byte[1024 * 1024];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) (i % 251);
        }

        client.put("large", large);
        Optional<byte[]> result = client.get("large");

        assertTrue(result.isPresent());
        assertArrayEquals(large, result.get());
    }

    @Test
    void oversizedRequestFromARealClientSurfacesAsAForgeServerExceptionAndClosesTheConnection(
            @TempDir Path dataDir) throws IOException {
        store = new ConcurrentLsmKeyValueStore(dataDir);
        int tinyMaxFrame = 64;
        server = new ForgeServer(store, 0, tinyMaxFrame, tinyMaxFrame);
        client = ForgeClient.connect("localhost", server.port(), 10 * tinyMaxFrame);

        byte[] tooLarge = new byte[tinyMaxFrame * 4];

        ForgeServerException e = assertThrows(ForgeServerException.class, () -> client.put("k", tooLarge));
        assertEquals(ProtocolConstants.ERROR_OVERSIZED_REQUEST, e.errorCode());

        // The connection is now closed server-side; a further call on the
        // same client must fail rather than hang.
        assertThrows(IOException.class, () -> client.get("k"));
    }
}
