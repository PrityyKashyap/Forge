package com.forge.server;

import com.forge.common.protocol.FrameCodec;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.common.protocol.Request;
import com.forge.common.protocol.Response;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import com.forge.storage.InMemoryKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These tests speak the wire protocol directly over raw {@link Socket}s
 * rather than through {@code ForgeClient} — {@code forge-server} must not
 * depend on {@code forge-client}, even in test scope. Real client/server
 * integration is covered by the {@code forge-tests} module.
 */
class ForgeServerTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putThenGetRoundTrips() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Put("k", bytes("v")));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));

            FrameCodec.writeRequest(out, new Request.Get("k"));
            Response response = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
            Response.OkPresent present = assertInstanceOf(Response.OkPresent.class, response);
            assertArrayEquals(bytes("v"), present.value());
        }
    }

    @Test
    void getOnAbsentKeyReturnsOkAbsent() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Get("missing"));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
        }
    }

    @Test
    void deleteRemovesKeyAndSubsequentGetReturnsAbsent() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Put("k", bytes("v")));
            FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);

            FrameCodec.writeRequest(out, new Request.Delete("k"));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));

            FrameCodec.writeRequest(out, new Request.Get("k"));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
        }
    }

    @Test
    void deleteOnAbsentKeyIsIdempotent() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Delete("never-existed"));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
        }
    }

    @Test
    void manySequentialRequestsOnOneConnectionAllSucceed() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            for (int i = 0; i < 200; i++) {
                FrameCodec.writeRequest(out, new Request.Put("k" + i, bytes("v" + i)));
                assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
            }
            for (int i = 0; i < 200; i++) {
                FrameCodec.writeRequest(out, new Request.Get("k" + i));
                Response response = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
                Response.OkPresent present = assertInstanceOf(Response.OkPresent.class, response);
                assertArrayEquals(bytes("v" + i), present.value());
            }
        }
    }

    @Test
    @Timeout(30)
    void manyConcurrentClientsEachSeeTheirOwnWritesCorrectly(@TempDir Path dataDir) throws Exception {
        int clientCount = 16;
        int opsPerClient = 50;

        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
             ForgeServer server = new ForgeServer(store, 0)) {

            ExecutorService clients = Executors.newFixedThreadPool(clientCount);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new java.util.ArrayList<>();

            for (int c = 0; c < clientCount; c++) {
                int clientId = c;
                futures.add(clients.submit(() -> {
                    try (Socket socket = new Socket("localhost", server.port())) {
                        InputStream in = socket.getInputStream();
                        OutputStream out = socket.getOutputStream();
                        start.await();
                        for (int i = 0; i < opsPerClient; i++) {
                            String key = "client-" + clientId + "-key-" + i;
                            String value = "client-" + clientId + "-value-" + i;
                            FrameCodec.writeRequest(out, new Request.Put(key, bytes(value)));
                            assertEquals(new Response.OkAbsent(),
                                    FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));

                            FrameCodec.writeRequest(out, new Request.Get(key));
                            Response response = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
                            Response.OkPresent present = assertInstanceOf(Response.OkPresent.class, response);
                            assertArrayEquals(bytes(value), present.value());
                        }
                        return null;
                    }
                }));
            }

            start.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
            clients.shutdown();
        }
    }

    @Test
    void malformedRequestGetsErrorResponseAndConnectionStaysOpenForTheNextRequest() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // Hand-craft a frame with an unrecognized opcode; FrameCodec's own
            // encoder would refuse to build one, so this bypasses it deliberately.
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(payload);
            data.writeByte(99);
            data.writeInt(1);
            data.write(bytes("k"));
            data.writeInt(0);
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            new DataOutputStream(frame).writeInt(payload.size());
            frame.write(payload.toByteArray());
            out.write(frame.toByteArray());
            out.flush();

            Response response = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
            Response.Error error = assertInstanceOf(Response.Error.class, response);
            assertEquals(ProtocolConstants.ERROR_UNKNOWN_OPERATION, error.errorCode());

            FrameCodec.writeRequest(out, new Request.Get("still-works"));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
        }
    }

    @Test
    void oversizedRequestGetsErrorResponseThenConnectionCloses() throws IOException {
        int tinyMaxFrame = 16;
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0,
                tinyMaxFrame, ProtocolConstants.DEFAULT_MAX_KEY_LENGTH);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            new DataOutputStream(out).writeInt(tinyMaxFrame + 1000); // declared length exceeds the server's cap
            out.flush();

            Response response = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
            Response.Error error = assertInstanceOf(Response.Error.class, response);
            assertEquals(ProtocolConstants.ERROR_OVERSIZED_REQUEST, error.errorCode());

            assertThrows(EOFException.class,
                    () -> FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
        }
    }

    @Test
    void closingServerClosesActiveConnections() throws Exception {
        ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
        Socket socket = new Socket("localhost", server.port());

        FrameCodec.writeRequest(socket.getOutputStream(), new Request.Get("k"));
        FrameCodec.readResponse(socket.getInputStream(), ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);

        server.close();

        // The client's next read must not hang forever: the server forcibly
        // closed the socket, so this either throws or returns EOF promptly.
        socket.setSoTimeout(5_000);
        assertThrows(IOException.class,
                () -> FrameCodec.readResponse(socket.getInputStream(), ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
        socket.close();
    }

    @Test
    void portReturnsActualBoundPortWhenConstructedWithZero() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0)) {
            assertTrue(server.port() > 0);
        }
    }

    // --- Phase 7: ownership predicate ---------------------------------------

    @Test
    void serverWithNoOwnershipPredicateBehavesExactlyAsBeforePhase7() throws IOException {
        // The pre-Phase-7 constructors must remain byte-for-byte backward compatible:
        // no ownership check at all, every key served unconditionally.
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Put("any-key", bytes("v")));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));
        }
    }

    @Test
    void ownedKeyIsServedNormally() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), key -> key.startsWith("mine-"), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Put("mine-1", bytes("v")));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));

            FrameCodec.writeRequest(out, new Request.Get("mine-1"));
            Response response = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
            assertArrayEquals(bytes("v"), assertInstanceOf(Response.OkPresent.class, response).value());
        }
    }

    @Test
    void unownedKeyIsRejectedWithoutTouchingTheStoreAndConnectionStaysOpen() throws IOException {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        try (ForgeServer server = new ForgeServer(store, key -> key.startsWith("mine-"), 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Put("not-mine", bytes("v")));
            Response response = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
            Response.Error error = assertInstanceOf(Response.Error.class, response);
            assertEquals(ProtocolConstants.ERROR_NOT_OWNER, error.errorCode());

            // The connection must still be usable — an ownership rejection is not connection-fatal.
            FrameCodec.writeRequest(out, new Request.Put("mine-1", bytes("v")));
            assertEquals(new Response.OkAbsent(), FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH));

            assertTrue(store.get("not-mine").isEmpty(), "a rejected PUT must never reach the store");
        }
    }

    @Test
    void unownedGetAndDeleteAreAlsoRejected() throws IOException {
        try (ForgeServer server = new ForgeServer(new InMemoryKeyValueStore(), key -> false, 0);
             Socket socket = new Socket("localhost", server.port())) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            FrameCodec.writeRequest(out, new Request.Get("k"));
            Response getResponse = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
            assertEquals(ProtocolConstants.ERROR_NOT_OWNER, assertInstanceOf(Response.Error.class, getResponse).errorCode());

            FrameCodec.writeRequest(out, new Request.Delete("k"));
            Response deleteResponse = FrameCodec.readResponse(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
            assertEquals(ProtocolConstants.ERROR_NOT_OWNER, assertInstanceOf(Response.Error.class, deleteResponse).errorCode());
        }
    }
}
