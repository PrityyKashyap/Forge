package com.forge.client;

import com.forge.common.protocol.FrameCodec;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.common.protocol.Request;
import com.forge.common.protocol.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code forge-client} must not depend on {@code forge-server}, even in test
 * scope, so these tests drive {@link ForgeClient} against a minimal
 * hand-rolled server harness that speaks the same wire protocol via
 * {@link FrameCodec} directly, rather than a real {@code ForgeServer}. Real
 * client/server integration is covered by the {@code forge-tests} module.
 */
class ForgeClientTest {

    /** A single-connection test double: services exactly the requests queued via {@link #respondNext}. */
    private static final class FakeServer implements Closeable {
        private final ServerSocket serverSocket;
        private final BlockingQueue<Response> scriptedResponses = new ArrayBlockingQueue<>(64);
        private final Deque<Request> receivedRequests = new ArrayDeque<>();
        private final Thread serverThread;
        private volatile IOException serverFailure;

        FakeServer() throws IOException {
            serverSocket = new ServerSocket(0);
            serverThread = Thread.ofPlatform().name("fake-forge-server").start(this::serve);
        }

        private void serve() {
            try (Socket socket = serverSocket.accept()) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                while (true) {
                    Request request;
                    try {
                        request = FrameCodec.readRequest(in, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH,
                                ProtocolConstants.DEFAULT_MAX_KEY_LENGTH);
                    } catch (IOException e) {
                        return; // client disconnected
                    }
                    synchronized (receivedRequests) {
                        receivedRequests.addLast(request);
                    }
                    Response response = scriptedResponses.take();
                    FrameCodec.writeResponse(out, response);
                }
            } catch (IOException e) {
                serverFailure = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        void respondNext(Response response) {
            scriptedResponses.add(response);
        }

        Request lastReceivedRequest() {
            synchronized (receivedRequests) {
                return receivedRequests.getLast();
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
        }
    }

    private FakeServer fakeServer;

    @AfterEach
    void tearDown() throws IOException {
        if (fakeServer != null) {
            fakeServer.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putSendsAPutRequestAndSucceedsOnOkAbsent() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.OkAbsent());

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            client.put("k", bytes("v"));
        }

        Request.Put sent = (Request.Put) fakeServer.lastReceivedRequest();
        assertEquals("k", sent.key());
        assertArrayEquals(bytes("v"), sent.value());
    }

    @Test
    void getReturnsPresentValueWhenServerRespondsOkPresent() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.OkPresent(bytes("v")));

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            Optional<byte[]> result = client.get("k");
            assertTrue(result.isPresent());
            assertArrayEquals(bytes("v"), result.get());
        }
    }

    @Test
    void getReturnsEmptyWhenServerRespondsOkAbsent() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.OkAbsent());

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            assertFalse(client.get("missing").isPresent());
        }
    }

    @Test
    void deleteSendsADeleteRequestAndSucceedsOnOkAbsent() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.OkAbsent());

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            client.delete("k");
        }

        assertEquals(new Request.Delete("k"), fakeServer.lastReceivedRequest());
    }

    @Test
    void putThrowsForgeServerExceptionWhenServerRespondsWithError() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.Error(ProtocolConstants.ERROR_STORAGE_ERROR, "disk full"));

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            ForgeServerException e = assertThrows(ForgeServerException.class, () -> client.put("k", bytes("v")));
            assertEquals(ProtocolConstants.ERROR_STORAGE_ERROR, e.errorCode());
            assertEquals("disk full", e.getMessage());
        }
    }

    @Test
    void getThrowsForgeServerExceptionWhenServerRespondsWithError() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.Error(ProtocolConstants.ERROR_INTERNAL_ERROR, "boom"));

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            ForgeServerException e = assertThrows(ForgeServerException.class, () -> client.get("k"));
            assertEquals(ProtocolConstants.ERROR_INTERNAL_ERROR, e.errorCode());
        }
    }

    @Test
    void deleteThrowsForgeServerExceptionWhenServerRespondsWithError() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.Error(ProtocolConstants.ERROR_STORAGE_ERROR, "nope"));

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            assertThrows(ForgeServerException.class, () -> client.delete("k"));
        }
    }

    @Test
    void multipleSequentialRequestsOnOneConnectionAllSucceed() throws IOException {
        fakeServer = new FakeServer();
        fakeServer.respondNext(new Response.OkAbsent());
        fakeServer.respondNext(new Response.OkPresent(bytes("v1")));
        fakeServer.respondNext(new Response.OkAbsent());
        fakeServer.respondNext(new Response.OkAbsent());

        try (ForgeClient client = ForgeClient.connect("localhost", fakeServer.port())) {
            client.put("k1", bytes("v1"));
            assertArrayEquals(bytes("v1"), client.get("k1").get());
            client.delete("k1");
            assertFalse(client.get("k1").isPresent());
        }
    }

    @Test
    void connectFailsWithIOExceptionWhenNothingIsListening() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            int freePort = probe.getLocalPort();
            probe.close();
            assertThrows(IOException.class, () -> ForgeClient.connect("localhost", freePort));
        }
    }

    @Test
    @Timeout(10)
    void closeUnblocksAConnectionThatIsWaitingOnAResponse() throws Exception {
        fakeServer = new FakeServer();
        // Deliberately never call respondNext(): the client's read will block.
        ForgeClient client = ForgeClient.connect("localhost", fakeServer.port());

        Thread requester = Thread.ofPlatform().start(() -> {
            try {
                client.get("k");
            } catch (IOException expected) {
                // expected once close() tears down the socket
            }
        });

        // Give the requester a moment to actually block in the read.
        Thread.sleep(200);
        client.close();
        requester.join(5_000);
        assertFalse(requester.isAlive());
    }
}
