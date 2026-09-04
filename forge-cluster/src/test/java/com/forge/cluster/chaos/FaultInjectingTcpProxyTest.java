package com.forge.cluster.chaos;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the proxy's own fault mechanics directly, against a trivial echo
 * server — isolating "does the fault injector actually inject the fault"
 * from any FORGE-specific protocol logic, which the chaos scenario tests
 * build on top of separately.
 */
class FaultInjectingTcpProxyTest {

    /** The simplest possible target: echoes back whatever it reads. */
    private static Thread startEchoServer(ServerSocket serverSocket) {
        return Thread.ofPlatform().start(() -> {
            try {
                while (!serverSocket.isClosed()) {
                    Socket client = serverSocket.accept();
                    Thread.ofPlatform().start(() -> {
                        try {
                            InputStream in = client.getInputStream();
                            OutputStream out = client.getOutputStream();
                            byte[] buf = new byte[256];
                            int n;
                            while ((n = in.read(buf)) >= 0) {
                                out.write(buf, 0, n);
                                out.flush();
                            }
                        } catch (IOException ignored) {
                            // connection ended
                        }
                    });
                }
            } catch (IOException ignored) {
                // server socket closed
            }
        });
    }

    @Test
    @Timeout(15)
    void forwardsBytesUnmodifiedWithNoFaultConfigured() throws Exception {
        try (ServerSocket echoServer = new ServerSocket(0)) {
            startEchoServer(echoServer);
            try (FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", echoServer.getLocalPort(), 0);
                 Socket client = new Socket("localhost", proxy.port())) {
                client.getOutputStream().write("hello".getBytes(StandardCharsets.UTF_8));
                client.getOutputStream().flush();

                byte[] buf = new byte[5];
                int read = client.getInputStream().read(buf);
                assertEquals("hello", new String(buf, 0, read, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    @Timeout(15)
    void delayActuallyDelaysDeliveryByAtLeastTheConfiguredAmount() throws Exception {
        try (ServerSocket echoServer = new ServerSocket(0)) {
            startEchoServer(echoServer);
            try (FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", echoServer.getLocalPort(), 0);
                 Socket client = new Socket("localhost", proxy.port())) {
                proxy.setDelay(Duration.ofMillis(300));

                Instant start = Instant.now();
                client.getOutputStream().write("x".getBytes(StandardCharsets.UTF_8));
                client.getOutputStream().flush();
                client.getInputStream().read();
                Duration elapsed = Duration.between(start, Instant.now());

                assertTrue(elapsed.toMillis() >= 300, "expected at least 300ms of injected delay, took " + elapsed.toMillis() + "ms");
            }
        }
    }

    @Test
    @Timeout(15)
    void dropNextGenuinelyDiscardsExactlyThatManyChunks() throws Exception {
        try (ServerSocket echoServer = new ServerSocket(0)) {
            startEchoServer(echoServer);
            try (FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", echoServer.getLocalPort(), 0);
                 Socket client = new Socket("localhost", proxy.port())) {
                client.setSoTimeout(3000);
                proxy.dropNext(1);

                client.getOutputStream().write("dropped".getBytes(StandardCharsets.UTF_8));
                client.getOutputStream().flush();
                // Give the proxy time to have read and (correctly) discarded this chunk as
                // its own read() call, before the next message is even written -- TCP has no
                // message boundaries, so writing both back-to-back risks the proxy reading
                // them as one combined chunk and this test proving nothing meaningful either way.
                Thread.sleep(300);

                // dropNext(1) only discards one chunk, so this second message must arrive.
                client.getOutputStream().write("delivered".getBytes(StandardCharsets.UTF_8));
                client.getOutputStream().flush();

                byte[] buf = new byte[9];
                int totalRead = 0;
                while (totalRead < buf.length) {
                    int n = client.getInputStream().read(buf, totalRead, buf.length - totalRead);
                    if (n < 0) {
                        break;
                    }
                    totalRead += n;
                }
                assertEquals("delivered", new String(buf, 0, totalRead, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    @Timeout(15)
    void duplicateAllDeliversEachChunkTwice() throws Exception {
        try (ServerSocket echoServer = new ServerSocket(0)) {
            startEchoServer(echoServer);
            try (FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", echoServer.getLocalPort(), 0);
                 Socket client = new Socket("localhost", proxy.port())) {
                proxy.setDuplicateAll(true);

                client.getOutputStream().write("hi".getBytes(StandardCharsets.UTF_8));
                client.getOutputStream().flush();

                byte[] buf = new byte[4];
                int totalRead = 0;
                while (totalRead < 4) {
                    totalRead += client.getInputStream().read(buf, totalRead, 4 - totalRead);
                }
                assertEquals("hihi", new String(buf, 0, 4, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    @Timeout(15)
    void partitionSeversExistingConnectionsAndRefusesNewOnes() throws Exception {
        try (ServerSocket echoServer = new ServerSocket(0)) {
            startEchoServer(echoServer);
            try (FaultInjectingTcpProxy proxy = new FaultInjectingTcpProxy("localhost", echoServer.getLocalPort(), 0)) {
                Socket existing = new Socket("localhost", proxy.port());
                existing.getOutputStream().write("still-connected".getBytes(StandardCharsets.UTF_8));
                existing.getOutputStream().flush();
                existing.getInputStream().read(new byte[16]); // proves the connection worked before partitioning

                proxy.partition();
                assertTrue(proxy.isPartitioned());

                // A severed connection surfaces as either a clean EOF (a graceful close,
                // Socket.close()'s normal behavior) or an IOException (e.g. a reset) --
                // both mean "severed"; only "the read blocks/succeeds with real data" would
                // mean partition() failed to do its job.
                existing.setSoTimeout(2000);
                try {
                    int result = existing.getInputStream().read(new byte[16]);
                    assertEquals(-1, result, "an existing connection must be severed by partition(), not left open");
                } catch (IOException expected) {
                    // also an acceptable "severed" outcome
                }

                // A new connection attempt while partitioned: the TCP handshake itself may
                // still succeed (the proxy calls accept() before checking partition state),
                // but the proxy closes it immediately afterward — so the connection is
                // unusable either way, surfacing as a read that throws or cleanly hits EOF.
                try (Socket refused = new Socket()) {
                    refused.connect(new java.net.InetSocketAddress("localhost", proxy.port()), 1000);
                    refused.setSoTimeout(1000);
                    int result = refused.getInputStream().read();
                    assertEquals(-1, result, "a connection accepted while partitioned must be closed immediately, not usable");
                } catch (IOException expected) {
                    // also an acceptable outcome: the close raced the read and surfaced as an error instead of a clean EOF
                }

                proxy.heal();
                try (Socket afterHeal = new Socket("localhost", proxy.port())) {
                    afterHeal.getOutputStream().write("healed".getBytes(StandardCharsets.UTF_8));
                    afterHeal.getOutputStream().flush();
                    byte[] buf = new byte[6];
                    int read = afterHeal.getInputStream().read(buf);
                    assertEquals("healed", new String(buf, 0, read, StandardCharsets.UTF_8));
                }
            }
        }
    }
}
