package com.forge.client;

import com.forge.common.protocol.FrameCodec;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.common.protocol.Request;
import com.forge.common.protocol.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;
import java.util.Optional;

/**
 * A blocking, synchronous client for one FORGE server connection.
 *
 * <p>Does not implement {@code KeyValueStore}: that interface's methods
 * return the previous value from {@code put}/{@code delete} and throw no
 * checked exceptions, neither of which this class can honor — the wire
 * protocol has no way to carry a previous value back (see
 * {@code ConnectionHandler}, which for the same reason never sends one),
 * and every call here can fail with a checked {@link IOException}.
 * Implementing it would also force an unwanted {@code forge-client} to
 * {@code forge-storage} dependency for no benefit.
 *
 * <p>Not thread-safe: one request is ever in flight on a given connection
 * at a time. Each instance owns exactly one socket and does not reconnect;
 * callers that need retry or connection-pooling behavior build it on top.
 */
public final class ForgeClient implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final int maxFrameLength;

    private ForgeClient(Socket socket, int maxFrameLength) throws IOException {
        this.socket = socket;
        this.maxFrameLength = maxFrameLength;
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
    }

    public static ForgeClient connect(String host, int port) throws IOException {
        return connect(host, port, ProtocolConstants.DEFAULT_MAX_FRAME_LENGTH);
    }

    public static ForgeClient connect(String host, int port, int maxFrameLength) throws IOException {
        Objects.requireNonNull(host, "host must not be null");
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be positive");
        }
        Socket socket = new Socket(host, port);
        try {
            return new ForgeClient(socket, maxFrameLength);
        } catch (IOException e) {
            closeQuietly(socket, e);
            throw e;
        }
    }

    /**
     * Sets {@code key} to {@code value}.
     *
     * @throws ForgeServerException if the server reported failure
     * @throws IOException          on any other transport-level failure
     */
    public void put(String key, byte[] value) throws IOException {
        requireOk(send(new Request.Put(key, value)));
    }

    /**
     * @return the value stored for {@code key}, or {@link Optional#empty()}
     *         if it is not present
     * @throws ForgeServerException if the server reported failure
     * @throws IOException          on any other transport-level failure
     */
    public Optional<byte[]> get(String key) throws IOException {
        Response response = send(new Request.Get(key));
        return switch (response) {
            case Response.OkAbsent ignored -> Optional.empty();
            case Response.OkPresent present -> Optional.of(present.value());
            case Response.Error error -> throw new ForgeServerException(error.errorCode(), error.message());
        };
    }

    /**
     * Deletes {@code key}, if present.
     *
     * @throws ForgeServerException if the server reported failure
     * @throws IOException          on any other transport-level failure
     */
    public void delete(String key) throws IOException {
        requireOk(send(new Request.Delete(key)));
    }

    private Response send(Request request) throws IOException {
        FrameCodec.writeRequest(out, request);
        return FrameCodec.readResponse(in, maxFrameLength);
    }

    private static void requireOk(Response response) throws ForgeServerException {
        if (response instanceof Response.Error error) {
            throw new ForgeServerException(error.errorCode(), error.message());
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private static void closeQuietly(Socket socket, Exception primary) {
        try {
            socket.close();
        } catch (IOException suppressed) {
            primary.addSuppressed(suppressed);
        }
    }
}
