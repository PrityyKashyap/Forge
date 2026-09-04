package com.forge.server;

import com.forge.common.protocol.FrameCodec;
import com.forge.common.protocol.ProtocolConstants;
import com.forge.common.protocol.ProtocolException;
import com.forge.common.protocol.Request;
import com.forge.common.protocol.Response;
import com.forge.storage.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.util.function.Predicate;

/**
 * Serves one client connection: reads requests, dispatches them to the
 * store, writes responses, until the connection ends.
 *
 * <p>PUT and DELETE acknowledge success with {@link Response.OkAbsent} — a
 * bare "no payload" ack. The wire protocol has no way to carry a request's
 * previous value back to the caller (see {@code ForgeClient}, which for the
 * same reason does not return one either); only GET's own {@code OkAbsent}/
 * {@code OkPresent} distinction is meaningful.
 *
 * <h2>Error recovery within a connection</h2>
 * A {@link ProtocolException} other than {@code ERROR_OVERSIZED_REQUEST}
 * always happens only after {@link FrameCodec} has already fully consumed
 * the declared frame's bytes — so the stream is guaranteed to be positioned
 * exactly at the next frame's boundary, and it's safe (and friendlier to the
 * client) to report the error and keep serving the connection.
 * {@code ERROR_OVERSIZED_REQUEST} is different: it's raised before the
 * declared payload is read, so the sender's oversized bytes are still
 * sitting unread on the wire with no safe way to skip exactly that many —
 * that case, and any other {@link IOException}, ends the connection.
 */
final class ConnectionHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);

    private final Socket socket;
    private final KeyValueStore store;
    private final Predicate<String> ownershipPredicate;
    private final WriteAuthority writeAuthority;
    private final int maxFrameLength;
    private final int maxKeyLength;

    ConnectionHandler(Socket socket, KeyValueStore store, Predicate<String> ownershipPredicate,
            WriteAuthority writeAuthority, int maxFrameLength, int maxKeyLength) {
        this.socket = socket;
        this.store = store;
        this.ownershipPredicate = ownershipPredicate;
        this.writeAuthority = writeAuthority;
        this.maxFrameLength = maxFrameLength;
        this.maxKeyLength = maxKeyLength;
    }

    @Override
    public void run() {
        InputStream in;
        OutputStream out;
        try {
            in = new BufferedInputStream(socket.getInputStream());
            out = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            return;
        }

        while (true) {
            Request request;
            try {
                request = FrameCodec.readRequest(in, maxFrameLength, maxKeyLength);
            } catch (ProtocolException e) {
                boolean sent = trySendError(out, e);
                if (!sent || e.errorCode() == ProtocolConstants.ERROR_OVERSIZED_REQUEST) {
                    return;
                }
                continue;
            } catch (IOException e) {
                return;
            }

            Response response = handle(request);
            try {
                FrameCodec.writeResponse(out, response);
            } catch (IOException e) {
                return;
            }
        }
    }

    private boolean trySendError(OutputStream out, ProtocolException e) {
        try {
            FrameCodec.writeResponse(out, new Response.Error(e.errorCode(), e.getMessage()));
            return true;
        } catch (IOException writeFailure) {
            return false;
        }
    }

    private Response handle(Request request) {
        String key = switch (request) {
            case Request.Put put -> put.key();
            case Request.Get get -> get.key();
            case Request.Delete delete -> delete.key();
        };
        if (!ownershipPredicate.test(key)) {
            // Fail closed per DESIGN.md's Phase 7 contract: never touch the store for a
            // key this node doesn't currently own under its partition map, rather than
            // risk silently serving a possibly-wrong (or possibly-orphaned) answer.
            return new Response.Error(ProtocolConstants.ERROR_NOT_OWNER, "this node does not own key: " + key);
        }
        boolean isWrite = request instanceof Request.Put || request instanceof Request.Delete;
        if (isWrite && !writeAuthority.canAcceptWrites()) {
            // Phase 15 fencing: reads are deliberately NOT gated here (see
            // docs/CONSISTENCY.md §5 — this node may still serve a stale local GET),
            // but a write is refused before it ever reaches the store, exactly like
            // the ownership check above. The message's "term=.../leader=..." shape is
            // a documented, machine-parseable convention (not part of the binary wire
            // format) that PartitionedForgeClient's retry logic knows how to read.
            return new Response.Error(ProtocolConstants.ERROR_NOT_LEADER, notLeaderMessage());
        }
        try {
            return switch (request) {
                case Request.Put put -> {
                    store.put(put.key(), put.value());
                    yield new Response.OkAbsent();
                }
                case Request.Get get -> store.get(get.key())
                        .<Response>map(Response.OkPresent::new)
                        .orElseGet(Response.OkAbsent::new);
                case Request.Delete delete -> {
                    store.delete(delete.key());
                    yield new Response.OkAbsent();
                }
            };
        } catch (UncheckedIOException e) {
            log.warn("storage error handling {}", request, e);
            return new Response.Error(ProtocolConstants.ERROR_STORAGE_ERROR, String.valueOf(e.getMessage()));
        } catch (RuntimeException e) {
            log.error("unexpected error handling {}", request, e);
            return new Response.Error(ProtocolConstants.ERROR_INTERNAL_ERROR, String.valueOf(e.getMessage()));
        }
    }

    /**
     * The documented {@code ERROR_NOT_LEADER} message convention:
     * {@code "NOT_LEADER term=<n> leader=<hint|none>"}. Plain text, not part
     * of the binary wire format ({@code Response.Error.message()} is already
     * an arbitrary string) — a client that doesn't know this convention just
     * sees a human-readable diagnostic; one that does (see
     * {@code PartitionedForgeClient}) can parse it to redirect a retry.
     */
    private String notLeaderMessage() {
        String leaderHint = writeAuthority.currentLeaderHint().orElse("none");
        return "NOT_LEADER term=" + writeAuthority.currentTerm() + " leader=" + leaderHint;
    }
}
