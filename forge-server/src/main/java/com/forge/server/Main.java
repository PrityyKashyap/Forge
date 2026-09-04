package com.forge.server;

import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * CLI entry point: starts a FORGE server over a durable, on-disk store.
 *
 * <p>Usage: {@code forge-server [dataDirectory] [port]}, both optional.
 *
 * <p>{@code main()} returns once setup completes; the process stays alive
 * because {@link ForgeServer}'s accept loop runs on a non-daemon platform
 * thread (virtual threads, used for connections, are always daemon threads
 * and would not by themselves keep the JVM running).
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_DATA_DIRECTORY = "forge-data";
    private static final int DEFAULT_PORT = 7070;

    private Main() {
    }

    public static void main(String[] args) throws IOException {
        Path dataDirectory = args.length > 0 ? Path.of(args[0]) : Path.of(DEFAULT_DATA_DIRECTORY);
        int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;

        ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDirectory);
        ForgeServer server;
        try {
            server = new ForgeServer(store, port);
        } catch (IOException | RuntimeException e) {
            store.close();
            throw e;
        }

        log.info("FORGE server listening on port {} (data directory: {})", server.port(), dataDirectory);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down");
            try {
                server.close();
            } catch (IOException e) {
                log.warn("error closing server during shutdown", e);
            }
            try {
                store.close();
            } catch (IOException e) {
                log.warn("error closing store during shutdown", e);
            }
        }, "forge-server-shutdown"));
    }
}
