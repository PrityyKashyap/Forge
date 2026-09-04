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
 *
 * <h2>{@code status} subcommand (Phase 14 observability)</h2>
 * {@code forge-server status <dataDirectory>} opens the store at that
 * directory, prints its {@link ConcurrentLsmKeyValueStore.StoreStatus}, and
 * exits — no server is started. This is an <b>offline</b> inspection tool:
 * it opens the store directly (replaying WAL recovery exactly as a normal
 * startup would), so it must not be run against a directory a live
 * {@code ForgeServer} process already has open (two processes cannot
 * safely share one WAL/SSTable directory — this is the same constraint
 * that already applies to starting two servers on the same directory).
 * See {@code docs/OPERATIONS.md} for what each field means and its
 * honestly-scoped limitations (storage-engine metrics only — no live
 * membership/leader/replication-lag query surface exists over the network
 * yet, a disclosed gap, not an oversight).
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_DATA_DIRECTORY = "forge-data";
    private static final int DEFAULT_PORT = 7070;

    private Main() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length > 0 && args[0].equals("status")) {
            if (args.length < 2) {
                System.err.println("Usage: forge-server status <dataDirectory>");
                System.exit(1);
                return;
            }
            printStatus(Path.of(args[1]));
            return;
        }

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

    private static void printStatus(Path dataDirectory) throws IOException {
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDirectory)) {
            ConcurrentLsmKeyValueStore.StoreStatus status = store.status();
            System.out.println("data directory:          " + dataDirectory.toAbsolutePath());
            System.out.println("sstable count:            " + status.sstableCount());
            System.out.println("sstable bytes (on disk):  " + status.totalSstableBytes());
            System.out.println("active MemTable bytes:    " + status.activeMemTableSizeBytes());
            System.out.println("flush in progress:        " + status.flushInProgress());
            System.out.println("next WAL sequence number: " + status.lastAppliedSequenceNumber());
        }
    }
}
