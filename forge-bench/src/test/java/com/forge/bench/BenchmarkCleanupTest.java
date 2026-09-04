package com.forge.bench;

import com.forge.client.ForgeClient;
import com.forge.server.ForgeServer;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Correctness requirement: a benchmark run must not leave server sockets (or,
 * for a harness that spawned one, processes) behind after it finishes. This
 * benchmark harness runs its server embedded in the same JVM (see
 * {@code BenchmarkRunner}'s Javadoc for why), so "no leftover process" is
 * automatic; what still needs proving is that the port is actually released
 * and reusable after a full start/work/close cycle, and that closing the
 * client and server doesn't leave the harness hanging.
 */
class BenchmarkCleanupTest {

    @Test
    @Timeout(30)
    void portIsFreeAndReusableAfterAFullBenchmarkStyleRunCloses(@TempDir Path dataDir) throws Exception {
        int port;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
                ForgeServer server = new ForgeServer(store, 0)) {
            port = server.port();

            try (ForgeClient client = ForgeClient.connect("localhost", port)) {
                client.put("k", Workload.fixedValue(16));
                client.get("k");
            }
        } // server.close() and store.close() both run here

        // If close() left the listening socket bound, this bind attempt fails.
        assertDoesNotThrow(() -> {
            try (ServerSocket probe = new ServerSocket(port)) {
                // successfully rebound the same port
            }
        });
    }

    @Test
    @Timeout(30)
    void concurrentBenchRunLeavesNoLingeringConnectionsAfterCompletion(@TempDir Path dataDir) throws Exception {
        int port;
        try (ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dataDir);
                ForgeServer server = new ForgeServer(store, 0)) {
            port = server.port();
            String host = "localhost";

            // ConcurrentBenchRun.run's try-with-resources closes every worker's
            // OpExecutor (and thus its socket) before the method returns, and its
            // own thread pool's awaitTermination blocks until every task has
            // actually finished — so by the time this call returns, none of its
            // connections should still be open.
            ConcurrentBenchRun.run(6, () -> {
                try {
                    return new NetworkOpExecutor(host, port);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }, threadId -> Workload.puts("cleanup-t" + threadId, 0, 10, Workload.fixedValue(16)));
        }

        assertDoesNotThrow(() -> {
            try (ServerSocket probe = new ServerSocket(port)) {
                // successfully rebound the same port after the concurrent run's cleanup
            }
        });
    }
}
