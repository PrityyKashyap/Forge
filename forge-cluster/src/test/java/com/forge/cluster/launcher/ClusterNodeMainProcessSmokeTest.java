package com.forge.cluster.launcher;

import com.forge.client.ForgeClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The one test in this project that runs {@link ClusterNodeMain} through
 * the exact mechanism this project's own docs tell an operator to use —
 * {@code mvn exec:java} as a genuine, separate process — rather than
 * calling {@link ClusterNode#start} in-JVM the way every other test in
 * this package (and {@code FailoverReplicationIntegrationTest}) does.
 * That in-JVM style proves the wiring is correct but can never catch a
 * <em>runtime classpath/packaging</em> defect: a JUnit test forked by
 * Surefire runs on the module's full <b>test</b> classpath (compile +
 * runtime + test-scoped dependencies), which is a strictly larger set than
 * what {@code mvn exec:java} actually resolves (compile + runtime only) —
 * so a dependency that's wrongly {@code test}-scoped in the POM is
 * invisible to every in-JVM test yet completely missing for a real launch.
 *
 * <p>This is not a hypothetical: {@code forge-cluster}'s own {@code
 * logback-classic} dependency was exactly this bug — declared with
 * {@code <scope>test</scope>}, which (a direct dependency declaration
 * always wins Maven's scope mediation over a transitive one, even a more
 * permissive one) silently dropped the SLF4J binding from a real {@code
 * mvn exec:java} launch's classpath, even though {@code forge-server}
 * transitively provides it at {@code compile} scope. Every existing
 * in-JVM test passed the whole time. Fixed by dropping the {@code test}
 * scope (matching {@code forge-server}'s identical, correct declaration);
 * this test exists so a future accidental re-introduction of that scope
 * fails a real, automated test instead of only being caught by manual
 * verification again.
 */
class ClusterNodeMainProcessSmokeTest {

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeout);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    private static boolean isListening(int port) {
        try (java.net.Socket s = new java.net.Socket()) {
            s.connect(new java.net.InetSocketAddress("localhost", port), 200);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    @Timeout(60)
    void mvnExecJavaStartsARealProcessWithNoMissingRuntimeDependencies(@TempDir Path baseDir) throws Exception {
        int raftPort = freePort();
        int replicationPort = freePort();
        int clientPort = freePort();
        int snapshotPort = freePort();
        Path configFile = baseDir.resolve("cluster.conf");
        Files.writeString(configFile, "solo localhost " + raftPort + " " + replicationPort + " "
                + clientPort + " " + snapshotPort + "\n");
        Path dataDir = baseDir.resolve("data");

        // Surefire's own forked JVM has this module's directory (forge-cluster) as its working
        // directory, so `mvn exec:java` here finds the same pom.xml (and the exec-maven-plugin
        // config already defaulting mainClass to ClusterNodeMain) a real operator would.
        String execArgs = configFile.toString() + " solo " + dataDir;
        ProcessBuilder pb = new ProcessBuilder("mvn", "-q", "exec:java", "-Dexec.args=" + execArgs);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        List<String> outputLines = new ArrayList<>();
        Thread reader = Thread.ofPlatform().start(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    synchronized (outputLines) {
                        outputLines.add(line);
                    }
                }
            } catch (IOException ignored) {
                // process ended
            }
        });

        try {
            waitUntil(Duration.ofSeconds(45), () -> isListening(clientPort));

            // A sole node with no peers still has to complete its own (up to
            // electionTimeoutMax) Raft election before it may accept writes.
            Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
            IOException lastFailure = null;
            while (Instant.now().isBefore(deadline)) {
                try (ForgeClient client = ForgeClient.connect("localhost", clientPort)) {
                    client.put("smoke-key", "smoke-value".getBytes(StandardCharsets.UTF_8));
                    lastFailure = null;
                    break;
                } catch (IOException e) {
                    lastFailure = e;
                    Thread.sleep(50);
                }
            }
            if (lastFailure != null) {
                throw lastFailure;
            }

            try (ForgeClient client = ForgeClient.connect("localhost", clientPort)) {
                Optional<byte[]> value = client.get("smoke-key");
                assertTrue(value.isPresent());
                assertArrayEquals("smoke-value".getBytes(StandardCharsets.UTF_8), value.get());
            }

            synchronized (outputLines) {
                String combined = String.join("\n", outputLines);
                assertFalse(combined.contains("NoClassDefFoundError"),
                        "the real mvn exec:java process must never fail on a missing runtime dependency:\n" + combined);
                assertFalse(combined.contains("No SLF4J providers were found"),
                        "the real mvn exec:java process must have a working SLF4J binding on its runtime "
                                + "classpath (compile+runtime scope), not merely on a test's classpath:\n" + combined);
                assertTrue(combined.contains("FORGE cluster node 'solo' up"),
                        "expected the real startup log line; got:\n" + combined);
            }
        } finally {
            process.destroy();
            process.waitFor(10, TimeUnit.SECONDS);
            if (process.isAlive()) {
                process.destroyForcibly();
            }
            reader.join(5_000);
        }
    }
}
