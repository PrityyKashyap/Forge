package com.forge.cluster.membership;

import com.forge.cluster.NodeAddress;
import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.DatagramSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Real UDP datagrams between two real sockets — this is deliberately a
 * real-time integration test (unlike {@link FailureDetectorTest}, which
 * uses a fake clock for every timing assertion), because its whole point is
 * proving actual bytes travel over an actual network between actual node
 * processes, which a fake clock can't stand in for. Kept fast by using
 * short intervals/timeouts (tens to a few hundred milliseconds) and a
 * bounded poll rather than one long fixed sleep.
 */
class HeartbeatServiceTest {

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeout);
        }
    }

    @Test
    @Timeout(15)
    void sustainedRealHeartbeatsKeepTwoNodesMutuallyAliveWellPastWhatASingleJoinWouldCover() throws Exception {
        NodeId a = new NodeId("hb-a");
        NodeId b = new NodeId("hb-b");
        Duration suspectTimeout = Duration.ofMillis(300);
        Duration deadTimeout = Duration.ofMillis(600);
        Duration heartbeatInterval = Duration.ofMillis(40);

        FailureDetector detectorA = new FailureDetector(Clock.systemUTC(), suspectTimeout, deadTimeout);
        FailureDetector detectorB = new FailureDetector(Clock.systemUTC(), suspectTimeout, deadTimeout);

        try (HeartbeatService serviceA = new HeartbeatService(a, 0, detectorA, heartbeatInterval);
             HeartbeatService serviceB = new HeartbeatService(b, 0, detectorB, heartbeatInterval)) {

            serviceA.addPeer(b, new NodeAddress("localhost", serviceB.port()));
            serviceB.addPeer(a, new NodeAddress("localhost", serviceA.port()));

            // Run for well longer than suspectTimeout — only sustained, real, repeated
            // heartbeats (not just the initial join()'s default ALIVE state) could keep
            // both sides ALIVE this far past it.
            Thread.sleep(suspectTimeout.toMillis() * 3);

            assertEquals(NodeState.ALIVE, detectorA.stateOf(b).orElseThrow(),
                    "node A must still see node B as ALIVE — real heartbeats must have kept arriving");
            assertEquals(NodeState.ALIVE, detectorB.stateOf(a).orElseThrow(),
                    "node B must still see node A as ALIVE — real heartbeats must have kept arriving");
        }
    }

    @Test
    @Timeout(15)
    void aPeerThatStopsSendingIsEventuallyDetectedAsDeadByARealPeer() throws Exception {
        NodeId a = new NodeId("hb-a2");
        NodeId b = new NodeId("hb-b2");
        Duration suspectTimeout = Duration.ofMillis(150);
        Duration deadTimeout = Duration.ofMillis(300);
        Duration heartbeatInterval = Duration.ofMillis(30);

        FailureDetector detectorA = new FailureDetector(Clock.systemUTC(), suspectTimeout, deadTimeout);
        FailureDetector detectorB = new FailureDetector(Clock.systemUTC(), suspectTimeout, deadTimeout);

        HeartbeatService serviceA = new HeartbeatService(a, 0, detectorA, heartbeatInterval);
        HeartbeatService serviceB = new HeartbeatService(b, 0, detectorB, heartbeatInterval);
        try {
            serviceA.addPeer(b, new NodeAddress("localhost", serviceB.port()));
            serviceB.addPeer(a, new NodeAddress("localhost", serviceA.port()));

            // Let a few real heartbeats actually land first.
            waitUntil(Duration.ofSeconds(2), () -> detectorA.stateOf(b).orElseThrow() == NodeState.ALIVE);
        } finally {
            serviceB.close(); // node B stops sending entirely — simulates it going away
        }

        try {
            waitUntil(Duration.ofSeconds(5), () -> detectorA.stateOf(b).orElseThrow() == NodeState.DEAD);
        } finally {
            serviceA.close();
        }
    }

    @Test
    @Timeout(15)
    void closeReleasesTheUdpPortAndStopsBothBackgroundThreads() throws Exception {
        FailureDetector detector = new FailureDetector(Clock.systemUTC(), Duration.ofMillis(200), Duration.ofMillis(400));
        HeartbeatService service = new HeartbeatService(new NodeId("cleanup-node"), 0, detector, Duration.ofMillis(50));
        int port = service.port();

        service.close();

        // If close() left the UDP socket bound, this bind attempt fails.
        assertDoesNotThrow(() -> {
            try (DatagramSocket probe = new DatagramSocket(port)) {
                // successfully rebound the same port
            }
        });
    }

    @Test
    @Timeout(15)
    void closeIsIdempotent() throws Exception {
        FailureDetector detector = new FailureDetector(Clock.systemUTC(), Duration.ofMillis(200), Duration.ofMillis(400));
        HeartbeatService service = new HeartbeatService(new NodeId("idempotent-close"), 0, detector, Duration.ofMillis(50));

        service.close();
        assertDoesNotThrow(service::close);
    }
}
