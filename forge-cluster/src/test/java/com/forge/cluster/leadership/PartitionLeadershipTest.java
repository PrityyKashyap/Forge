package com.forge.cluster.leadership;

import com.forge.cluster.NodeId;
import com.forge.cluster.consensus.RaftCluster;
import com.forge.storage.ConcurrentLsmKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A sole-node {@link RaftCluster} (no peers) becomes a confirmed,
 * authoritative leader almost immediately and deterministically — used
 * here purely as real machinery to drive {@link PartitionLeadership}'s own
 * logic (which this test is actually about), not to re-test Raft election
 * itself (see {@code RaftNodeTest}/{@code RaftClusterIntegrationTest}).
 */
class PartitionLeadershipTest {

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeout);
        }
    }

    private static RaftCluster soleNodeCluster(NodeId id) throws Exception {
        return new RaftCluster(id, Map.of(), 0, Clock.systemUTC(),
                Duration.ofMillis(60), Duration.ofMillis(100), Duration.ofMillis(20),
                Duration.ofMillis(10), Duration.ofSeconds(1), new Random(1));
    }

    @Test
    void refusesWritesUntilRaftConfirmsLeadership(@TempDir Path dir) throws Exception {
        try (RaftCluster raft = soleNodeCluster(new NodeId("solo"));
             ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            PartitionLeadership leadership = new PartitionLeadership("p0", raft, store);

            waitUntil(Duration.ofSeconds(5), leadership::canAcceptWrites);
            assertTrue(leadership.canAcceptWrites());
            assertEquals(raft.currentTerm(), leadership.currentTerm());
        }
    }

    @Test
    void ratchetsTheStoresSequenceNumberIntoTheCurrentTermsBandExactlyOnce(@TempDir Path dir) throws Exception {
        try (RaftCluster raft = soleNodeCluster(new NodeId("solo"));
             ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            PartitionLeadership leadership = new PartitionLeadership("p0", raft, store);
            waitUntil(Duration.ofSeconds(5), leadership::canAcceptWrites);

            long term = raft.currentTerm();
            long expectedBand = SequenceEpochs.bandStart(term);
            assertTrue(store.lastAppliedSequenceNumber() >= expectedBand,
                    "store's sequence number must be ratcheted into term " + term + "'s band");

            // A real write should now be assigned a sequence number inside that band.
            store.put("k", "v".getBytes());
            assertTrue(store.lastAppliedSequenceNumber() > expectedBand);
            long afterFirstWrite = store.lastAppliedSequenceNumber();

            // Calling canAcceptWrites() again (same term) must not re-ratchet or move the
            // counter backward/forward spuriously.
            assertTrue(leadership.canAcceptWrites());
            assertEquals(afterFirstWrite, store.lastAppliedSequenceNumber());
        }
    }

    @Test
    void currentLeaderHintReportsThisNodeOnceConfirmed(@TempDir Path dir) throws Exception {
        NodeId self = new NodeId("solo");
        try (RaftCluster raft = soleNodeCluster(self);
             ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            PartitionLeadership leadership = new PartitionLeadership("p0", raft, store);
            waitUntil(Duration.ofSeconds(5), leadership::canAcceptWrites);

            assertEquals("solo", leadership.currentLeaderHint().orElseThrow());
        }
    }

    @Test
    void beforeConfirmationWritesAreRefusedAndNoRatchetHappens(@TempDir Path dir) throws Exception {
        try (RaftCluster raft = soleNodeCluster(new NodeId("solo"));
             ConcurrentLsmKeyValueStore store = new ConcurrentLsmKeyValueStore(dir)) {
            PartitionLeadership leadership = new PartitionLeadership("p0", raft, store);

            // Immediately, before the sole-node election has had time to run at all.
            assertFalse(leadership.canAcceptWrites());
            assertEquals(1L, store.lastAppliedSequenceNumber(), "no ratchet should happen while not yet confirmed");
        }
    }
}
