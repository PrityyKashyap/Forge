package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Post-Phase-15 audit addition: {@link RaftPersistentState}'s own unit
 * tests, in the same "pure state, no network" spirit as {@code
 * RaftNodeTest} — the actual cross-restart safety property this exists for
 * is proven separately in {@code RaftPersistenceIntegrationTest}.
 */
class RaftPersistentStateTest {

    @Test
    void loadOfAMissingFileReturnsFreshDefaults(@TempDir Path dir) throws IOException {
        RaftPersistentState state = RaftPersistentState.load(dir.resolve("does-not-exist"));
        assertEquals(0L, state.currentTerm());
        assertTrue(state.votedFor().isEmpty());
    }

    @Test
    void saveThenLoadRoundTripsExactlyWithAVotedFor(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("raft-state");
        RaftPersistentState state = RaftPersistentState.load(file);
        state.save(7L, new NodeId("candidate-x"));

        RaftPersistentState reloaded = RaftPersistentState.load(file);
        assertEquals(7L, reloaded.currentTerm());
        assertEquals(new NodeId("candidate-x"), reloaded.votedFor().orElseThrow());
    }

    @Test
    void saveThenLoadRoundTripsExactlyWithNoVotedFor(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("raft-state");
        RaftPersistentState state = RaftPersistentState.load(file);
        state.save(3L, null);

        RaftPersistentState reloaded = RaftPersistentState.load(file);
        assertEquals(3L, reloaded.currentTerm());
        assertFalse(reloaded.votedFor().isPresent());
    }

    @Test
    void aLaterSaveOverwritesAnEarlierOneCompletely(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("raft-state");
        RaftPersistentState state = RaftPersistentState.load(file);
        state.save(1L, new NodeId("a"));
        state.save(2L, new NodeId("b"));

        RaftPersistentState reloaded = RaftPersistentState.load(file);
        assertEquals(2L, reloaded.currentTerm());
        assertEquals(new NodeId("b"), reloaded.votedFor().orElseThrow());
    }

    @Test
    void loadOfACorruptedFileThrowsRatherThanSilentlyDefaultingToTermZero(@TempDir Path dir) throws IOException {
        // Silently falling back to term 0 on a corrupt file would be LESS safe than refusing to
        // start: it could make a node forget a real vote it already durably cast (see class Javadoc).
        Path file = dir.resolve("raft-state");
        RaftPersistentState.load(file).save(9L, new NodeId("x"));

        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 0xFF; // flip a bit in the trailing checksum
        Files.write(file, bytes);

        assertThrows(IOException.class, () -> RaftPersistentState.load(file));
    }

    @Test
    void loadOfATruncatedFileThrows(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("raft-state");
        Files.write(file, new byte[]{1, 2, 3});
        assertThrows(IOException.class, () -> RaftPersistentState.load(file));
    }

    @Test
    void noTempFileIsLeftBehindAfterASuccessfulSave(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("raft-state");
        RaftPersistentState.load(file).save(1L, null);
        assertFalse(Files.exists(dir.resolve("raft-state.tmp")), "the temp file must be renamed away, never left behind");
        assertTrue(Files.exists(file));
    }
}
