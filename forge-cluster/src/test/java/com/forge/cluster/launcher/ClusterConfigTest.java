package com.forge.cluster.launcher;

import com.forge.cluster.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterConfigTest {

    @Test
    void parsesEveryFieldOfEachLineInOrder(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("cluster.conf");
        Files.writeString(file, """
                # a comment line, and a blank line below should both be ignored

                a  localhost  17001  17002  17003  17004
                b  localhost  17011  17012  17013  17014
                """);

        List<NodeSpec> specs = ClusterConfig.load(file);
        assertEquals(2, specs.size());
        assertEquals(new NodeSpec(new NodeId("a"), "localhost", 17001, 17002, 17003, 17004), specs.get(0));
        assertEquals(new NodeSpec(new NodeId("b"), "localhost", 17011, 17012, 17013, 17014), specs.get(1));
    }

    @Test
    void rejectsAMissingField(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("cluster.conf");
        Files.writeString(file, "a localhost 17001 17002 17003\n");
        assertThrows(IOException.class, () -> ClusterConfig.load(file));
    }

    @Test
    void rejectsANonNumericPort(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("cluster.conf");
        Files.writeString(file, "a localhost not-a-port 17002 17003 17004\n");
        assertThrows(IOException.class, () -> ClusterConfig.load(file));
    }

    @Test
    void rejectsADuplicateNodeId(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("cluster.conf");
        Files.writeString(file, """
                a localhost 17001 17002 17003 17004
                a localhost 17011 17012 17013 17014
                """);
        assertThrows(IOException.class, () -> ClusterConfig.load(file));
    }

    @Test
    void rejectsAnEmptyFile(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("cluster.conf");
        Files.writeString(file, "# nothing but comments\n\n");
        assertThrows(IOException.class, () -> ClusterConfig.load(file));
    }
}
