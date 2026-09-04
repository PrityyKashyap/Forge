package com.forge.storage.repl;

import com.forge.storage.InMemoryKeyValueStore;
import com.forge.storage.repl.Repl.CommandResult;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplTest {

    private Repl repl;

    @BeforeEach
    void setUp() {
        // execute() does no I/O itself, so the streams here are never touched
        // by the command-parsing tests below; only the run()-loop tests at
        // the bottom actually exercise real input/output.
        repl = new Repl(new InMemoryKeyValueStore(), InputStream.nullInputStream(),
                new PrintStream(OutputStream.nullOutputStream()));
    }

    // --- PUT ---------------------------------------------------------

    @Test
    void putNewKeyReportsOk() {
        CommandResult result = repl.execute("PUT foo bar");
        assertEquals("OK", result.message());
        assertFalse(result.exit());
    }

    @Test
    void putOverwriteReportsPreviousValueReplaced() {
        repl.execute("PUT foo bar");
        CommandResult result = repl.execute("PUT foo baz");
        assertEquals("OK (previous value replaced)", result.message());
    }

    @Test
    void putValueMayContainSpaces() {
        repl.execute("PUT foo hello world this is one value");
        CommandResult get = repl.execute("GET foo");
        assertEquals("hello world this is one value", get.message());
    }

    @Test
    void putWithoutValueIsUsageError() {
        CommandResult result = repl.execute("PUT foo");
        assertTrue(result.message().toLowerCase().contains("requires a key and a value"));
    }

    @Test
    void putWithoutAnyArgumentsIsUsageError() {
        CommandResult result = repl.execute("PUT");
        assertTrue(result.message().toLowerCase().contains("requires a key and a value"));
    }

    // --- GET ---------------------------------------------------------

    @Test
    void getMissingKeyReportsNil() {
        assertEquals("(nil)", repl.execute("GET nope").message());
    }

    @Test
    void getAfterPutReturnsStoredValue() {
        repl.execute("PUT foo bar");
        assertEquals("bar", repl.execute("GET foo").message());
    }

    @Test
    void getWithoutKeyIsUsageError() {
        CommandResult result = repl.execute("GET");
        assertTrue(result.message().toLowerCase().contains("requires a key"));
    }

    @Test
    void getWithSpaceInKeyIsUsageError() {
        CommandResult result = repl.execute("GET foo bar");
        assertTrue(result.message().toLowerCase().contains("no spaces"));
    }

    // --- DELETE --------------------------------------------------------

    @Test
    void deleteExistingKeyReportsRemoved() {
        repl.execute("PUT foo bar");
        assertEquals("OK (removed)", repl.execute("DELETE foo").message());
        assertEquals("(nil)", repl.execute("GET foo").message());
    }

    @Test
    void deleteMissingKeyReportsNotFoundAndIsNotAnError() {
        assertEquals("OK (not found)", repl.execute("DELETE nope").message());
    }

    @Test
    void deleteWithoutKeyIsUsageError() {
        CommandResult result = repl.execute("DELETE");
        assertTrue(result.message().toLowerCase().contains("requires a key"));
    }

    // --- HELP / EXIT / QUIT / unknown / blank -----------------------------

    @Test
    void helpReturnsNonEmptyMessageAndDoesNotExit() {
        CommandResult result = repl.execute("HELP");
        assertFalse(result.message().isBlank());
        assertFalse(result.exit());
    }

    @Test
    void exitRequestsLoopTermination() {
        CommandResult result = repl.execute("EXIT");
        assertTrue(result.exit());
    }

    @Test
    void quitRequestsLoopTermination() {
        CommandResult result = repl.execute("QUIT");
        assertTrue(result.exit());
    }

    @Test
    void commandWordIsCaseInsensitive() {
        assertEquals("OK", repl.execute("put a b").message());
        assertEquals("b", repl.execute("get a").message());
        assertTrue(repl.execute("exit").exit());
    }

    @Test
    void unknownCommandIsReportedAndDoesNotExit() {
        CommandResult result = repl.execute("FROBNICATE foo");
        assertTrue(result.message().startsWith("Unknown command"));
        assertFalse(result.exit());
    }

    @Test
    void blankLineProducesNoMessageAndDoesNotExit() {
        CommandResult result = repl.execute("   ");
        assertEquals("", result.message());
        assertFalse(result.exit());
    }

    // --- run() loop, end to end over real streams -------------------------

    @Test
    void runProcessesLinesUntilExitAndStopsReadingAfterward() {
        String input = String.join("\n",
                "PUT foo bar",
                "GET foo",
                "DELETE foo",
                "GET foo",
                "EXIT",
                "GET should-never-be-processed") + "\n";

        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        Repl liveRepl = new Repl(new InMemoryKeyValueStore(),
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                new PrintStream(captured, true, StandardCharsets.UTF_8));

        liveRepl.run();

        List<String> lines = List.of(captured.toString(StandardCharsets.UTF_8).split("\n"));
        // first line is the banner; the rest are one line per non-blank command result
        assertEquals("OK", lines.get(1));
        assertEquals("bar", lines.get(2));
        assertEquals("OK (removed)", lines.get(3));
        assertEquals("(nil)", lines.get(4));
        assertEquals("Bye.", lines.get(5));
        assertEquals(6, lines.size(), "the line after EXIT must never have been processed");
    }
}
