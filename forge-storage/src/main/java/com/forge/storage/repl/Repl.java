package com.forge.storage.repl;

import com.forge.storage.DurableKeyValueStore;
import com.forge.storage.KeyValueStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * An interactive line-based REPL over a {@link KeyValueStore}.
 *
 * <p>This class talks to the store purely through the {@link KeyValueStore}
 * interface, never a concrete implementation, so swapping the engine in a
 * later phase requires no change here. UTF-8 text/{@code byte[]} conversion
 * happens only in this class: {@link KeyValueStore} itself never sees or
 * produces text, only bytes.
 *
 * <p>Supported commands (the command word is case-insensitive; keys and
 * values are used exactly as typed):
 * <pre>
 *   PUT &lt;key&gt; &lt;value...&gt;   store value (rest of the line) under key
 *   GET &lt;key&gt;              print the value for key, or (nil)
 *   DELETE &lt;key&gt;           remove key
 *   HELP                    show this list
 *   EXIT | QUIT             end the session
 * </pre>
 */
public final class Repl {

    private static final String HELP_TEXT = """
            Commands:
              PUT <key> <value...>   store value (rest of the line) under key
              GET <key>              print the value for key, or (nil)
              DELETE <key>           remove key
              HELP                   show this list
              EXIT | QUIT            end the session""";

    /**
     * Deliberately says nothing about durability, threading, or which engine
     * is behind {@link #store} — this class only ever talks to a
     * {@link KeyValueStore}, so it has no way to know those properties, and
     * hardcoding a claim here has already gone stale once (a durable store
     * wired in by {@link #main} while this text still said "not durable").
     * A caller that knows which concrete store it constructed — like
     * {@link #main} does — is responsible for announcing that store's own
     * guarantees itself.
     */
    private static final String BANNER = "FORGE REPL. Type HELP for commands.";

    private final KeyValueStore store;
    private final BufferedReader reader;
    private final PrintStream out;

    public Repl(KeyValueStore store, InputStream in, PrintStream out) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.reader = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(in, "in must not be null"), StandardCharsets.UTF_8));
        this.out = Objects.requireNonNull(out, "out must not be null");
    }

    /** Runs the read-eval-print loop until the input ends or EXIT/QUIT is entered. */
    public void run() {
        out.println(BANNER);
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                CommandResult result = execute(line);
                if (!result.message().isEmpty()) {
                    out.println(result.message());
                }
                if (result.exit()) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Parses and executes a single line against {@link #store}. Package-private
     * so tests can exercise command parsing/dispatch without driving I/O streams.
     */
    CommandResult execute(String line) {
        String trimmed = line.strip();
        if (trimmed.isEmpty()) {
            return new CommandResult("", false);
        }

        int firstSpace = trimmed.indexOf(' ');
        String command = (firstSpace == -1 ? trimmed : trimmed.substring(0, firstSpace))
                .toUpperCase(Locale.ROOT);
        String rest = firstSpace == -1 ? "" : trimmed.substring(firstSpace + 1).stripLeading();

        return switch (command) {
            case "PUT" -> handlePut(rest);
            case "GET" -> handleGet(rest);
            case "DELETE" -> handleDelete(rest);
            case "HELP" -> new CommandResult(HELP_TEXT, false);
            case "EXIT", "QUIT" -> new CommandResult("Bye.", true);
            default -> error("Unknown command: " + command + ". Type HELP for a list of commands.");
        };
    }

    private CommandResult handlePut(String rest) {
        if (rest.isEmpty()) {
            return error("PUT requires a key and a value.");
        }
        int sp = rest.indexOf(' ');
        if (sp == -1) {
            return error("PUT requires a key and a value.");
        }
        String key = rest.substring(0, sp);
        String valueText = rest.substring(sp + 1);
        Optional<byte[]> previous = store.put(key, valueText.getBytes(StandardCharsets.UTF_8));
        return previous.isPresent()
                ? new CommandResult("OK (previous value replaced)", false)
                : new CommandResult("OK", false);
    }

    private CommandResult handleGet(String rest) {
        if (rest.isEmpty()) {
            return error("GET requires a key.");
        }
        if (rest.indexOf(' ') != -1) {
            return error("GET takes exactly one key (no spaces).");
        }
        return store.get(rest)
                .map(bytes -> new CommandResult(new String(bytes, StandardCharsets.UTF_8), false))
                .orElse(new CommandResult("(nil)", false));
    }

    private CommandResult handleDelete(String rest) {
        if (rest.isEmpty()) {
            return error("DELETE requires a key.");
        }
        if (rest.indexOf(' ') != -1) {
            return error("DELETE takes exactly one key (no spaces).");
        }
        Optional<byte[]> removed = store.delete(rest);
        return removed.isPresent()
                ? new CommandResult("OK (removed)", false)
                : new CommandResult("OK (not found)", false);
    }

    private static CommandResult error(String message) {
        return new CommandResult(message, false);
    }

    /** The outcome of executing one REPL line: what to print, and whether to stop the loop. */
    record CommandResult(String message, boolean exit) {
        CommandResult {
            Objects.requireNonNull(message, "message must not be null");
        }
    }

    /**
     * Runs the REPL against a {@link DurableKeyValueStore}, so this is now an
     * actual demonstration of Phase 2's durability, not just Phase 1's engine.
     * The WAL file path is the first CLI argument, defaulting to {@code forge.wal}
     * in the current directory.
     */
    public static void main(String[] args) throws IOException {
        Path walFile = args.length > 0 ? Path.of(args[0]) : Path.of("forge.wal");
        System.out.println("Using WAL file: " + walFile.toAbsolutePath());
        System.out.println("Backed by a durable, write-ahead-logged store: "
                + "writes survive a restart, but this session is still single-threaded.");
        try (DurableKeyValueStore store = new DurableKeyValueStore(walFile)) {
            new Repl(store, System.in, System.out).run();
        }
    }
}
