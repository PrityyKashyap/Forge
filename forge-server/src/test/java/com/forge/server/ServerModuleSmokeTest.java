package com.forge.server;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerModuleSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(ServerModuleSmokeTest.class);

    @Test
    void moduleCompilesAndRunsOnASupportedJavaVersion() {
        log.info("forge-server running on Java {}", Runtime.version());
        assertTrue(Runtime.version().feature() >= 21, "FORGE targets Java 21 (LTS) or newer");
    }
}
