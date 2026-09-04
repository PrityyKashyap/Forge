package com.forge.storage;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageModuleSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(StorageModuleSmokeTest.class);

    @Test
    void moduleCompilesAndRunsOnASupportedJavaVersion() {
        log.info("forge-storage running on Java {}", Runtime.version());
        assertTrue(Runtime.version().feature() >= 21, "FORGE targets Java 21 (LTS) or newer");
    }
}
