package com.forge.common;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CommonModuleSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(CommonModuleSmokeTest.class);

    @Test
    void moduleCompilesAndRunsOnASupportedJavaVersion() {
        log.info("forge-common running on Java {}", Runtime.version());
        assertTrue(Runtime.version().feature() >= 21, "FORGE targets Java 21 (LTS) or newer");
    }
}
