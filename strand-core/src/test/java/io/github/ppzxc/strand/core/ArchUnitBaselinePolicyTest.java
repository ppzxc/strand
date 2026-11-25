package io.github.ppzxc.strand.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ArchUnitBaselinePolicyTest {
    @Test
    void checkedInBaselineCannotBeCreatedOrRegeneratedByTests() throws IOException {
        Properties properties = new Properties();
        try (InputStream input = getClass().getResourceAsStream("/archunit.properties")) {
            properties.load(input);
        }

        assertEquals("false", properties.getProperty("freeze.store.default.allowStoreCreation"));
    }

    @Test
    void everyFrozenRuleHasAnExplicitBaselineEntry() throws IOException {
        try (InputStream input = getClass().getResourceAsStream("/archunit_store/stored.rules")) {
            Properties rules = new Properties();
            rules.load(input);

            assertEquals(
                    new HashSet<>(Arrays.asList(
                            "no classes that reside in a package 'io.github.ppzxc.strand.core..' should depend on classes that reside in any package ['io.github.ppzxc.strand.nio..', 'io.github.ppzxc.strand.netty..']",
                            "slices matching 'io.github.ppzxc.strand.core.(*)..' should be free of cycles")),
                    rules.stringPropertyNames());
        }
    }
}
