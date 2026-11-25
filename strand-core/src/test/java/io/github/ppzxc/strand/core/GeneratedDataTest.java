package io.github.ppzxc.strand.core;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.instancio.Instancio;
import org.instancio.junit.InstancioExtension;
import org.instancio.junit.Seed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(InstancioExtension.class)
class GeneratedDataTest {
    @Test
    @Seed(12345L)
    void generatedTextIsAvailableForSeededReproduction() {
        assertFalse(Instancio.create(String.class).isEmpty());
    }
}
