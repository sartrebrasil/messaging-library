package com.example.messaging.loadtest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoadProfileTest {

    @TempDir
    Path dir;

    private static String invalid(Map<String, String> props) {
        return assertThrows(IllegalArgumentException.class, () -> LoadProfile.of("p", props).validate()).getMessage();
    }

    @Test
    void nomeVemDoArquivoSemExtensao() throws Exception {
        Path f = dir.resolve("rabbit-w4.properties");
        Files.writeString(f, "broker = rabbit\nmessages=50\n");
        LoadProfile p = LoadProfile.load(f);
        assertEquals("rabbit-w4", p.name());
        assertEquals("rabbit", p.broker());
        assertEquals(50, p.num("messages", 1));
    }

    @Test
    void ausenteDevolvePadrao() {
        LoadProfile p = LoadProfile.of("p", Map.of());
        assertEquals(1000, p.num("messages", 1000));
        assertEquals("inmemory", p.broker());
        assertEquals(0.0, p.rate("poisonRate"));
        assertTrue(p.flag("consume", true));
    }

    @Test
    void validaCombinacoesInvalidas() {
        assertTrue(invalid(Map.of("preload", "true", "ratePerSec", "100")).contains("preload"));
        assertTrue(invalid(Map.of("poisonRate", "0.7", "transientFailureRate", "0.5")).contains("poisonRate"));
        assertTrue(invalid(Map.of("poisonRate", "-0.1")).contains("poisonRate"));
        assertTrue(invalid(Map.of("orderingKeys", "50", "messages", "10")).contains("orderingKeys"));
    }

    @Test
    void validaValoresQueTravariamOuFalhariamTarde() {
        assertTrue(invalid(Map.of("orderingKeys", "0")).contains("orderingKeys"));
        assertTrue(invalid(Map.of("producers", "0")).contains("producers"));
        assertTrue(invalid(Map.of("consumer.concurrency", "0")).contains("consumer.concurrency"));
        assertTrue(invalid(Map.of("consumer.ordering", "ALEATORIO")).contains("consumer.ordering"));
        assertTrue(invalid(Map.of("consumer.strategy", "nada")).contains("consumer.strategy"));
        assertTrue(invalid(Map.of("handlerLatency", "banana")).contains("handlerLatency"));
    }

    @Test
    void profileMinimoValida() {
        assertDoesNotThrow(() -> LoadProfile.of("p", Map.of("broker", "inmemory")).validate());
    }
}
