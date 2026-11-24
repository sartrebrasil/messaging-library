package com.example.messaging.loadtest;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HandlerModelTest {

    private static HandlerModel model(String... kv) {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return HandlerModel.from(LoadProfile.of("p", m));
    }

    @Test
    void decisaoDeFalhaEDeterministicaPorSeed() {
        HandlerModel a = model("poisonRate", "0.3", "seed", "1");
        HandlerModel b = model("poisonRate", "0.3", "seed", "1");
        HandlerModel c = model("poisonRate", "0.3", "seed", "2");
        for (int i = 0; i < 1000; i++) {
            assertEquals(a.isPoison("id" + i), b.isPoison("id" + i));
            assertEquals(a.isPoison("id" + i), a.isPoison("id" + i));
        }
        long diferentes = IntStream.range(0, 1000).filter(i -> a.isPoison("id" + i) != c.isPoison("id" + i)).count();
        assertTrue(diferentes > 0, "seed diferente deve mudar o conjunto");
    }

    @Test
    void fracaoDePoisonSegueATaxa() {
        HandlerModel m = model("poisonRate", "0.1", "seed", "7");
        long poison = IntStream.range(0, 100_000).filter(i -> m.isPoison(Integer.toString(i))).count();
        assertEquals(0.1, poison / 100_000.0, 0.01);
    }

    @Test
    void transienteFalhaNasPrimeirasTentativasPorId() throws Exception {
        HandlerModel m = model("transientFailureRate", "1", "transientFailAttempts", "2");
        assertThrows(IllegalStateException.class, () -> m.handle("a"));
        assertThrows(IllegalStateException.class, () -> m.handle("a"));
        m.handle("a");
        assertThrows(IllegalStateException.class, () -> m.handle("b")); // contagem é por id
    }

    @Test
    void poisonETransienteSaoDisjuntos() {
        HandlerModel m = model("poisonRate", "0.5", "transientFailureRate", "0.5", "seed", "3");
        for (int i = 0; i < 5000; i++) {
            String id = Integer.toString(i);
            assertFalse(m.isPoison(id) && m.isTransient(id));
            assertTrue(m.isPoison(id) || m.isTransient(id)); // 0.5 + 0.5 cobre tudo
        }
    }

    @Test
    void latenciaFixaDormeOTempo() throws Exception {
        HandlerModel m = model("handlerLatency", "fixed:20ms");
        long t0 = System.nanoTime();
        m.handle("x");
        assertTrue(System.nanoTime() - t0 >= 20_000_000L);
    }

    @Test
    void lognormalTemMedianaNoP50() {
        HandlerModel m = model("handlerLatency", "lognormal:p50=10ms,p99=100ms");
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long[] s = IntStream.range(0, 20_001).mapToLong(i -> m.sampleNanos(r)).sorted().toArray();
        double medianMs = s[s.length / 2] / 1e6;
        assertTrue(medianMs > 7 && medianMs < 14, "mediana=" + medianMs);
        assertTrue(s[(int) (s.length * 0.99)] / 1e6 > 50, "p99 deve ficar bem acima da mediana");
    }

    @Test
    void uniformFicaNaFaixa() {
        HandlerModel m = model("handlerLatency", "uniform:5-50ms");
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long[] s = IntStream.range(0, 2000).mapToLong(i -> m.sampleNanos(r)).toArray();
        assertTrue(Arrays.stream(s).allMatch(n -> n >= 5_000_000L && n <= 50_000_000L));
    }

    @Test
    void latenciaInvalidaCitaAChave() {
        String msg = assertThrows(IllegalArgumentException.class, () -> model("handlerLatency", "banana")).getMessage();
        assertTrue(msg.contains("handlerLatency"));
        assertNotEquals("", msg);
    }

    @Test
    void padraoNuncaDormeNemFalha() {
        HandlerModel m = HandlerModel.from(LoadProfile.of("p", Map.of()));
        long t0 = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            String id = "i" + i;
            assertDoesNotThrow(() -> m.handle(id));
        }
        assertTrue(System.nanoTime() - t0 < 500_000_000L);
    }
}
