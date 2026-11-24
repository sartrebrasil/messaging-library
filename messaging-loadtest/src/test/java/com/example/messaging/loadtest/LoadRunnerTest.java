package com.example.messaging.loadtest;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** O runner sobre o broker em memória: sem Docker. */
class LoadRunnerTest {

    private static Map<String, Object> run(Map<String, String> props) throws Exception {
        try (BrokerFixture f = BrokerFixture.create("inmemory")) {
            return new LoadRunner().run(LoadProfile.of("t", props), f);
        }
    }

    @Test
    void drenaOCasoPerfeito() throws Exception {
        Map<String, Object> r = run(Map.of("messages", "500", "orderingKeys", "10", "timeoutSeconds", "30"));
        assertEquals(true, r.get("drained"));
        assertEquals(500L, r.get("consumed"));
    }

    @Test
    void timeoutNaProducaoDevolveParcialSemLancarENemDeixarProdutorVivo() throws Exception {
        // 1000 msgs a 50/s levariam 20 s; o prazo é 1 s
        Map<String, Object> r = run(Map.of("messages", "1000", "orderingKeys", "10", "ratePerSec", "50",
                "timeoutSeconds", "1"));
        assertEquals(false, r.get("drained"));
        assertTrue((Long) r.get("produced") < 1000L);
        Thread.sleep(500);
        assertFalse(Thread.getAllStackTraces().keySet().stream()
                .anyMatch(th -> th.getName().equals("loadtest-producer") && th.isAlive()), "produtor ainda vivo");
    }

    @Test
    void amostraOBacklogEnquantoAindaProduz() throws Exception {
        Map<String, Object> r = run(Map.of("messages", "200", "orderingKeys", "10", "ratePerSec", "100",
                "handlerLatency", "fixed:20ms", "timeoutSeconds", "30"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> backlog = (List<Map<String, Object>>) r.get("backlog");
        assertTrue(backlog.stream().anyMatch(b -> (Long) b.get("produced") < 200L),
                "nenhuma amostra durante a produção: " + backlog);
    }
}
