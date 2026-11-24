package com.example.messaging.loadtest;

import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.Effect;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrackerTest {

    private static void ok(Tracker t, String id, String key, long seq) {
        t.handled(id, key, seq, System.currentTimeMillis(), true, 1_000_000L);
    }

    private static void delivered(Tracker t, Effect effect) {
        t.delivered(new ConsumerObserver.Report(null, null, null, effect, null, false, false,
                Duration.ofMillis(2), null));
    }

    @Test
    void sucessoDuplicadoContaDuplicataENaoConsumo() {
        Tracker t = new Tracker(1);
        ok(t, "a", "k", 1);
        ok(t, "a", "k", 1);
        Map<String, Object> s = t.snapshot(1);
        assertEquals(1L, s.get("consumed"));
        assertEquals(1L, s.get("duplicates"));
    }

    @Test
    void seqMenorNaMesmaChaveEForaDeOrdem() {
        Tracker t = new Tracker(1);
        ok(t, "a", "k", 7);
        ok(t, "b", "k", 5);
        ok(t, "c", "outra", 1);
        assertEquals(1L, t.snapshot(1).get("outOfOrder"));
    }

    @Test
    void falhaDoHandlerNaoContaComoConsumo() {
        Tracker t = new Tracker(1);
        t.handled("a", "k", 1, System.currentTimeMillis(), false, 1_000_000L);
        assertEquals(0L, t.snapshot(1).get("consumed"));
    }

    @Test
    void terminaisSomamAckedFailedEDeadLettered() {
        Tracker t = new Tracker(1);
        delivered(t, Effect.ACKED);
        delivered(t, Effect.FAILED);
        delivered(t, Effect.DEAD_LETTERED);
        delivered(t, Effect.REDELIVERY_DELAYED);
        assertEquals(3L, t.terminal());
        @SuppressWarnings("unchecked")
        Map<String, Long> byEffect = (Map<String, Long>) t.snapshot(1).get("terminalByEffect");
        assertEquals(1L, byEffect.get("ACKED"));
        assertEquals(1L, byEffect.get("REDELIVERY_DELAYED"));
    }

    @Test
    void terminalContaMensagensDistintasNaoEntregas() {
        Tracker t = new Tracker(1);
        for (int i = 0; i < 2; i++) { // a mesma mensagem acusada duas vezes (entrega duplicada)
            t.delivered(new ConsumerObserver.Report(null, inbox("7;k0;0;1;x"), null, Effect.ACKED, null, false, false,
                    Duration.ofMillis(1), null));
        }
        assertEquals(1L, t.terminal());
        t.delivered(new ConsumerObserver.Report(null, inbox("8;k0;1;1;x"), null, Effect.DEAD_LETTERED, null, false,
                false, Duration.ofMillis(1), null));
        assertEquals(2L, t.terminal());
    }

    private static com.example.messaging.inbox.InboxMessage inbox(String body) {
        return new com.example.messaging.inbox.InboxMessage("broker-id", null, null, null, null, null, null, null,
                Map.of(), body.getBytes(java.nio.charset.StandardCharsets.UTF_8), "d",
                new com.example.messaging.inbox.Delivery(com.example.messaging.inbox.Delivery.Level.IMMEDIATE, 1));
    }

    @Test
    void percentis() {
        long[] v = IntStream.rangeClosed(1, 100).asLongStream().toArray();
        Map<String, Double> p = Tracker.percentiles(v, 1);
        assertEquals(51.0, p.get("p50"));
        assertEquals(96.0, p.get("p95"));
        assertEquals(100.0, p.get("p99"));
        assertEquals(100.0, p.get("max"));
        assertTrue(Tracker.percentiles(new long[0], 1).isEmpty());
    }

    @Test
    void utilizacaoDosWorkers() {
        Tracker t = new Tracker(2);
        t.handled("a", "k", 1, System.currentTimeMillis(), true, 1_000_000_000L);
        assertEquals(0.5, t.snapshot(1).get("workerUtilization"));
    }

    @Test
    void contagemConcorrenteNaoPerde() throws Exception {
        Tracker t = new Tracker(8);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (int w = 0; w < 8; w++) {
            int base = w * 10_000;
            pool.submit(() -> IntStream.range(0, 10_000).forEach(i -> {
                ok(t, "id" + (base + i), "k" + (i % 50), base + i);
                delivered(t, Effect.ACKED);
            }));
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertEquals(80_000L, t.snapshot(1).get("consumed"));
        assertEquals(80_000L, t.terminal());
    }
}
