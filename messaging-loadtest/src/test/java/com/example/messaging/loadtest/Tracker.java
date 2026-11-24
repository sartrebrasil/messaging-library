package com.example.messaging.loadtest;

import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.Effect;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Medição do listener. O handler do runner chama {@link #handled}; o {@link ConsumerObserver} entrega o efeito de
 * cada entrega no broker ({@link #delivered}) e as falhas de receive. O observer não sobrescreve o
 * {@code handled} do consumer: o handler do runner já mede cada chamada, e as duas fontes somariam em dobro.
 */
final class Tracker implements ConsumerObserver {

    private final int concurrency;
    private final ConcurrentHashMap<String, Long> lastSeq = new ConcurrentHashMap<>();
    private final Map<String, Boolean> seen = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Long> e2eMillis = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Long> handlerNanos = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Long> effectNanos = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<Effect, LongAdder> effects = new ConcurrentHashMap<>();
    private final LongAdder handlerNanosTotal = new LongAdder();
    private final LongAdder consumed = new LongAdder();
    private final LongAdder duplicates = new LongAdder();
    private final LongAdder outOfOrder = new LongAdder();
    private final LongAdder receiveFailed = new LongAdder();
    private final AtomicLong lastActivityNanos = new AtomicLong();
    /** Mensagens (pelo id do corpo) que chegaram a um estado terminal; entrega duplicada não conta duas vezes. */
    private final Set<String> terminalIds = ConcurrentHashMap.newKeySet();
    private final LongAdder terminalUnkeyed = new LongAdder();

    /** @param concurrency threads que podem rodar o handler ao mesmo tempo: workers × key-lanes */
    Tracker(int concurrency) {
        this.concurrency = concurrency;
    }

    /** Uma chamada do handler; só os sucessos contam como consumo, duplicata e ordem. */
    void handled(String id, String key, long seq, long sentAtMs, boolean success, long nanos) {
        handlerNanos.add(nanos);
        handlerNanosTotal.add(nanos);
        lastActivityNanos.set(System.nanoTime());
        if (!success) {
            return;
        }
        if (seen.putIfAbsent(id, Boolean.TRUE) != null) {
            duplicates.increment();
            return;
        }
        consumed.increment();
        e2eMillis.add(System.currentTimeMillis() - sentAtMs);
        boolean[] late = new boolean[1];
        lastSeq.merge(key, seq, (last, v) -> {
            if (v < last) {
                late[0] = true;
                return last;
            }
            return v;
        });
        if (late[0]) {
            outOfOrder.increment();
        }
    }

    @Override
    public void delivered(Report report) {
        effects.computeIfAbsent(report.effect(), e -> new LongAdder()).increment();
        if (report.duration() != null) {
            effectNanos.add(report.duration().toNanos());
        }
        lastActivityNanos.set(System.nanoTime());
        if (report.effect() == Effect.ACKED || report.effect() == Effect.FAILED
                || report.effect() == Effect.DEAD_LETTERED) {
            if (report.inbox() == null) { // envelope ilegível: sem id para deduplicar
                terminalUnkeyed.increment();
            } else {
                String body = report.inbox().payloadAsString();
                terminalIds.add(body.substring(0, Math.max(0, body.indexOf(';'))));
            }
        }
    }

    @Override
    public void receiveFailed(RuntimeException failure) {
        receiveFailed.increment();
    }

    /** Mensagens distintas que terminaram: ack, failed ou DLQ. */
    long terminal() {
        return terminalIds.size() + terminalUnkeyed.sum();
    }

    long consumed() {
        return consumed.sum();
    }

    long lastActivityNanos() {
        return lastActivityNanos.get();
    }

    Map<String, Object> snapshot(double wallSeconds) {
        double wall = Math.max(1e-9, wallSeconds);
        Map<String, Long> byEffect = new TreeMap<>();
        effects.forEach((e, a) -> byEffect.put(e.name(), a.sum()));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("consumed", consumed.sum());
        m.put("terminalByEffect", byEffect);
        m.put("duplicates", duplicates.sum());
        m.put("outOfOrder", outOfOrder.sum());
        m.put("receiveFailed", receiveFailed.sum());
        m.put("throughputMsgPerSec", round(consumed.sum() / wall));
        m.put("e2eMs", percentiles(toArray(e2eMillis), 1));
        m.put("handlerMs", percentiles(toArray(handlerNanos), 1e6));
        m.put("effectMs", percentiles(toArray(effectNanos), 1e6));
        m.put("workerUtilization", round(handlerNanosTotal.sum() / 1e9 / (concurrency * wall)));
        return m;
    }

    private static long[] toArray(ConcurrentLinkedQueue<Long> q) {
        return q.stream().mapToLong(Long::longValue).toArray();
    }

    /** Mesma fórmula do outbox-loadtest: índice {@code min(n-1, (int)(n*q))} sobre o vetor ordenado. */
    static Map<String, Double> percentiles(long[] values, double divisor) {
        long[] a = values.clone();
        java.util.Arrays.sort(a);
        Map<String, Double> m = new LinkedHashMap<>();
        if (a.length == 0) {
            return m;
        }
        m.put("p50", round(a[Math.min(a.length - 1, (int) (a.length * 0.50))] / divisor));
        m.put("p95", round(a[Math.min(a.length - 1, (int) (a.length * 0.95))] / divisor));
        m.put("p99", round(a[Math.min(a.length - 1, (int) (a.length * 0.99))] / divisor));
        m.put("max", round(a[a.length - 1] / divisor));
        return m;
    }

    static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
