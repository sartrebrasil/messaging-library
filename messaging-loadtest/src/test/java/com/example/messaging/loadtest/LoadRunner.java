package com.example.messaging.loadtest;

import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.inbox.ConsumerSettings;
import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Destinations;
import com.example.messaging.inbox.EnvelopeMode;
import com.example.messaging.inbox.EnvelopeReader;
import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.InboxConsumer;
import com.example.messaging.inbox.InboxResultHandler;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.inbox.Pipeline;
import com.example.messaging.inbox.StopSignal;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Roda um profile: produtores enviam pelo {@link MessageSender}, um {@link InboxConsumer} real consome, e o
 * {@link Tracker} mede.
 *
 * <p>Leitura: {@code workerUtilization} baixo com throughput baixo aponta receive/ack ou broker; alto aponta o
 * handler ou {@code concurrency} curta. {@code maxBacklog} crescendo = consumo mais lento que a produção.
 */
final class LoadRunner {

    private static final long SAMPLE_MS = 250;

    Map<String, Object> run(LoadProfile p, BrokerFixture fixture) throws Exception {
        p.validate();
        int messages = p.num("messages", 1000);
        int producers = p.num("producers", 2);
        int keys = p.num("orderingKeys", 100);
        int payloadBytes = p.num("payloadBytes", 256);
        double ratePerSec = p.rate("ratePerSec");
        boolean preload = p.flag("preload", false);
        boolean consume = p.flag("consume", true);
        int concurrency = p.num("consumer.concurrency", 1);

        BrokerFixture.Endpoints ep = fixture.endpoints(p.name().replaceAll("[^A-Za-z0-9_-]", "-"));
        Tracker tracker = new Tracker(concurrency * p.num("consumer.keyLanes", 1));
        HandlerModel model = HandlerModel.from(p);
        StopSignal stop = new StopSignal();
        InboxConsumer consumer = consume ? buildConsumer(p, ep, tracker, model, stop) : null;

        AtomicLong produced = new AtomicLong();
        List<Map<String, Object>> backlog = new ArrayList<>();
        long start = System.nanoTime();
        long consumeStart = start;
        long produceEnd;
        boolean drained;
        String error = null;
        long end;
        try {
            if (consumer != null && !preload) {
                consumer.start();
            }
            ExecutorService pool = Executors.newFixedThreadPool(producers, r -> {
                Thread t = new Thread(r, "loadtest-producer");
                t.setDaemon(true);
                return t;
            });
            long deadline = start + TimeUnit.SECONDS.toNanos(p.num("timeoutSeconds", 120));
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int w = 0; w < producers; w++) {
                    futures.add(pool.submit(producer(w, producers, messages, keys, payloadBytes, ratePerSec, ep,
                            produced)));
                }
                // amostra o backlog durante a produção também: é aí que ele cresce quando o consumo não acompanha
                while (!futures.stream().allMatch(Future::isDone) && System.nanoTime() < deadline) {
                    sample(backlog, start, produced, tracker);
                    Thread.sleep(SAMPLE_MS);
                }
                for (Future<?> f : futures) {
                    if (f.isDone()) {
                        f.get(); // falha de envio vira "error" no resultado
                    }
                }
            } catch (ExecutionException e) {
                error = String.valueOf(e.getCause());
            } finally {
                pool.shutdownNow();
            }
            produceEnd = System.nanoTime();
            if (consumer != null && preload && error == null) {
                consumeStart = System.nanoTime();
                consumer.start();
            }
            while (consumer != null && error == null && tracker.terminal() < messages
                    && System.nanoTime() < deadline) {
                sample(backlog, start, produced, tracker);
                Thread.sleep(SAMPLE_MS);
            }
            drained = error == null && (consumer == null ? produced.get() >= messages
                    : tracker.terminal() >= messages);
            end = consumer != null && drained ? Math.max(tracker.lastActivityNanos(), consumeStart)
                    : System.nanoTime();
        } finally {
            stop.stop();
            if (consumer != null) {
                consumer.stop();
            }
        }
        double produceSeconds = Math.max(1e-9, (produceEnd - start) / 1e9);
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("drained", drained);
        if (error != null) {
            r.put("error", error);
        }
        r.put("total", (long) messages);
        r.put("produced", produced.get());
        r.put("produceSeconds", Tracker.round(produceSeconds));
        r.put("producedPerSec", Tracker.round(produced.get() / produceSeconds));
        if (consumer != null) {
            double wall = Math.max(1e-9, (end - consumeStart) / 1e9);
            r.put("wallSeconds", Tracker.round(wall));
            r.putAll(tracker.snapshot(wall));
            r.put("maxBacklog", backlog.stream().mapToLong(b -> (Long) b.get("backlog")).max().orElse(0));
            r.put("backlog", backlog);
        }
        return r;
    }

    private static void sample(List<Map<String, Object>> backlog, long start, AtomicLong produced, Tracker tracker) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("tMs", (System.nanoTime() - start) / 1_000_000);
        s.put("produced", produced.get());
        s.put("terminal", tracker.terminal());
        s.put("backlog", produced.get() - tracker.terminal());
        backlog.add(s);
    }

    private InboxConsumer buildConsumer(LoadProfile p, BrokerFixture.Endpoints ep, Tracker tracker,
                                        HandlerModel model, StopSignal stop) {
        String name = "loadtest-" + p.name();
        Duration retryDelay = Duration.ofMillis(p.num("retry.delayMs", 100));
        ConsumerSettings settings = new ConsumerSettings(name, name + "-main", p.num("consumer.concurrency", 1),
                p.num("consumer.maxMessagesPerPoll", 1), Duration.ofMillis(p.num("consumer.pollWaitMs", 1000)),
                Duration.ofMinutes(15), Duration.ofSeconds(10),
                Ordering.valueOf(p.str("consumer.ordering", "RELAXED").toUpperCase()),
                DelayedStrategy.valueOf(p.str("consumer.strategy", "AUTO").toUpperCase().replace('-', '_')),
                false, p.num("consumer.keyLanes", 1), Duration.ofHours(1), 1);
        // um retry imediato curto e dois atrasados; esgotados, a mensagem vai à DLQ (sem passes do nível 3)
        RetryPolicy policy = new RetryPolicy(new BackoffSpec(1, Duration.ofMillis(10), 1.0, Duration.ofMillis(10), 0),
                new BackoffSpec(2, retryDelay, 1.0, retryDelay, 0), true, 0, List.of(), List.of(), List.of());
        InboxResultHandler<String> handler = m -> {
            String[] f = m.payload().split(";", 5);
            long t0 = System.nanoTime();
            try {
                model.handle(f[0]);
            } catch (Exception | Error e) {
                tracker.handled(f[0], f[1], Long.parseLong(f[2]), Long.parseLong(f[3]), false, System.nanoTime() - t0);
                throw e;
            }
            tracker.handled(f[0], f[1], Long.parseLong(f[2]), Long.parseLong(f[3]), true, System.nanoTime() - t0);
            return HandlerResult.ack();
        };
        Pipeline<String> pipeline = new Pipeline<>(name, new EnvelopeReader(EnvelopeMode.AUTO),
                (m, t) -> m.payloadAsString(), String.class, handler, policy, stop, () -> 0.0);
        Destinations destinations = new Destinations(ep.receivers(), ep.failed(), ep.deadLetter(), ep.retryQueues());
        return new InboxConsumer(settings, pipeline, destinations, stop, Clock.systemUTC(), () -> 0.0, tracker);
    }

    /** Produtor {@code w}: as chaves {@code k % producers == w}, cada uma em ordem de {@code seq}. */
    private static Runnable producer(int w, int producers, int messages, int keys, int payloadBytes, double ratePerSec,
                                     BrokerFixture.Endpoints ep, AtomicLong produced) {
        return () -> {
            long intervalNanos = ratePerSec > 0 ? (long) (producers * 1e9 / ratePerSec) : 0;
            long next = System.nanoTime();
            try (MessageSender sender = ep.senders().get()) {
                for (long round = 0; round * keys < messages; round++) {
                    for (int k = w; k < keys; k += producers) {
                        long index = round * keys + k;
                        if (index >= messages) {
                            continue;
                        }
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        if (intervalNanos > 0) {
                            next += intervalNanos;
                            long wait = next - System.nanoTime();
                            if (wait > 0) {
                                LockSupport.parkNanos(wait);
                            }
                        }
                        String id = Long.toString(index);
                        String head = id + ";k" + k + ";" + round + ";" + System.currentTimeMillis() + ";";
                        String body = head + "x".repeat(Math.max(0, payloadBytes - head.length()));
                        sender.send(new OutgoingMessage(body.getBytes(StandardCharsets.UTF_8), Map.of(), null,
                                "k" + k, id, null, null));
                        produced.incrementAndGet();
                    }
                }
            }
        };
    }
}
