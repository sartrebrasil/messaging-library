package com.example.messaging.inbox;

import com.example.messaging.Capabilities;
import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/** The loop and the effects over {@link InMemoryMessaging} (ADR-0009, ADR-0002). */
class InboxConsumerTest {

    private static final String MAIN = "orders";
    private static final String DLQ = "orders-dlq";
    private static final String FAILED = "orders-failed";

    @SkipImmediate
    static class RateLimited extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private final InMemoryMessaging messaging = new InMemoryMessaging();
    private final List<InboxConsumer> consumers = new ArrayList<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final List<TypedMessage<String>> handled = new CopyOnWriteArrayList<>();

    InboxConsumerTest() {
        messaging.createQueue(MAIN, QueueOptions.defaults().withLease(Duration.ofMillis(400)));
        messaging.createQueue(DLQ, QueueOptions.defaults());
        messaging.createQueue(FAILED, QueueOptions.defaults());
    }

    @AfterEach
    void stopAll() {
        consumers.forEach(InboxConsumer::stop);
    }

    @Test
    void successIsAckedAndTheLivenessAdvances() {
        InboxConsumer consumer = start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), ok(), dlqAndFailed(List.of()));
        Instant started = consumer.lastReceive();
        send("{\"messageId\":\"m-1\",\"eventType\":\"E\",\"payload\":\"hi\"}");

        await(() -> messaging.size(MAIN) == 0);
        assertEquals("m-1", handled.getFirst().message().id());
        assertEquals(DelayedStrategy.BROKER_DELAY, consumer.strategy());
        await(() -> consumer.lastReceive().isAfter(started));
    }

    @Test
    void aFailingReceiveDoesNotKillTheWorker() {
        var failures = new AtomicInteger(1);
        Supplier<MessageReceiver> flaky = () -> new Delegating(messaging.receiver(MAIN)) {
            @Override
            public List<ReceivedMessage> receive(int max, Duration wait) {
                if (failures.getAndDecrement() > 0) {
                    throw new MessagingException("memory", "network down", null, true);
                }
                return super.receive(max, wait);
            }
        };
        start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), ok(),
                new Destinations(flaky, messaging.queueSender(FAILED), messaging.queueSender(DLQ), List.of()));
        send("plain");

        await(() -> handled.size() == 1);
    }

    @Test
    void anErrorFromTheHandlerReleasesTheMessageAndTheWorkerGoesOn() {
        start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), m -> {
            if (calls.incrementAndGet() == 1) {
                throw new AssertionError("bug");
            }
            handled.add(m);
            return null;
        }, dlqAndFailed(List.of()));
        send("plain");

        await(() -> handled.size() == 1 && messaging.size(MAIN) == 0);
        assertEquals(2, calls.get());
    }

    @Test
    void aSlowHandlerKeepsTheLease() {
        start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), m -> {
            calls.incrementAndGet();
            Thread.sleep(1_000);
            handled.add(m);
            return null;
        }, dlqAndFailed(List.of()));
        send("plain");

        await(() -> handled.size() == 1 && messaging.size(MAIN) == 0);
        assertEquals(1, calls.get());
    }

    @Test
    void stopWaitsForTheRunningHandlerWithoutInterruptingIt() {
        var interrupted = new AtomicBoolean();
        InboxConsumer consumer = start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), m -> {
            calls.incrementAndGet();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
            handled.add(m);
            return null;
        }, dlqAndFailed(List.of()));
        send("plain");
        await(() -> calls.get() == 1);

        consumer.stop();

        assertEquals(1, handled.size());
        assertFalse(interrupted.get());
        assertEquals(0, messaging.size(MAIN));
        assertFalse(consumer.running());
    }

    @Test
    void stopReleasesTheMessagesOfThePollThatDidNotStart() {
        ConsumerSettings three = new ConsumerSettings("billing", MAIN, 1, 3, Duration.ofMillis(100),
                Duration.ofSeconds(5), Duration.ofSeconds(5), Ordering.RELAXED, DelayedStrategy.AUTO, true);
        send("a");
        send("b");
        send("c");
        InboxConsumer consumer = start(three, policy(2, 0), m -> {
            calls.incrementAndGet();
            Thread.sleep(300);
            handled.add(m);
            return null;
        }, dlqAndFailed(List.of()));
        await(() -> calls.get() == 1);

        consumer.stop();

        assertEquals(1, handled.size());
        assertEquals(2, messaging.size(MAIN));
        assertEquals(2, messaging.receiver(MAIN).receive(10, Duration.ZERO).size(), "released right away");
    }

    @Test
    void stopCutsAHoldWaitAndReleasesTheMessage() {
        InboxConsumer consumer = start(settings(DelayedStrategy.HOLD, 1), policy(5, Duration.ofSeconds(30)),
                m -> {
                    calls.incrementAndGet();
                    throw new RateLimited();
                }, dlqAndFailed(List.of()));
        send("plain");
        await(() -> calls.get() == 1);

        long start = System.nanoTime();
        consumer.stop();

        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(2)) < 0);
        assertEquals(1, messaging.receiver(MAIN).receive(10, Duration.ZERO).size(), "released right away");
    }

    @Test
    void brokerDelayRedeliversThenSendsToTheDeadLetterDestination() {
        start(settings(DelayedStrategy.BROKER_DELAY, 1), policy(2, Duration.ofMillis(50)), m -> {
            calls.incrementAndGet();
            throw new RateLimited();
        }, dlqAndFailed(List.of()));
        String id = send("plain");

        await(() -> messaging.size(DLQ) == 1);
        assertEquals(3, calls.get());
        assertEquals(0, messaging.size(MAIN));
        ReceivedMessage copy = messaging.receiver(DLQ).receive(1, Duration.ZERO).getFirst();
        assertEquals("l2=2;l3=0", copy.attributes().get("inbox_attempts"));
        assertTrue(copy.attributes().get("inbox_failure").contains("exception=" + RateLimited.class.getName()));
        assertEquals(id, copy.deadLetter().originalMessageId());
        assertEquals("plain", copy.bodyAsString());
    }

    /** Pub/Sub sem dead-letter policy: sem deliveryCount o nível 2 não conta pelo broker, então segura em memória. */
    @Test
    void brokerDelayWithoutDeliveryCountHoldsInMemory() {
        var receives = new AtomicInteger();
        Destinations noCount = new Destinations(() -> new Delegating(messaging.receiver(MAIN)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                return super.receive(maxMessages, maxWait).stream().map(m -> {
                    receives.incrementAndGet();
                    return new ReceivedMessage(m.messageId(), m.body(), m.attributes(), m.contentType(),
                            m.orderingKey(), java.util.OptionalInt.empty(), m.enqueuedAt(), m.receivedAt(),
                            m.leaseExpiresAt(), m.traceparent(), m.deadLetter(), m.handle());
                }).toList();
            }
        }, messaging.queueSender(FAILED), messaging.queueSender(DLQ), List.of());
        start(settings(DelayedStrategy.BROKER_DELAY, 1), policy(2, Duration.ofMillis(50)), m -> {
            calls.incrementAndGet();
            throw new RateLimited();
        }, noCount);
        send("plain");

        await(() -> messaging.size(DLQ) == 1);
        assertEquals(3, calls.get());
        assertEquals(1, receives.get(), "the delayed level stayed in memory");
        ReceivedMessage copy = messaging.receiver(DLQ).receive(1, Duration.ZERO).getFirst();
        assertEquals("l2=2;l3=0", copy.attributes().get("inbox_attempts"));
    }

    @Test
    void holdRetriesInMemoryWithoutReturningToTheBroker() {
        var levels = new CopyOnWriteArrayList<Delivery>();
        start(settings(DelayedStrategy.HOLD, 1), policy(5, Duration.ofMillis(20)), m -> {
            levels.add(m.message().delivery());
            if (calls.incrementAndGet() < 3) {
                throw new RateLimited();
            }
            handled.add(m);
            return null;
        }, dlqAndFailed(List.of()));
        send("plain");

        await(() -> handled.size() == 1 && messaging.size(MAIN) == 0);
        assertEquals(List.of(Delivery.first(), new Delivery(Delivery.Level.DELAYED, 1),
                new Delivery(Delivery.Level.DELAYED, 2)), levels);
    }

    @Test
    void holdExhaustsAfterTheDelayedAttempts() {
        start(settings(DelayedStrategy.HOLD, 1), policy(2, Duration.ofMillis(10)), m -> {
            calls.incrementAndGet();
            throw new RateLimited();
        }, dlqAndFailed(List.of()));
        send("plain");

        await(() -> messaging.size(DLQ) == 1);
        assertEquals(3, calls.get());
    }

    @Test
    void retryQueuesCopyToTheBandAndAckTheOriginal() {
        messaging.createQueue("retry-short", QueueOptions.defaults());
        messaging.createQueue("retry-long", QueueOptions.defaults());
        List<RetryQueue> bands = List.of(new RetryQueue(Duration.ofMinutes(10), messaging.queueSender("retry-long")),
                new RetryQueue(Duration.ofSeconds(1), messaging.queueSender("retry-short")));
        start(settings(DelayedStrategy.RETRY_QUEUES, 1), policy(5, Duration.ofMillis(500)), m -> {
            throw new RateLimited();
        }, dlqAndFailed(bands));
        send("plain");

        await(() -> messaging.size("retry-short") == 1 && messaging.size(MAIN) == 0);
        ReceivedMessage copy = messaging.receiver("retry-short").receive(1, Duration.ZERO).getFirst();
        assertEquals("l2=1;l3=0", copy.attributes().get("inbox_attempts"));
        assertNull(copy.attributes().get("inbox_failure"));
        assertEquals(0, messaging.size("retry-long"));
    }

    @Test
    void nonRetryableGoesToFailedAfterOneExecution() {
        start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), m -> {
            calls.incrementAndGet();
            throw new IllegalArgumentException("invalid order");
        }, dlqAndFailed(List.of()));
        String id = send("plain");

        await(() -> messaging.size(FAILED) == 1 && messaging.size(MAIN) == 0);
        assertEquals(1, calls.get());
        ReceivedMessage copy = messaging.receiver(FAILED).receive(1, Duration.ZERO).getFirst();
        assertTrue(copy.attributes().get("inbox_failure").contains("exception=java.lang.IllegalArgumentException"));
        assertEquals("l2=0;l3=0", copy.attributes().get("inbox_attempts"));
        assertEquals("java.lang.IllegalArgumentException: invalid order", copy.deadLetter().reason());
        assertEquals(id, copy.deadLetter().originalMessageId());
    }

    @Test
    void rejectCarriesTheHandlerReasonToFailed() {
        start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), m -> HandlerResult.reject("order without cnpj"),
                dlqAndFailed(List.of()));
        send("plain");

        await(() -> messaging.size(FAILED) == 1 && messaging.size(MAIN) == 0);
        ReceivedMessage copy = messaging.receiver(FAILED).receive(1, Duration.ZERO).getFirst();
        assertEquals("rejected: order without cnpj", copy.deadLetter().reason());
    }

    @Test
    void aFailedAckAfterTheCopyDuplicatesInsteadOfLosing() {
        var ackFailures = new AtomicInteger(1);
        Supplier<MessageReceiver> flakyAck = () -> new Delegating(messaging.receiver(MAIN)) {
            @Override
            public void ack(ReceivedMessage message) {
                if (ackFailures.getAndDecrement() > 0) {
                    throw new MessagingException("memory", "ack lost", null, true);
                }
                super.ack(message);
            }
        };
        start(settings(DelayedStrategy.AUTO, 1), policy(2, 0), m -> {
            calls.incrementAndGet();
            throw new IllegalArgumentException("invalid order");
        }, new Destinations(flakyAck, messaging.queueSender(FAILED), messaging.queueSender(DLQ), List.of()));
        send("plain");

        await(() -> messaging.size(FAILED) == 2 && messaging.size(MAIN) == 0);
        assertEquals(2, calls.get());
    }

    @Test
    void withIdempotencyADuplicateCopyIsAckedWithoutTheHandler() {
        var idempotency = new com.example.messaging.inbox.InboxIdempotency(
                new com.example.idempotency.store.InMemoryIdempotencyStore(), Duration.ofMinutes(1), Duration.ofDays(7),
                Clock.systemUTC());
        var stop = new StopSignal();
        var pipeline = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> m.payloadAsString(),
                String.class, ok(), policy(2, 0), stop, () -> 0.0, idempotency);
        var consumer = new InboxConsumer(settings(DelayedStrategy.AUTO, 1), pipeline, dlqAndFailed(List.of()), stop,
                Clock.systemUTC(), () -> 0.0);
        consumers.add(consumer);
        consumer.start();
        String outbox = "{\"messageId\":\"m-1\",\"eventType\":\"E\",\"payload\":\"hi\"}";
        send(outbox);
        send(outbox);

        await(() -> messaging.size(MAIN) == 0);
        assertEquals(1, handled.size());
    }

    @Test
    void holdWaitingForAnotherInstanceDoesNotSpendAttempts() {
        var store = new com.example.idempotency.store.InMemoryIdempotencyStore();
        var key = new com.example.idempotency.model.IdempotencyKeyId("m-1", "billing");
        var other = com.example.idempotency.model.IdempotencyRecord.inProgress(key, "other", null, Instant.now(),
                Instant.now().plusSeconds(60));
        store.reserve(other);
        var levels = new CopyOnWriteArrayList<Delivery>();
        var stop = new StopSignal();
        var pipeline = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> m.payloadAsString(),
                String.class, m -> {
                    levels.add(m.message().delivery());
                    handled.add(m);
                    return null;
                }, policy(1, Duration.ofMillis(20)), stop, () -> 0.0,
                new com.example.messaging.inbox.InboxIdempotency(store, Duration.ofMinutes(1), Duration.ofDays(7),
                        Clock.systemUTC()));
        var consumer = new InboxConsumer(settings(DelayedStrategy.HOLD, 1), pipeline, dlqAndFailed(List.of()), stop,
                Clock.systemUTC(), () -> 0.0);
        consumers.add(consumer);
        consumer.start();
        send("{\"messageId\":\"m-1\",\"eventType\":\"E\",\"payload\":\"hi\"}");

        try {
            Thread.sleep(200); // several in_progress waits, more than delayed.max-attempts
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        store.release(key, "other");

        await(() -> handled.size() == 1 && messaging.size(MAIN) == 0);
        assertEquals(List.of(Delivery.first()), levels);
        assertEquals(0, messaging.size(DLQ));
    }

    /**
     * O dono da reserva caiu: o hold não segura a mensagem além do orçamento do keepAlive; libera ao broker. Daí em
     * diante as reentregas contam no nível 2, como no broker-delay (ADR-0004).
     */
    @Test
    void holdWaitingForAnotherInstanceReleasesAfterTheLeaseBudget() {
        var store = new com.example.idempotency.store.InMemoryIdempotencyStore();
        var key = new com.example.idempotency.model.IdempotencyKeyId("m-1", "billing");
        store.reserve(com.example.idempotency.model.IdempotencyRecord.inProgress(key, "other", null, Instant.now(),
                Instant.now().plusSeconds(60)));
        var receives = new AtomicInteger();
        var stop = new StopSignal();
        var pipeline = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> m.payloadAsString(),
                String.class, m -> {
                    handled.add(m);
                    return null;
                }, policy(5, Duration.ofMillis(20)), stop, () -> 0.0,
                new com.example.messaging.inbox.InboxIdempotency(store, Duration.ofMinutes(1), Duration.ofDays(7),
                        Clock.systemUTC()));
        var shortBudget = new ConsumerSettings("billing", MAIN, 1, 1, Duration.ofMillis(100), Duration.ofMillis(300),
                Duration.ofSeconds(5), Ordering.RELAXED, DelayedStrategy.HOLD, true);
        var counting = new Destinations(() -> new Delegating(messaging.receiver(MAIN)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                List<ReceivedMessage> received = super.receive(maxMessages, maxWait);
                receives.addAndGet(received.size());
                return received;
            }
        }, messaging.queueSender(FAILED), messaging.queueSender(DLQ), List.of());
        var consumer = new InboxConsumer(shortBudget, pipeline, counting, stop, Clock.systemUTC(), () -> 0.0);
        consumers.add(consumer);
        consumer.start();
        send("{\"messageId\":\"m-1\",\"eventType\":\"E\",\"payload\":\"hi\"}");

        await(() -> receives.get() >= 2);
        assertTrue(handled.isEmpty());
        assertEquals(0, messaging.size(DLQ));
    }

    private static ConsumerSettings passSettings(Duration interval, Duration shutdownTimeout) {
        return new ConsumerSettings("billing", MAIN, 1, 1, Duration.ofMillis(50), Duration.ofSeconds(5),
                shutdownTimeout, Ordering.RELAXED, DelayedStrategy.AUTO, true, 1, interval, 100);
    }

    private Destinations withDeadLetterPass() {
        return new Destinations(() -> messaging.receiver(MAIN), messaging.queueSender(FAILED),
                messaging.queueSender(DLQ), List.of(), () -> messaging.receiver(DLQ));
    }

    private void parkInDeadLetter(String id) {
        messaging.queueSender(DLQ).send(OutgoingMessage.ofText("{\"messageId\":\"" + id
                + "\",\"eventType\":\"E\",\"payload\":\"hi\"}").withAttribute("inbox_attempts", "l2=5;l3=0"));
    }

    private static RetryPolicy passPolicy(int deadLetterAttempts) {
        return new RetryPolicy(BackoffSpec.none(), new BackoffSpec(1, Duration.ofMillis(20), 1.0, Duration.ofMillis(20), 0),
                true, deadLetterAttempts, List.of(IllegalArgumentException.class), List.of(), List.of());
    }

    @Test
    void deadLetterPassRunsOnItsInterval() {
        parkInDeadLetter("m-1");
        start(passSettings(Duration.ofMillis(200), Duration.ofSeconds(2)), passPolicy(3), ok(), withDeadLetterPass());

        await(() -> messaging.size(DLQ) == 0);
        assertEquals(1, handled.size());
        assertEquals(Delivery.Level.DEAD_LETTER, handled.getFirst().message().delivery().level());
    }

    @Test
    void noPassWithoutDeadLetterAttempts() throws InterruptedException {
        parkInDeadLetter("m-1");
        start(passSettings(Duration.ofMillis(100), Duration.ofSeconds(2)), passPolicy(0), ok(), withDeadLetterPass());

        Thread.sleep(500);
        assertEquals(1, messaging.size(DLQ));
        assertTrue(Thread.getAllStackTraces().keySet().stream()
                .noneMatch(t -> t.getName().equals("inbox-billing-dead-letter")));
    }

    @Test
    void stopDuringAPassDoesNotHang() {
        var blocked = new java.util.concurrent.CountDownLatch(1);
        var entered = new java.util.concurrent.CountDownLatch(1);
        parkInDeadLetter("m-1");
        InboxConsumer consumer = start(passSettings(Duration.ofMillis(100), Duration.ofSeconds(1)), passPolicy(3), m -> {
            entered.countDown();
            blocked.await();
            return null;
        }, withDeadLetterPass());
        await(() -> entered.getCount() == 0);

        long begin = System.nanoTime();
        consumer.stop();

        assertTrue(Duration.ofNanos(System.nanoTime() - begin).compareTo(Duration.ofSeconds(3)) < 0);
        await(() -> messaging.size(DLQ) == 1);
    }

    @Test
    void slowPassDoesNotOverlap() {
        var running = new AtomicInteger();
        var overlapped = new AtomicBoolean();
        parkInDeadLetter("m-1");
        parkInDeadLetter("m-2");
        start(passSettings(Duration.ofMillis(100), Duration.ofSeconds(2)), passPolicy(3), m -> {
            if (running.incrementAndGet() > 1) {
                overlapped.set(true);
            }
            Thread.sleep(300);
            running.decrementAndGet();
            handled.add(m);
            return null;
        }, withDeadLetterPass());

        await(() -> handled.size() == 2);
        assertFalse(overlapped.get());
    }

    @Test
    void startupRejectsAReservationTtlBelowMaxProcessing() {
        var idempotency = new com.example.messaging.inbox.InboxIdempotency(
                new com.example.idempotency.store.InMemoryIdempotencyStore(), Duration.ofSeconds(5), Duration.ofDays(7),
                Clock.systemUTC());
        var stop = new StopSignal();
        var pipeline = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> "x",
                String.class, ok(), policy(2, 0), stop, () -> 0.0, idempotency);

        assertThrows(IllegalStateException.class, () -> new InboxConsumer(settings(DelayedStrategy.AUTO, 1), pipeline,
                dlqAndFailed(List.of()), stop, Clock.systemUTC(), () -> 0.0));
    }

    @Test
    void theObserverSeesEachDeliveryOnceAndItsFailuresDoNotTouchMessages() {
        var reports = new CopyOnWriteArrayList<ConsumerObserver.Report>();
        var retries = new AtomicInteger();
        var observer = new ConsumerObserver() {
            @Override
            public void immediateRetry(InboxMessage message, Throwable failure) {
                retries.incrementAndGet();
            }

            @Override
            public void delivered(Report report) {
                reports.add(report);
                throw new IllegalStateException("metrics down");
            }
        };
        var stop = new StopSignal();
        var immediateOnce = new RetryPolicy(new BackoffSpec(1, Duration.ofMillis(1), 1, Duration.ofMillis(1), 0),
                BackoffSpec.none(), true, 3, List.of(), List.of(), List.of());
        var pipeline = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> m.payloadAsString(),
                String.class, m -> {
                    if (calls.incrementAndGet() == 1) {
                        throw new IllegalStateException("timeout");
                    }
                    return null;
                }, immediateOnce, stop, () -> 0.0);
        var consumer = new InboxConsumer(settings(DelayedStrategy.AUTO, 1), pipeline, dlqAndFailed(List.of()), stop,
                Clock.systemUTC(), () -> 0.0, observer);
        consumers.add(consumer);
        consumer.start();
        send("plain");

        await(() -> reports.size() == 1 && messaging.size(MAIN) == 0);
        ConsumerObserver.Report report = reports.getFirst();
        assertEquals(Effect.ACKED, report.effect());
        assertEquals(new Outcome.Ack(Outcome.AckReason.SUCCESS), report.outcome());
        assertEquals("plain", report.inbox().payloadAsString());
        assertEquals(1, retries.get());
        assertEquals(2, calls.get());
    }

    @Test
    void keyLanesHandleABatchInParallel() {
        for (int i = 0; i < 10; i++) {
            send("m" + i);
        }
        long start = System.nanoTime();
        start(lanes(10), policy(2, 0), m -> {
            Thread.sleep(200);
            handled.add(m);
            return null;
        }, dlqAndFailed(List.of()));

        await(() -> handled.size() == 10 && messaging.size(MAIN) == 0);
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofMillis(1500)) < 0,
                "ten 200 ms messages in parallel, not in sequence");
    }

    @Test
    void keyLanesKeepTheOrderOfOneKey() {
        var inKey = new AtomicInteger();
        var overlap = new AtomicBoolean();
        var order = new CopyOnWriteArrayList<String>();
        for (int i = 0; i < 5; i++) {
            messaging.queueSender(MAIN).send(OutgoingMessage.ofText("doc-" + i).withOrderingKey("doc"));
        }
        for (int i = 0; i < 5; i++) {
            send("other-" + i);
        }
        start(lanes(10), policy(2, 0), m -> {
            boolean keyed = m.payload().startsWith("doc-");
            if (keyed && inKey.incrementAndGet() > 1) {
                overlap.set(true);
            }
            Thread.sleep(50);
            if (keyed) {
                order.add(m.payload());
                inKey.decrementAndGet();
            }
            handled.add(m);
            return null;
        }, dlqAndFailed(List.of()));

        await(() -> handled.size() == 10 && messaging.size(MAIN) == 0);
        assertFalse(overlap.get(), "one key runs in sequence");
        assertEquals(List.of("doc-0", "doc-1", "doc-2", "doc-3", "doc-4"), order);
    }

    private static ConsumerSettings lanes(int keyLanes) {
        return new ConsumerSettings("billing", MAIN, 1, 10, Duration.ofMillis(100), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Ordering.RELAXED, DelayedStrategy.AUTO, true, keyLanes);
    }

    @Test
    void startupRejectsWhatCannotWork() {
        var strict = new ConsumerSettings("billing", MAIN, 1, 1, Duration.ofMillis(100), Duration.ofSeconds(5),
                Duration.ofSeconds(2), Ordering.STRICT, DelayedStrategy.AUTO, true);
        var noDlq = new Destinations(() -> messaging.receiver(MAIN), messaging.queueSender(FAILED), null, List.of());

        assertThrows(IllegalStateException.class, () -> consumer(strict, policy(2, 0), ok(), dlqAndFailed(List.of())));
        assertThrows(IllegalStateException.class, () -> consumer(settings(DelayedStrategy.AUTO, 1), policy(2, 0), ok(), noDlq));
        assertThrows(IllegalStateException.class,
                () -> consumer(settings(DelayedStrategy.RETRY_QUEUES, 1), policy(2, 0), ok(), dlqAndFailed(List.of())));
        assertThrows(NullPointerException.class,
                () -> new Destinations(() -> messaging.receiver(MAIN), null, null, List.of()));
    }

    @Test
    void stopIsIdempotentAndSafeBeforeStart() {
        InboxConsumer consumer = consumer(settings(DelayedStrategy.AUTO, 1), policy(2, 0), ok(), dlqAndFailed(List.of()));

        consumer.stop();
        consumer.stop();

        assertFalse(consumer.running());
        assertThrows(IllegalStateException.class, consumer::start);
    }

    // --- helpers

    private InboxResultHandler<String> ok() {
        return m -> {
            handled.add(m);
            return null;
        };
    }

    private String send(String body) {
        return messaging.queueSender(MAIN).send(OutgoingMessage.ofText(body)).messageId();
    }

    private Destinations dlqAndFailed(List<RetryQueue> bands) {
        return new Destinations(() -> messaging.receiver(MAIN), messaging.queueSender(FAILED),
                messaging.queueSender(DLQ), bands);
    }

    private static ConsumerSettings settings(DelayedStrategy strategy, int concurrency) {
        return new ConsumerSettings("billing", MAIN, concurrency, 1, Duration.ofMillis(100), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Ordering.RELAXED, strategy, true);
    }

    private static RetryPolicy policy(int delayedAttempts, long delayMillis) {
        return policy(delayedAttempts, Duration.ofMillis(delayMillis));
    }

    private static RetryPolicy policy(int delayedAttempts, Duration delay) {
        return new RetryPolicy(BackoffSpec.none(),
                new BackoffSpec(delayedAttempts, delay, 1.0, delay, 0),
                true, 3, List.of(IllegalArgumentException.class), List.of(), List.of());
    }

    private InboxConsumer start(ConsumerSettings settings, RetryPolicy policy, InboxResultHandler<String> handler,
                                Destinations destinations) {
        InboxConsumer consumer = consumer(settings, policy, handler, destinations);
        consumer.start();
        return consumer;
    }

    private InboxConsumer consumer(ConsumerSettings settings, RetryPolicy policy, InboxResultHandler<String> handler,
                                   Destinations destinations) {
        var stop = new StopSignal();
        var pipeline = new Pipeline<>(settings.consumer(), new EnvelopeReader(EnvelopeMode.AUTO),
                (m, t) -> m.payloadAsString(), String.class, handler, policy, stop, () -> 0.0);
        InboxConsumer consumer = new InboxConsumer(settings, pipeline, destinations, stop, Clock.systemUTC(), () -> 0.0);
        consumers.add(consumer);
        return consumer;
    }

    static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met in 10 s");
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** Receiver that forwards everything; tests override one method. */
    static class Delegating implements MessageReceiver {
        private final MessageReceiver delegate;

        Delegating(MessageReceiver delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
            return delegate.receive(maxMessages, maxWait);
        }

        @Override
        public void ack(ReceivedMessage message) {
            delegate.ack(message);
        }

        @Override
        public void nack(ReceivedMessage message, Duration redeliverAfter) {
            delegate.nack(message, redeliverAfter);
        }

        @Override
        public void extendLease(ReceivedMessage message, Duration lease) {
            delegate.extendLease(message, lease);
        }

        @Override
        public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
            return delegate.keepAlive(message, maxTotal);
        }

        @Override
        public void deadLetter(ReceivedMessage message, String reason) {
            delegate.deadLetter(message, reason);
        }

        @Override
        public void checkAccess() {
            delegate.checkAccess();
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

}
