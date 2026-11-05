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
