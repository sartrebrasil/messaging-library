package com.example.messaging.inbox;

import com.example.messaging.MessageReceiver;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Level 3 (ADR-0002): one pass over the DLQ, run synchronously. */
class DeadLetterPassTest {

    private static final String MAIN = "orders";
    private static final String DLQ = "orders-dlq";
    private static final String FAILED = "orders-failed";
    private static final String OUTBOX = "{\"messageId\":\"m-1\",\"eventType\":\"E\",\"payload\":\"hi\"}";

    private final InMemoryMessaging messaging = new InMemoryMessaging();
    private final AtomicInteger calls = new AtomicInteger();
    private final StopSignal stop = new StopSignal();

    DeadLetterPassTest() {
        messaging.createQueue(MAIN, QueueOptions.defaults());
        messaging.createQueue(DLQ, QueueOptions.defaults().withLease(Duration.ofSeconds(5)));
        messaging.createQueue(FAILED, QueueOptions.defaults());
    }

    private static ConsumerSettings settings() {
        return new ConsumerSettings("billing", MAIN, 1, 1, Duration.ofMillis(50), Duration.ofSeconds(5),
                Duration.ofSeconds(5), Ordering.RELAXED, DelayedStrategy.BROKER_DELAY, true);
    }

    private static RetryPolicy policy() {
        return new RetryPolicy(BackoffSpec.none(), new BackoffSpec(5, Duration.ofSeconds(1), 1.0, Duration.ofSeconds(1), 0),
                true, 3, List.of(IllegalArgumentException.class), List.of(), List.of());
    }

    private DeadLetterPass pass(InboxResultHandler<String> handler, MessageReceiver dlqReceiver, int maxPerPass,
                                InboxIdempotency idempotency) {
        ConsumerSettings settings = settings();
        Pipeline<String> pipeline = new Pipeline<>("billing", new EnvelopeReader(EnvelopeMode.AUTO),
                (m, t) -> m.payloadAsString(), String.class, m -> {
                    calls.incrementAndGet();
                    return handler.handle(m);
                }, policy(), stop, () -> 0.0, idempotency);
        Destinations destinations = new Destinations(() -> messaging.receiver(MAIN), messaging.queueSender(FAILED),
                messaging.queueSender(DLQ), List.of(), () -> dlqReceiver);
        OutcomeApplier applier = new OutcomeApplier(settings, DelayedStrategy.BROKER_DELAY, destinations,
                Clock.systemUTC());
        return new DeadLetterPass(settings, pipeline, applier, destinations, stop, Clock.systemUTC(),
                ConsumerObserver.NONE, maxPerPass);
    }

    private DeadLetterPass pass(InboxResultHandler<String> handler) {
        return pass(handler, messaging.receiver(DLQ), 100, null);
    }

    private void park(String body, int l3) {
        messaging.queueSender(DLQ).send(OutgoingMessage.ofText(body).withAttribute("inbox_attempts", "l2=5;l3=" + l3));
    }

    private ReceivedMessage only(String queue) {
        List<ReceivedMessage> all = messaging.receiver(queue).receive(10, Duration.ZERO);
        assertEquals(1, all.size(), queue);
        return all.getFirst();
    }

    @Test
    void successIsAcked() {
        park(OUTBOX, 0);

        assertEquals(PassEnd.EMPTY, pass(m -> HandlerResult.ack()).runPass());

        assertEquals(1, calls.get());
        assertEquals(0, messaging.size(DLQ));
        assertEquals(0, messaging.size(FAILED));
    }

    @Test
    void retryableGoesToTheEndWithL3Plus1() {
        park(OUTBOX, 0);

        assertEquals(PassEnd.NEWER_THAN_PASS, pass(m -> {
            throw new IllegalStateException("down");
        }).runPass());

        assertEquals(1, calls.get());
        assertEquals("l2=5;l3=1", only(DLQ).attributes().get("inbox_attempts"));
    }

    @Test
    void exhaustedGoesToFailed() {
        park(OUTBOX, 3);

        pass(m -> {
            throw new IllegalStateException("down");
        }).runPass();

        assertEquals(0, messaging.size(DLQ));
        assertTrue(only(FAILED).deadLetter().reason().contains("down"));
    }

    @Test
    void nonRetryableGoesToFailed() {
        park(OUTBOX, 0);

        pass(m -> {
            throw new IllegalArgumentException("invalid");
        }).runPass();

        assertEquals(0, messaging.size(DLQ));
        assertEquals("java.lang.IllegalArgumentException: invalid", only(FAILED).deadLetter().reason());
    }

    @Test
    void inProgressGoesToTheEndWithoutSpendingL3() {
        var store = new com.example.idempotency.store.InMemoryIdempotencyStore();
        var key = new com.example.idempotency.model.IdempotencyKeyId("m-1", "billing");
        store.reserve(com.example.idempotency.model.IdempotencyRecord.inProgress(key, "other", null, Instant.now(),
                Instant.now().plusSeconds(60)));
        park(OUTBOX, 0);

        pass(m -> HandlerResult.ack(), messaging.receiver(DLQ), 100,
                new InboxIdempotency(store, Duration.ofMinutes(1), Duration.ofDays(7), Clock.systemUTC())).runPass();

        assertEquals(0, calls.get());
        assertEquals("l2=5;l3=0", only(DLQ).attributes().get("inbox_attempts"));
    }

    @Test
    void repeatedKeyEndsThePassWithoutEnqueuedAt() {
        park(OUTBOX, 0);
        park(OUTBOX, 0);
        MessageReceiver noEnqueuedAt = new InboxConsumerTest.Delegating(messaging.receiver(DLQ)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                return super.receive(maxMessages, maxWait).stream().map(m -> new ReceivedMessage(m.messageId(),
                        m.body(), m.attributes(), m.contentType(), m.orderingKey(), m.deliveryCount(), null,
                        m.receivedAt(), m.leaseExpiresAt(), m.traceparent(), m.deadLetter(), m.handle())).toList();
            }
        };

        assertEquals(PassEnd.REPEATED_KEY, pass(m -> {
            throw new IllegalStateException("down");
        }, noEnqueuedAt, 100, null).runPass());

        assertEquals(1, calls.get());
    }

    @Test
    void maxPerPassEndsThePass() {
        for (int i = 0; i < 5; i++) {
            park("{\"messageId\":\"m-" + i + "\",\"eventType\":\"E\",\"payload\":\"hi\"}", 0);
        }

        assertEquals(PassEnd.MAX_PER_PASS, pass(m -> HandlerResult.ack(), messaging.receiver(DLQ), 2, null).runPass());

        assertEquals(2, calls.get());
        assertEquals(3, messaging.size(DLQ));
    }

    @Test
    void stopEndsThePass() {
        park(OUTBOX, 0);
        stop.stop();

        assertEquals(PassEnd.STOPPING, pass(m -> HandlerResult.ack()).runPass());

        assertEquals(0, calls.get());
        assertEquals(1, messaging.size(DLQ));
    }

    @Test
    void receiveFailureEndsThePass() {
        MessageReceiver broken = new InboxConsumerTest.Delegating(messaging.receiver(DLQ)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                throw new IllegalStateException("broker down");
            }
        };

        assertEquals(PassEnd.RECEIVE_FAILED, pass(m -> HandlerResult.ack(), broken, 100, null).runPass());
    }
}
