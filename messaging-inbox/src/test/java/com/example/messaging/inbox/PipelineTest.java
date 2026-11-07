package com.example.messaging.inbox;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PipelineTest {

    private static final Instant NOW = Instant.parse("2026-11-05T10:00:00Z");
    private static final String OUTBOX = "{\"messageId\":\"m-1\",\"eventType\":\"E\",\"payload\":\"hi\"}";

    @SkipImmediate
    static class RateLimited extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private final AtomicInteger calls = new AtomicInteger();
    private final List<Duration> sleeps = new ArrayList<>();
    private final List<Delivery> deliveries = new ArrayList<>();

    private Pipeline<String> pipeline(InboxResultHandler<String> handler) {
        var policy = new RetryPolicy(
                new BackoffSpec(3, Duration.ofMillis(100), 2, Duration.ofSeconds(2), 0),
                new BackoffSpec(5, Duration.ofSeconds(30), 2, Duration.ofMinutes(15), 0),
                true, 3, List.of(IllegalArgumentException.class), List.of(), List.of());
        PayloadDeserializer strings = (m, type) -> m.payloadAsString();
        return new Pipeline<>("billing", new EnvelopeReader(EnvelopeMode.AUTO), strings, String.class, message -> {
            calls.incrementAndGet();
            deliveries.add(message.message().delivery());
            return handler.handle(message);
        }, policy, sleeps::add, () -> 0.0);
    }

    @Test
    void voidHandlerSuccessIsAck() {
        var handled = new ArrayList<String>();
        Outcome outcome = pipeline(InboxResultHandler.of(m -> handled.add(m.payload()))).handle(received(OUTBOX, Map.of(), 1), "q", 0);

        assertEquals(new Outcome.Ack(Outcome.AckReason.SUCCESS), outcome);
        assertEquals(List.of("\"hi\""), handled);
    }

    @Test
    void explicitResults() {
        assertEquals(new Outcome.Ack(Outcome.AckReason.DISCARDED),
                pipeline(m -> HandlerResult.discard("not for us")).handle(received(OUTBOX, Map.of(), 1), "q", 0));
        Outcome rejected = pipeline(m -> HandlerResult.reject("bad order")).handle(received(OUTBOX, Map.of(), 1), "q", 0);
        assertEquals(Outcome.FailureReason.REJECTED, assertInstanceOf(Outcome.Failed.class, rejected).reason());
        assertEquals("bad order", ((Outcome.Failed) rejected).detail());
    }

    @Test
    void retryAfterSkipsTheImmediateLevel() {
        Outcome outcome = pipeline(m -> HandlerResult.retryAfter(Duration.ofSeconds(7))).handle(received(OUTBOX, Map.of(), 1), "q", 0);

        var retry = assertInstanceOf(Outcome.Retry.class, outcome);
        assertEquals(Duration.ofSeconds(7), retry.delay());
        assertEquals(1, retry.next().delayed());
        assertEquals(1, calls.get());
        assertTrue(sleeps.isEmpty());
    }

    @Test
    void nonRetryableRunsOnceAndFails() {
        Outcome outcome = pipeline(m -> {
            throw new RuntimeException("wrapper", new IllegalArgumentException("invalid order"));
        }).handle(received(OUTBOX, Map.of(), 1), "q", 0);

        assertEquals(Outcome.FailureReason.NON_RETRYABLE, assertInstanceOf(Outcome.Failed.class, outcome).reason());
        assertEquals(1, calls.get());
    }

    @Test
    void transientFailureSucceedsOnTheSecondExecution() {
        Outcome outcome = pipeline(m -> {
            if (calls.get() == 1) {
                throw new IllegalStateException("timeout");
            }
            return null;
        }).handle(received(OUTBOX, Map.of(), 1), "q", 0);

        assertEquals(new Outcome.Ack(Outcome.AckReason.SUCCESS), outcome);
        assertEquals(2, calls.get());
        assertEquals(List.of(Duration.ofMillis(100)), sleeps);
    }

    @Test
    void immediateLevelExhaustedGoesToTheDelayedLevel() {
        Outcome outcome = pipeline(m -> {
            throw new IllegalStateException("down");
        }).handle(received(OUTBOX, Map.of(), 1), "q", 0);

        var retry = assertInstanceOf(Outcome.Retry.class, outcome);
        assertEquals(Duration.ofSeconds(30), retry.delay());
        assertEquals(new AttemptState(1, 0), retry.next());
        assertEquals(4, calls.get());
    }

    @Test
    void skipImmediateGoesStraightToTheDelayedLevel() {
        Outcome outcome = pipeline(m -> {
            throw new RateLimited();
        }).handle(received(OUTBOX, Map.of(), 1), "q", 0);

        assertInstanceOf(Outcome.Retry.class, outcome);
        assertEquals(1, calls.get());
    }

    @Test
    void delayedAttemptsCountAttributeDeliveryCountAndHold() {
        InboxResultHandler<String> failing = m -> {
            throw new RateLimited();
        };

        Outcome fromAttribute = pipeline(failing).handle(received(OUTBOX, Map.of("inbox_attempts", "l2=5;l3=0"), 1), "q", 0);
        Outcome fromDeliveryCount = pipeline(failing).handle(received(OUTBOX, Map.of(), 6), "q", 0);
        Outcome fromHold = pipeline(failing).handle(received(OUTBOX, Map.of(), 1), "q", 5);
        Outcome combined = pipeline(failing).handle(received(OUTBOX, Map.of("inbox_attempts", "l2=2;l3=0"), 2), "q", 1);

        assertInstanceOf(Outcome.DeadLetter.class, fromAttribute);
        assertInstanceOf(Outcome.DeadLetter.class, fromDeliveryCount);
        assertInstanceOf(Outcome.DeadLetter.class, fromHold);
        assertEquals(new AttemptState(5, 0), assertInstanceOf(Outcome.Retry.class, combined).next());
        assertEquals(new Delivery(Delivery.Level.DELAYED, 4), deliveries.getLast());
    }

    @Test
    void deadLetterPassRetryableGoesBackWithL3Plus1() {
        Outcome outcome = pipeline(m -> {
            throw new IllegalStateException("down");
        }).process(received(OUTBOX, Map.of("inbox_attempts", "l2=5;l3=0"), 1), "q", 0, ConsumerObserver.NONE,
                PassMode.DEAD_LETTER).outcome();

        assertEquals(1, assertInstanceOf(Outcome.DeadLetter.class, outcome).next().deadLetter());
    }

    @Test
    void deadLetterPassExhaustedGoesToFailed() {
        Outcome outcome = pipeline(m -> {
            throw new IllegalStateException("down");
        }).process(received(OUTBOX, Map.of("inbox_attempts", "l2=5;l3=3"), 1), "q", 0, ConsumerObserver.NONE,
                PassMode.DEAD_LETTER).outcome();

        assertEquals(Outcome.FailureReason.RETRIES_EXHAUSTED, assertInstanceOf(Outcome.Failed.class, outcome).reason());
    }

    @Test
    void deadLetterPassIgnoresDeliveryCount() {
        Outcome outcome = pipeline(m -> {
            throw new RateLimited();
        }).process(received(OUTBOX, Map.of("inbox_attempts", "l2=0;l3=0"), 9), "q", 0, ConsumerObserver.NONE,
                PassMode.DEAD_LETTER).outcome();

        assertEquals(1, assertInstanceOf(Outcome.DeadLetter.class, outcome).next().deadLetter());
    }

    @Test
    void deadLetterPassDeliveryLevel() {
        pipeline(m -> null).process(received(OUTBOX, Map.of("inbox_attempts", "l2=5;l3=0"), 1), "q", 0,
                ConsumerObserver.NONE, PassMode.DEAD_LETTER);

        assertEquals(new Delivery(Delivery.Level.DEAD_LETTER, 1), deliveries.getFirst());
    }

    @Test
    void unreadablePayloadFailsWithoutCallingTheHandler() {
        PayloadDeserializer broken = (m, type) -> {
            throw new IllegalStateException("bad json");
        };
        var p = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.OUTBOX), broken, String.class,
                m -> null, RetryPolicy.defaults(), sleeps::add, () -> 0.0);

        Outcome badEnvelope = p.handle(received("not json", Map.of(), 1), "q", 0);
        Outcome badPayload = p.handle(received(OUTBOX, Map.of(), 1), "q", 0);

        assertEquals(Outcome.FailureReason.DESERIALIZATION, assertInstanceOf(Outcome.Failed.class, badEnvelope).reason());
        assertEquals(Outcome.FailureReason.DESERIALIZATION, assertInstanceOf(Outcome.Failed.class, badPayload).reason());
    }

    @Test
    void interruptedWaitReleases() {
        var stop = new StopSignal();
        stop.stop();
        var p = new Pipeline<String>("billing", new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> "x", String.class, m -> {
            throw new IllegalStateException("down");
        }, RetryPolicy.defaults(), stop, () -> 0.0);

        assertEquals(Outcome.RELEASE, p.handle(received(OUTBOX, Map.of(), 1), "q", 0));
    }

    static ReceivedMessage received(String body, Map<String, String> attributes, int deliveryCount) {
        return new ReceivedMessage("native-1", body.getBytes(StandardCharsets.UTF_8), attributes, null, null,
                OptionalInt.of(deliveryCount), NOW, NOW, NOW.plusSeconds(30), null, null, new Object());
    }
}
