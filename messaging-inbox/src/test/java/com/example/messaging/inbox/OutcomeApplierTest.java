package com.example.messaging.inbox;

import com.example.messaging.Capabilities;
import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.SendResult;
import com.example.messaging.inbox.retry.AttemptState;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

/** Effects the in-memory broker cannot produce. */
class OutcomeApplierTest {

    private static final Instant NOW = Instant.parse("2026-11-05T10:00:00Z");

    private final Recording receiver = new Recording();
    private final Collecting dlq = new Collecting();
    private final Collecting failed = new Collecting();

    private OutcomeApplier applier(DelayedStrategy strategy) {
        var settings = ConsumerSettings.defaults("billing", "orders");
        return new OutcomeApplier(settings, strategy, new Destinations(() -> receiver, failed, dlq, List.of()),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void brokerDelayWithoutDeliveryCountSkipsTheDelayedLevel() {
        var applied = applier(DelayedStrategy.BROKER_DELAY).apply(receiver, received(OptionalInt.empty(), null),
                new Outcome.Retry(Duration.ofSeconds(30), new AttemptState(1, 0), new IllegalStateException()));

        assertEquals(Effect.DEAD_LETTERED, applied.effect());
        assertTrue(applied.retrySkipped());
        assertEquals(1, dlq.sent.size());
        assertEquals(List.of("ack"), receiver.calls);
    }

    @Test
    void brokerDelayIsCappedAndARefusedDelayGoesToTheDeadLetterDestination() {
        receiver.maxDelay = Duration.ofMinutes(10);
        var applied = applier(DelayedStrategy.BROKER_DELAY).apply(receiver, received(OptionalInt.of(1), null),
                new Outcome.Retry(Duration.ofHours(1), new AttemptState(1, 0), null));
        assertEquals(Effect.REDELIVERY_DELAYED, applied.effect());
        assertEquals(List.of("nack PT10M"), receiver.calls);

        receiver.calls.clear();
        receiver.refuseDelay = true;
        var refused = applier(DelayedStrategy.BROKER_DELAY).apply(receiver, received(OptionalInt.of(1), null),
                new Outcome.Retry(Duration.ofMinutes(1), new AttemptState(1, 0), null));
        assertEquals(Effect.DEAD_LETTERED, refused.effect());
        assertTrue(refused.retrySkipped());
    }

    @Test
    void aMessageThatCannotBeCopiedIsReleasedNotAcked() {
        var applied = applier(DelayedStrategy.BROKER_DELAY).apply(receiver, received(OptionalInt.of(1), "key with spaces"),
                new Outcome.Failed(Outcome.FailureReason.NON_RETRYABLE, AttemptState.initial(), new IllegalArgumentException(), null));

        assertEquals(Effect.COPY_IMPOSSIBLE, applied.effect());
        assertEquals(List.of("nack PT0S"), receiver.calls);
        assertTrue(failed.sent.isEmpty());
    }

    @Test
    void aFailedCopyReleasesTheMessage() {
        failed.fail = true;
        var applied = applier(DelayedStrategy.BROKER_DELAY).apply(receiver, received(OptionalInt.of(1), null),
                new Outcome.Failed(Outcome.FailureReason.REJECTED, AttemptState.initial(), null, "bad"));

        assertEquals(Effect.EFFECT_FAILED, applied.effect());
        assertEquals(List.of("nack PT0S"), receiver.calls);
    }

    private static ReceivedMessage received(OptionalInt deliveryCount, String orderingKey) {
        return new ReceivedMessage("m-1", "body".getBytes(StandardCharsets.UTF_8), Map.of(), null, orderingKey,
                deliveryCount, NOW, NOW, NOW.plusSeconds(30), null, null, new Object());
    }

    static final class Collecting implements MessageSender {
        final List<OutgoingMessage> sent = new ArrayList<>();
        boolean fail;

        @Override
        public SendResult send(OutgoingMessage message) {
            if (fail) {
                throw new IllegalStateException("broker down");
            }
            sent.add(message);
            return new SendResult("copy-" + sent.size());
        }

        @Override
        public void checkAccess() {
        }

        @Override
        public Capabilities capabilities() {
            return null;
        }

        @Override
        public void close() {
        }
    }

    static final class Recording implements MessageReceiver {
        final List<String> calls = new ArrayList<>();
        Duration maxDelay = Duration.ofHours(12);
        boolean refuseDelay;

        @Override
        public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
            return List.of();
        }

        @Override
        public void ack(ReceivedMessage message) {
            calls.add("ack");
        }

        @Override
        public void nack(ReceivedMessage message, Duration redeliverAfter) {
            if (refuseDelay && !redeliverAfter.isZero()) {
                throw new IllegalArgumentException("past the 12 h limit");
            }
            calls.add("nack " + redeliverAfter);
        }

        @Override
        public void extendLease(ReceivedMessage message, Duration lease) {
        }

        @Override
        public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
            return null;
        }

        @Override
        public void deadLetter(ReceivedMessage message, String reason) {
        }

        @Override
        public void checkAccess() {
        }

        @Override
        public Capabilities capabilities() {
            return new Capabilities(256 * 1024, 10, true, maxDelay, false, false, false, true);
        }

        @Override
        public void close() {
        }
    }
}
