package com.example.messaging.spring;

import com.example.messaging.AckResult;
import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.SendResult;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Guards the names, types, tag keys and descriptions of the {@code messaging.*} meters (ADR-0008). */
class MeterNamesTest {

    private static final String COMMON = "[destination, exception, outcome, provider]";
    private static final String BATCHED = "[batch, destination, exception, outcome, provider]";

    static final List<String> EXPECTED = List.of(
            "messaging.ack.duration TIMER " + BATCHED,
            "messaging.dead.letter.duration TIMER " + COMMON,
            "messaging.message.size DISTRIBUTION_SUMMARY [destination, provider]",
            "messaging.messages.failed COUNTER [destination, exception, operation, provider]",
            "messaging.nack.duration TIMER " + COMMON,
            "messaging.receive.duration TIMER " + COMMON,
            "messaging.receive.messages DISTRIBUTION_SUMMARY [destination, provider]",
            "messaging.send.duration TIMER " + BATCHED);

    @Test
    void transportMetersKeepTheirNamesTagsAndTypes() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MessageReceiver receiver = new MeteredMessageReceiver(new StubReceiver(), "aws", "pedidos", registry);
        ReceivedMessage message = receiver.receive(1, Duration.ZERO).getFirst();
        receiver.ack(message);
        receiver.ackAll(List.of(message));
        receiver.nack(message, Duration.ZERO);
        receiver.deadLetter(message, "why");
        MessageSender sender = new ObservedMessageSender(new StubSender(), "aws", "pedidos", registry, null);
        sender.send(OutgoingMessage.ofText("x"));
        sender.sendAll(List.of(OutgoingMessage.ofText("x")));
        // a failure, for messaging.messages.failed
        try {
            new ObservedMessageSender(new StubSender() {
                @Override
                public SendResult send(OutgoingMessage message) {
                    throw new MessagingException("aws", "down", null, true);
                }
            }, "aws", "pedidos", registry, null).send(OutgoingMessage.ofText("x"));
        } catch (MessagingException expected) {
            // counted
        }

        List<String> actual = registry.getMeters().stream().map(MeterNamesTest::describe).distinct().sorted().toList();

        assertThat(actual).isEqualTo(EXPECTED);
        assertThat(registry.getMeters()).allSatisfy(m -> assertThat(m.getId().getDescription())
                .as("description of %s", m.getId().getName()).isNotBlank());
        // same destination as the inbox meters of that receiver, which are tagged with the receiver name
        assertThat(registry.getMeters()).allSatisfy(m -> assertThat(m.getId().getTag("destination")).isEqualTo("pedidos"));
    }

    /** The {@code messaging.publish} observation: the timer takes the tags of {@code messaging.*}, the span keeps OTel. */
    @Test
    void thePublishObservationUsesTheTagsOfTheTransportMeters() {
        var meters = new SimpleMeterRegistry();
        var observations = io.micrometer.observation.ObservationRegistry.create();
        observations.observationConfig().observationHandler(
                new io.micrometer.core.instrument.observation.DefaultMeterObservationHandler(meters));
        new ObservedMessageSender(new StubSender(), "aws", "pedidos", null, observations).send(OutgoingMessage.ofText("x"));

        assertThat(meters.getMeters().stream().map(MeterNamesTest::describe).distinct().sorted().toList()).containsExactly(
                "messaging.publish TIMER [destination, error, provider]",
                "messaging.publish.active LONG_TASK_TIMER [destination, provider]");
    }

    @Test
    void theExceptionTagNamesWhatTheCallThrewAndIsNoneOtherwise() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MessageSender sender = new ObservedMessageSender(new StubSender() {
            @Override
            public SendResult send(OutgoingMessage message) {
                throw new MessagingException("aws", "throttled", null, true);
            }
        }, "aws", "pedidos", registry, null);
        new ObservedMessageSender(new StubSender(), "aws", "pedidos", registry, null).send(OutgoingMessage.ofText("x"));
        try {
            sender.send(OutgoingMessage.ofText("x"));
        } catch (MessagingException expected) {
            // counted below
        }

        assertThat(registry.get("messaging.send.duration").tags("outcome", "success", "exception", "none").timer()
                .count()).isEqualTo(1);
        assertThat(registry.get("messaging.send.duration").tags("outcome", "error", "exception",
                "MessagingException").timer().count()).isEqualTo(1);
    }

    @Test
    void aBatchWhereSomeItemsFailedIsPartialAndWhereAllFailedIsAnError() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        var failure = new MessagingException("aws", "throttled", null, true);
        MessageSender sender = new ObservedMessageSender(new StubSender() {
            @Override
            public BatchSendResult sendAll(List<OutgoingMessage> messages) {
                return new BatchSendResult(Map.of(0, new SendResult("id")), Map.of(1, failure));
            }
        }, "aws", "pedidos", registry, null);
        sender.sendAll(List.of(OutgoingMessage.ofText("a"), OutgoingMessage.ofText("b")));

        MessageReceiver receiver = new MeteredMessageReceiver(new StubReceiver() {
            @Override
            public AckResult ackAll(List<ReceivedMessage> messages) {
                return new AckResult(Map.of("m-1", failure));
            }
        }, "aws", "pedidos", registry);
        receiver.ackAll(receiver.receive(1, Duration.ZERO));

        assertThat(registry.get("messaging.send.duration").tags("batch", "true", "outcome", "partial").timer().count())
                .isEqualTo(1);
        assertThat(registry.get("messaging.ack.duration").tags("batch", "true", "outcome", "error").timer().count())
                .isEqualTo(1);
        assertThat(registry.find("messaging.ack.duration").tags("batch", "true", "outcome", "success").timer()).isNull();
    }

    private static String describe(Meter meter) {
        return meter.getId().getName() + " " + meter.getId().getType() + " "
                + meter.getId().getTags().stream().map(t -> t.getKey()).sorted().toList();
    }

    private static class StubReceiver implements MessageReceiver {
        @Override
        public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
            var now = java.time.Instant.now();
            return List.of(new ReceivedMessage("m-1", new byte[0], null, null, null, null, null, now,
                    now.plusSeconds(30), null, null, null));
        }

        @Override
        public void ack(ReceivedMessage message) {
        }

        @Override
        public void nack(ReceivedMessage message, Duration redeliverAfter) {
        }

        @Override
        public void extendLease(ReceivedMessage message, Duration lease) {
        }

        @Override
        public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deadLetter(ReceivedMessage message, String reason) {
        }

        @Override
        public void checkAccess() {
        }

        @Override
        public Capabilities capabilities() {
            return new Capabilities(1, 1, false, Duration.ZERO, false, false, false, false);
        }

        @Override
        public void close() {
        }
    }

    private static class StubSender implements MessageSender {
        @Override
        public SendResult send(OutgoingMessage message) {
            return new SendResult("id");
        }

        @Override
        public void checkAccess() {
        }

        @Override
        public Capabilities capabilities() {
            return new Capabilities(1, 1, false, Duration.ZERO, false, false, false, false);
        }

        @Override
        public void close() {
        }
    }
}
