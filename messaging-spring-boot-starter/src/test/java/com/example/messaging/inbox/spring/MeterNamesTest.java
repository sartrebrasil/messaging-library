package com.example.messaging.inbox.spring;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.ConsumerObserver.BrokerCall;
import com.example.messaging.inbox.ConsumerObserver.Report;
import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Delivery;
import com.example.messaging.inbox.Effect;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.Outcome;
import com.example.messaging.inbox.PassEnd;
import com.example.messaging.inbox.redrive.Filter;
import com.example.messaging.inbox.redrive.RedriveReport;
import com.example.messaging.inbox.redrive.RedriveRequest;
import com.example.messaging.inbox.redrive.Source;
import com.example.messaging.inbox.redrive.Target;
import com.example.messaging.inbox.retry.AttemptState;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the names, types, tag keys and units of the {@code inbox.*} meters (ADR-0007, "Convenção de nomes"): renaming
 * one means changing this list and the ADR together. {@code inbox.last.receive.age} needs the whole consumer and is
 * covered by {@link ObservabilityTest}.
 */
class MeterNamesTest {

    private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");

    static final List<String> EXPECTED = List.of(
            "inbox.attributes.budget.exceeded COUNTER [consumer, destination]",
            "inbox.dead.letter.passes COUNTER [consumer, end]",
            "inbox.delivery.attempts DISTRIBUTION_SUMMARY [consumer, destination, level]",
            "inbox.effects.failed COUNTER [consumer, destination, effect]",
            "inbox.failures.unexpected COUNTER [consumer, destination, exception, stage]",
            "inbox.handler.duration TIMER [consumer, destination, outcome]",
            "inbox.hold.wait.duration TIMER [consumer, destination]",
            "inbox.idempotency.duration TIMER [consumer, destination, operation, outcome]",
            "inbox.keepalives.failed COUNTER [consumer, destination]",
            "inbox.message.age TIMER [consumer, destination]",
            "inbox.messages.dead.lettered COUNTER [consumer, destination, exception]",
            "inbox.messages.failed COUNTER [consumer, destination, exception, reason]",
            "inbox.messages.in.flight GAUGE [consumer, destination]",
            "inbox.messages.processed COUNTER [consumer, destination, outcome]",
            "inbox.messages.received COUNTER [consumer, destination]",
            "inbox.messages.redriven COUNTER [consumer, result, source, target]",
            "inbox.messages.retried COUNTER [consumer, destination, exception, level, strategy]",
            "inbox.processing.duration TIMER [consumer, destination, outcome]",
            "inbox.redrive.duration TIMER [consumer, source, target]",
            "inbox.redrives.ended COUNTER [consumer, end, source, target]",
            "inbox.retries.skipped COUNTER [consumer, destination, reason]");

    @Test
    void inboxMetersKeepTheirNamesTagsAndTypes() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        emitEveryMeter(registry);

        List<String> actual = registry.getMeters().stream().map(MeterNamesTest::describe).distinct().sorted().toList();

        assertThat(actual).isEqualTo(EXPECTED);
        assertThat(registry.getMeters()).allSatisfy(m -> assertThat(m.getId().getDescription())
                .as("description of %s", m.getId().getName()).isNotBlank());
    }

    /** The receiver is created with the consumer name, which is what {@code messaging.*} meters use as destination. */
    @Test
    void destinationIsTheConsumerName() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        emitEveryMeter(registry);

        assertThat(registry.getMeters()).filteredOn(m -> m.getId().getTag("destination") != null)
                .allSatisfy(m -> assertThat(m.getId().getTag("destination")).isEqualTo("billing-orders"));
    }

    private static void emitEveryMeter(SimpleMeterRegistry registry) {
        MicrometerConsumerObserver observer = new MicrometerConsumerObserver("billing-orders", "billing-orders", "aws",
                registry, null, Clock.fixed(NOW, ZoneOffset.UTC), false);
        ReceivedMessage received = new ReceivedMessage("m-1", new byte[0], Map.of(), null, null, OptionalInt.empty(),
                null, NOW, NOW.plusSeconds(30), null, null, null);
        InboxMessage inbox = new InboxMessage("m-1", null, null, NOW.minusSeconds(10), null, null, null, null, Map.of(),
                new byte[0], "billing-orders", Delivery.first());
        RuntimeException failure = new IllegalStateException("boom");
        AttemptState state = AttemptState.initial();
        Duration took = Duration.ofMillis(5);

        observer.received(received);
        observer.immediateRetry(inbox, failure);
        observer.held(inbox, new Outcome.Retry(Duration.ofSeconds(1), state, failure));
        observer.receiveFailed(failure);
        observer.keepAliveFailed(received, failure);
        observer.unexpectedFailure(com.example.messaging.inbox.ConsumerObserver.Stage.DELIVERY, failure);
        observer.handled(inbox, took, null);
        observer.idempotency(inbox, com.example.messaging.inbox.ConsumerObserver.StoreCall.RESERVE, took, null);
        observer.passEnded(PassEnd.EMPTY, 0);
        // retrySkipped and budgetExceeded both true; inbox.time() set, so the age is recorded
        observer.delivered(new Report(received, inbox, new Outcome.Ack(Outcome.AckReason.SUCCESS), Effect.ACKED,
                DelayedStrategy.BROKER_DELAY, true, true, took, null));
        observer.delivered(new Report(received, inbox, new Outcome.Retry(Duration.ofSeconds(1), state, failure),
                Effect.REDELIVERY_DELAYED, DelayedStrategy.BROKER_DELAY, false, false, took, null));
        observer.delivered(new Report(received, inbox, new Outcome.DeadLetter(state, failure), Effect.DEAD_LETTERED,
                DelayedStrategy.BROKER_DELAY, false, false, took, null));
        observer.delivered(new Report(received, inbox,
                new Outcome.Failed(Outcome.FailureReason.REJECTED, state, null, "no"), Effect.FAILED,
                DelayedStrategy.BROKER_DELAY, false, false, took, null));
        observer.delivered(new Report(received, inbox, Outcome.RELEASE, Effect.EFFECT_FAILED,
                DelayedStrategy.BROKER_DELAY, false, false, took, BrokerCall.NACK));

        new MeteredInboxRedrive(request -> new RedriveReport("r-1", PassEnd.EMPTY, 1, 0, 0, Map.of(), Map.of()),
                registry).redrive(new RedriveRequest("billing-orders", Source.FAILED, Target.MAIN, 10, Filter.NONE, 100,
                false));
    }

    private static String describe(Meter meter) {
        return meter.getId().getName() + " " + meter.getId().getType() + " "
                + meter.getId().getTags().stream().map(t -> t.getKey()).sorted().toList();
    }
}
