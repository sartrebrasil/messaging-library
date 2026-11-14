package com.example.messaging.inbox.spring;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.ConsumerObserver.Report;
import com.example.messaging.inbox.ConsumerObserver.Stage;
import com.example.messaging.inbox.ConsumerObserver.StoreCall;
import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Delivery;
import com.example.messaging.inbox.Effect;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.Outcome;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

/** What the diagnostic meters count, and when. */
class DiagnosticMetersTest {

    private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");
    private static final ReceivedMessage RECEIVED = new ReceivedMessage("m-1", new byte[0], Map.of(), null, null,
            OptionalInt.of(1), null, NOW, NOW.plusSeconds(30), null, null, null);
    private static final RuntimeException FAILURE = new IllegalStateException("boom");

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private MicrometerConsumerObserver observer(MeterRegistry meters) {
        return new MicrometerConsumerObserver("billing-orders", "billing-orders", "aws", meters, null,
                Clock.fixed(NOW, ZoneOffset.UTC), false);
    }

    private static InboxMessage inbox(Delivery delivery) {
        return new InboxMessage("m-1", null, null, null, null, null, null, null, Map.of(), new byte[0],
                "billing-orders", delivery);
    }

    private static Report report(InboxMessage inbox) {
        return new Report(RECEIVED, inbox, new Outcome.Ack(Outcome.AckReason.SUCCESS), Effect.ACKED,
                DelayedStrategy.BROKER_DELAY, false, false, Duration.ofMillis(5), null);
    }

    private double inFlight() {
        return registry.get("inbox.messages.in.flight").gauge().value();
    }

    @Test
    void inFlightIsReceivedMinusDelivered() {
        var observer = observer(registry);
        observer.received(RECEIVED);
        observer.received(RECEIVED);
        assertThat(inFlight()).isEqualTo(2);

        observer.delivered(report(inbox(Delivery.first())));
        assertThat(inFlight()).isEqualTo(1);
    }

    @Test
    void aRestartedConsumerReplacesTheGaugeOfTheOneBefore() {
        observer(registry).received(RECEIVED);
        assertThat(inFlight()).isEqualTo(1);

        var restarted = observer(registry); // the consumer is built again on each start, with a fresh counter
        assertThat(inFlight()).isZero();
        restarted.received(RECEIVED);
        assertThat(inFlight()).isEqualTo(1);
        assertThat(registry.find("inbox.messages.in.flight").gauges()).hasSize(1);
    }

    @Test
    void failuresAreCountedByStageAndByWhatFailed() {
        var observer = observer(registry);
        observer.keepAliveFailed(RECEIVED, FAILURE);
        observer.unexpectedFailure(Stage.LANE, new AssertionError("bug"));
        observer.unexpectedFailure(Stage.DEAD_LETTER_PASS, FAILURE);

        assertThat(registry.get("inbox.keepalives.failed").counter().count()).isEqualTo(1);
        assertThat(registry.get("inbox.failures.unexpected").tags("stage", "lane", "exception", "AssertionError")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("inbox.failures.unexpected").tags("stage", "dead_letter_pass", "exception",
                "IllegalStateException").counter().count()).isEqualTo(1);
    }

    @Test
    void handlerAndStoreCallsAreTimedWithTheirOutcome() {
        var observer = observer(registry);
        var message = inbox(Delivery.first());
        observer.handled(message, Duration.ofMillis(3), null);
        observer.handled(message, Duration.ofMillis(7), FAILURE);
        observer.idempotency(message, StoreCall.COMPLETE, Duration.ofMillis(1), null);
        observer.idempotency(message, StoreCall.RESERVE, Duration.ofMillis(1), FAILURE);

        assertThat(registry.get("inbox.handler.duration").tags("outcome", "success").timer().count()).isEqualTo(1);
        assertThat(registry.get("inbox.handler.duration").tags("outcome", "error").timer().count()).isEqualTo(1);
        assertThat(registry.get("inbox.idempotency.duration").tags("operation", "complete", "outcome", "success")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("inbox.idempotency.duration").tags("operation", "reserve", "outcome", "error")
                .timer().count()).isEqualTo(1);
    }

    @Test
    void attemptsAreRecordedByLevelAndHoldRecordsTheWait() {
        var observer = observer(registry);
        observer.received(RECEIVED);
        observer.delivered(report(inbox(new Delivery(Delivery.Level.DELAYED, 3))));
        observer.held(inbox(Delivery.first()), new Outcome.Retry(Duration.ofSeconds(2),
                com.example.messaging.inbox.retry.AttemptState.initial(), FAILURE));

        var attempts = registry.get("inbox.delivery.attempts").tags("level", "delayed").summary();
        assertThat(attempts.count()).isEqualTo(1);
        assertThat(attempts.totalAmount()).isEqualTo(3);
        assertThat(registry.get("inbox.hold.wait.duration").timer().totalTime(java.util.concurrent.TimeUnit.SECONDS))
                .isEqualTo(2);
    }
}
