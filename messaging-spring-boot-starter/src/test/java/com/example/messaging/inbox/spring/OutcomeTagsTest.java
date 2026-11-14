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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

/** What the {@code outcome}, {@code level} and {@code effect} tags say in the cases that used to say something else. */
class OutcomeTagsTest {

    private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");
    private static final ReceivedMessage RECEIVED = new ReceivedMessage("m-1", new byte[0], Map.of(), null, null,
            OptionalInt.of(1), null, NOW, NOW.plusSeconds(30), null, null, null);
    private static final InboxMessage INBOX = new InboxMessage("m-1", null, null, null, null, null, null, null,
            Map.of(), new byte[0], "billing-orders", Delivery.first());
    private static final RuntimeException FAILURE = new IllegalStateException("boom");

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private MicrometerConsumerObserver observer(boolean pass) {
        return new MicrometerConsumerObserver("billing-orders", pass ? "billing-orders.dead-letter" : "billing-orders",
                "aws", registry, null, Clock.fixed(NOW, ZoneOffset.UTC), pass);
    }

    private static Report report(Outcome outcome, Effect effect, BrokerCall failedCall) {
        return new Report(RECEIVED, INBOX, outcome, effect, DelayedStrategy.BROKER_DELAY, false, false,
                Duration.ofMillis(5), failedCall);
    }

    private double processed(String outcome) {
        var counter = registry.find("inbox.messages.processed").tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    @Test
    void anExpiredLeaseOnTheAckIsNotASuccess() {
        observer(false).delivered(report(new Outcome.Ack(Outcome.AckReason.SUCCESS), Effect.ACK_LEASE_EXPIRED, null));

        assertThat(processed("lease_expired")).isEqualTo(1);
        assertThat(processed("success")).isZero();
    }

    @Test
    void aFailureInALevelThreePassIsARetryNotANewEntryIntoTheDlq() {
        observer(true).delivered(report(new Outcome.DeadLetter(AttemptState.initial(), FAILURE), Effect.DEAD_LETTERED,
                null));

        assertThat(processed("retry_dead_letter")).isEqualTo(1);
        assertThat(registry.find("inbox.messages.dead.lettered").counter()).isNull();
        assertThat(registry.get("inbox.messages.retried").tags("level", "dead_letter", "strategy", "none",
                "exception", "IllegalStateException").counter().count()).isEqualTo(1);
    }

    @Test
    void outsideAPassTheDeadLetterStillCountsAsAnEntry() {
        observer(false).delivered(report(new Outcome.DeadLetter(AttemptState.initial(), FAILURE), Effect.DEAD_LETTERED,
                null));

        assertThat(processed("dead_letter")).isEqualTo(1);
        assertThat(registry.get("inbox.messages.dead.lettered").counter().count()).isEqualTo(1);
        assertThat(registry.find("inbox.messages.retried").counter()).isNull();
    }

    @Test
    void effectFailedNamesTheCallThatFailed() {
        // a copy that went out and an ack that failed: the outcome (a copy to failed) must not decide the tag
        observer(false).delivered(report(new Outcome.Failed(Outcome.FailureReason.REJECTED, AttemptState.initial(),
                null, "no"), Effect.EFFECT_FAILED, BrokerCall.ACK));
        observer(false).delivered(report(new Outcome.Retry(Duration.ofSeconds(1), AttemptState.initial(), FAILURE),
                Effect.EFFECT_FAILED, BrokerCall.NACK));
        observer(false).delivered(report(new Outcome.DeadLetter(AttemptState.initial(), FAILURE),
                Effect.COPY_IMPOSSIBLE, BrokerCall.COPY));

        for (String call : new String[] {"ack", "nack", "copy"}) {
            assertThat(registry.get("inbox.effects.failed").tag("effect", call).counter().count())
                    .as(call).isEqualTo(1);
        }
    }

    @Test
    void aRedriveRunRecordsHowItEnded() {
        var redrive = new MeteredInboxRedrive(request -> new RedriveReport("r-1", PassEnd.RECEIVE_FAILED, 3, 0, 1,
                Map.of(), Map.of()), registry);

        redrive.redrive(new RedriveRequest("billing-orders", Source.FAILED, Target.MAIN, 10, Filter.NONE, 100, false));

        assertThat(registry.get("inbox.redrives.ended").tags("end", "receive_failed", "source", "failed",
                "target", "main").counter().count()).isEqualTo(1);
    }
}
