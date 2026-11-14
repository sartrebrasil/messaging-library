package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.Delivery;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.Outcome;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The timers Micrometer builds from the {@code inbox.consume} observation (M11): their names and tags, and that the
 * span keeps the OpenTelemetry attributes while the timers use the tags of the {@code inbox.*} meters (N2).
 */
class ObservationMetersTest {

    private static final InboxMessage INBOX = new InboxMessage("m-1", null, null, null, null, null, null, null,
            Map.of(), new byte[0], "billing-orders", Delivery.first());

    static final List<String> EXPECTED = List.of(
            // `error` is the meter handler's own tag (simple class name, or none); the inbox meters call it `exception`
            "inbox.consume TIMER [consumer, destination, error, level, outcome]",
            // `outcome` is only known at the end, so on the active timer it is always `released`
            "inbox.consume.active LONG_TASK_TIMER [consumer, destination, level, outcome]");

    @Test
    void theTimersUseTheTagsOfTheInboxMeters() {
        var meters = new SimpleMeterRegistry();
        var observations = ObservationRegistry.create();
        observations.observationConfig().observationHandler(new DefaultMeterObservationHandler(meters));
        var observer = new MicrometerConsumerObserver("billing-orders", "billing-orders", "aws", null, observations,
                Clock.systemUTC(), false);

        observer.execution(INBOX, () -> new Outcome.Ack(Outcome.AckReason.SUCCESS));
        assertThatThrownBy(() -> observer.execution(INBOX, () -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(meters.getMeters().stream().map(ObservationMetersTest::describe).distinct().sorted().toList())
                .isEqualTo(EXPECTED);
        assertThat(meters.get("inbox.consume").tags("outcome", "success", "error", "none").timer().count()).isEqualTo(1);
        assertThat(meters.get("inbox.consume").tags("outcome", "released", "error", "IllegalStateException").timer()
                .count()).isEqualTo(1);
    }

    @Test
    void theSpanKeepsTheOpenTelemetryAttributes() {
        Map<String, String> span = new HashMap<>();
        var observations = ObservationRegistry.create();
        observations.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
            @Override
            public boolean supportsContext(Observation.Context context) {
                return true;
            }

            @Override
            public void onStop(Observation.Context context) {
                context.getLowCardinalityKeyValues().forEach(kv -> span.put(kv.getKey(), kv.getValue()));
                context.getHighCardinalityKeyValues().forEach(kv -> span.put(kv.getKey(), kv.getValue()));
            }
        });
        new MicrometerConsumerObserver("billing-orders", "billing-orders", "aws", null, observations,
                Clock.systemUTC(), false).execution(INBOX, () -> new Outcome.Ack(Outcome.AckReason.SUCCESS));

        assertThat(span).containsEntry("messaging.system", "aws")
                .containsEntry("messaging.destination.name", "billing-orders")
                .containsEntry("messaging.consumer.group.name", "billing-orders")
                .containsEntry("messaging.message.id", "m-1")
                .containsEntry("inbox.attempt", "0")
                .containsEntry("outcome", "success").containsEntry("level", "immediate");
    }

    private static String describe(Meter meter) {
        return meter.getId().getName() + " " + meter.getId().getType() + " "
                + meter.getId().getTags().stream().map(t -> t.getKey()).sorted().toList();
    }
}
