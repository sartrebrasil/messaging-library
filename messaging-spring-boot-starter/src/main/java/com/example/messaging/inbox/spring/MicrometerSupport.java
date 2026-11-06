package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.ConsumerObserver;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ListableBeanFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/** Micrometer wiring, kept apart because Micrometer is optional. */
final class MicrometerSupport {

    private MicrometerSupport() {
    }

    static ConsumerObserver observer(InboxConfig.Consumer c, ListableBeanFactory beans, Clock clock,
                                     Supplier<Instant> lastReceive) {
        MeterRegistry meters = beans.getBeanProvider(MeterRegistry.class).getIfAvailable();
        ObservationRegistry observations = beans.getBeanProvider(ObservationRegistry.class).getIfAvailable();
        if (meters == null && observations == null) {
            return ConsumerObserver.NONE;
        }
        if (meters != null) {
            // strong holder: the supplier reads whichever consumer is current, across restarts (ADR-0007, D7)
            Gauge.builder("inbox.last.receive.age", lastReceive, s -> {
                        Instant last = s.get();
                        return last == null ? Double.NaN : Duration.between(last, clock.instant()).toMillis() / 1000.0;
                    })
                    .strongReference(true)
                    .baseUnit("seconds")
                    .description("seconds since the consumer last finished a receive, empty or not")
                    .tag("consumer", c.name())
                    .register(meters);
        }
        return new MicrometerConsumerObserver(c.name(), c.name(), meters, observations, clock);
    }
}
