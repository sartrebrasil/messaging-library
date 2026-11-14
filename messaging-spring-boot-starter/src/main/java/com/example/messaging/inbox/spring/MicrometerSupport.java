package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.redrive.InboxRedrive;
import com.example.messaging.spring.MessagingProperties;
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
        return new MicrometerConsumerObserver(c.name(), c.name(), system(c, beans), meters, observations, clock, false);
    }

    /** The level-3 passes of {@code c} (ADR-0002): its metrics under {@code <consumer>.dead-letter}, no liveness gauge. */
    static ConsumerObserver passObserver(InboxConfig.Consumer c, ListableBeanFactory beans, Clock clock) {
        MeterRegistry meters = beans.getBeanProvider(MeterRegistry.class).getIfAvailable();
        ObservationRegistry observations = beans.getBeanProvider(ObservationRegistry.class).getIfAvailable();
        if (meters == null && observations == null) {
            return ConsumerObserver.NONE;
        }
        // same consumer as the main loop; the destination (the name of the DLQ receiver in the transport meters) tells
        // the pass apart
        return new MicrometerConsumerObserver(c.name(), c.name() + ".dead-letter", system(c, beans), meters,
                observations, clock, true);
    }

    private static String system(InboxConfig.Consumer c, ListableBeanFactory beans) {
        return system(c.provider(), beans.getBeanProvider(MessagingProperties.class).getIfAvailable());
    }

    /**
     * The type of the receiver's provider, as the transport meters tag it. {@code provider} {@code null} means the
     * only one. {@code "messaging"} when it cannot be told (a custom {@code InboxDestinations} without properties).
     */
    static String system(String provider, MessagingProperties properties) {
        if (properties == null) {
            return "messaging";
        }
        var providers = properties.providers();
        var found = provider != null ? providers.get(provider)
                : providers.size() == 1 ? providers.values().iterator().next() : null;
        return found == null || found.type() == null ? "messaging" : found.type().tag();
    }

    static InboxRedrive metered(InboxRedrive redrive, ListableBeanFactory beans) {
        MeterRegistry meters = beans.getBeanProvider(MeterRegistry.class).getIfAvailable();
        return meters == null ? redrive : new MeteredInboxRedrive(redrive, meters);
    }
}
