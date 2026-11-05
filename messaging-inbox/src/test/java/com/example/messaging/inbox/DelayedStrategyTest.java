package com.example.messaging.inbox;

import com.example.messaging.Capabilities;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static com.example.messaging.inbox.DelayedStrategy.*;
import static org.junit.jupiter.api.Assertions.*;

/** The selection and validation tables of ADR-0002 and ADR-0009. */
class DelayedStrategyTest {

    private static final RetryPolicy POLICY = RetryPolicy.defaults();

    @Test
    void autoSelection() {
        assertEquals(BROKER_DELAY, resolve(AUTO, Ordering.STRICT, caps(true, true), false));
        assertEquals(HOLD, resolve(AUTO, Ordering.STRICT, caps(false, true), true));
        assertEquals(BROKER_DELAY, resolve(AUTO, Ordering.RELAXED, caps(true, false), true));
        assertEquals(RETRY_QUEUES, resolve(AUTO, Ordering.RELAXED, caps(false, false), true));
        assertEquals(HOLD, resolve(AUTO, Ordering.RELAXED, caps(false, false), false));
    }

    @Test
    void explicitStrategiesAreValidated() {
        assertEquals(HOLD, resolve(HOLD, Ordering.RELAXED, caps(true, false), false));
        assertThrows(IllegalStateException.class, () -> resolve(BROKER_DELAY, Ordering.RELAXED, caps(false, false), false));
        assertThrows(IllegalStateException.class, () -> resolve(RETRY_QUEUES, Ordering.RELAXED, caps(false, false), false));
        assertThrows(IllegalStateException.class, () -> resolve(RETRY_QUEUES, Ordering.STRICT, caps(false, true), true));
        assertThrows(IllegalStateException.class, () -> resolve(AUTO, Ordering.STRICT, caps(true, false), false));
    }

    @Test
    void delayedMaxAboveTheProviderLimitFails() {
        var tenMinutes = new Capabilities(1, 10, true, Duration.ofMinutes(10), false, false, false, true);
        var policy = new RetryPolicy(BackoffSpec.none(),
                new BackoffSpec(5, Duration.ofSeconds(30), 2, Duration.ofMinutes(15), 0.2), true, 3,
                List.of(), List.of(), List.of());

        assertThrows(IllegalStateException.class, () -> DelayedStrategy.resolve(AUTO, Ordering.RELAXED, tenMinutes, false, policy));
    }

    private static DelayedStrategy resolve(DelayedStrategy requested, Ordering ordering, Capabilities caps, boolean queues) {
        return DelayedStrategy.resolve(requested, ordering, caps, queues, POLICY).strategy();
    }

    private static Capabilities caps(boolean delayedRedelivery, boolean ordered) {
        return new Capabilities(256 * 1024, 10, delayedRedelivery, delayedRedelivery ? Duration.ofHours(12) : Duration.ZERO,
                false, ordered, false, true);
    }
}
