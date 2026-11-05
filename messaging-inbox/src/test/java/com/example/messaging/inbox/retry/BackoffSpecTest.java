package com.example.messaging.inbox.retry;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BackoffSpecTest {

    private final BackoffSpec delayed = RetryPolicy.defaults().delayed();

    @Test
    void delayedFollowsAdrSequence() {
        assertEquals(Duration.ofSeconds(30), delayed.baseDelay(1));
        assertEquals(Duration.ofMinutes(1), delayed.baseDelay(2));
        assertEquals(Duration.ofMinutes(2), delayed.baseDelay(3));
        assertEquals(Duration.ofMinutes(4), delayed.baseDelay(4));
        assertEquals(Duration.ofMinutes(8), delayed.baseDelay(5));
        assertEquals(Duration.ofSeconds(930), delayed.totalBaseDelay(5));
    }

    @Test
    void capWithoutOverflow() {
        assertEquals(Duration.ofMinutes(15), delayed.baseDelay(6));
        assertEquals(Duration.ofMinutes(15), delayed.baseDelay(60));
    }

    @Test
    void jitterOnlyShortensTheDelay() {
        Duration base = delayed.baseDelay(1);
        assertEquals(base, delayed.delayFor(1, () -> 0.0));
        assertEquals(base.multipliedBy(9).dividedBy(10), delayed.delayFor(1, () -> 0.5));
        Duration capped = delayed.baseDelay(10);
        assertEquals(Duration.ofMinutes(15), delayed.delayFor(10, () -> 0.0));
        assertEquals(capped.multipliedBy(8).dividedBy(10), delayed.delayFor(10, () -> 1.0));
    }

    @Test
    void immediateDefaultsWaitUpTo700ms() {
        BackoffSpec immediate = RetryPolicy.defaults().immediate();
        assertEquals(Duration.ofMillis(700), immediate.totalBaseDelay(immediate.maxAttempts()));
    }

    @Test
    void zeroAttemptsIsAllowed() {
        assertEquals(0, new BackoffSpec(0, Duration.ofSeconds(1), 2, Duration.ofSeconds(1), 0).maxAttempts());
    }

    @Test
    void validatesParameters() {
        assertThrows(IllegalArgumentException.class,
                () -> new BackoffSpec(-1, Duration.ofSeconds(1), 2, Duration.ofSeconds(1), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BackoffSpec(1, Duration.ofSeconds(2), 2, Duration.ofSeconds(1), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BackoffSpec(1, Duration.ofSeconds(1), 0.5, Duration.ofSeconds(1), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BackoffSpec(1, Duration.ofSeconds(1), 2, Duration.ofSeconds(1), 1.5));
    }
}
