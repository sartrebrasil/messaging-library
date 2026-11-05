package com.example.messaging.inbox.retry;

import com.example.messaging.inbox.PayloadDeserializationException;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Three-level retry policy plus exception classification lists (ADR-0002, ADR-0014).
 *
 * <p>Every {@code maxAttempts} counts retries, never the first execution. Worst case handler executions:
 * {@code (1 + immediate) × (1 + delayed) + (1 + immediate) × deadLetter}.
 *
 * @param deadLetterEnabled      a dead-letter destination is configured; without it, exhausting the
 *                               delayed level goes straight to {@code failed}
 * @param deadLetterMaxAttempts  slow passes over the DLQ; {@code 0} keeps the DLQ as a parking queue
 * @param retryable              when not empty, enables allowlist mode: only these exceptions (and the
 *                               {@code skipImmediate} ones) are retryable
 */
public record RetryPolicy(BackoffSpec immediate,
                          BackoffSpec delayed,
                          boolean deadLetterEnabled,
                          int deadLetterMaxAttempts,
                          List<Class<? extends Throwable>> nonRetryable,
                          List<Class<? extends Throwable>> skipImmediate,
                          List<Class<? extends Throwable>> retryable) {

    public RetryPolicy {
        Objects.requireNonNull(immediate, "immediate");
        Objects.requireNonNull(delayed, "delayed");
        if (deadLetterMaxAttempts < 0) {
            throw new IllegalArgumentException("deadLetterMaxAttempts < 0: " + deadLetterMaxAttempts);
        }
        nonRetryable = List.copyOf(nonRetryable == null ? List.of() : nonRetryable);
        skipImmediate = List.copyOf(skipImmediate == null ? List.of() : skipImmediate);
        retryable = List.copyOf(retryable == null ? List.of() : retryable);
    }

    /** Defaults from ADR-0002. */
    public static RetryPolicy defaults() {
        return new RetryPolicy(
                new BackoffSpec(3, Duration.ofMillis(100), 2.0, Duration.ofSeconds(2), 0.2),
                new BackoffSpec(5, Duration.ofSeconds(30), 2.0, Duration.ofMinutes(15), 0.2),
                true, 3,
                List.of(PayloadDeserializationException.class, IllegalArgumentException.class),
                List.of(), List.of());
    }

    public int maxExecutions() {
        int perDelivery = 1 + immediate.maxAttempts();
        int passes = deadLetterEnabled ? deadLetterMaxAttempts : 0;
        return perDelivery * (1 + delayed.maxAttempts()) + perDelivery * passes;
    }
}
