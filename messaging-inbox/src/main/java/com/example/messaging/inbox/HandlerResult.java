package com.example.messaging.inbox;

import java.time.Duration;
import java.util.Objects;

/**
 * Explicit result of a handler. Takes precedence over exception classification.
 * A {@code void} handler that returns normally is equivalent to {@link #ack()}.
 *
 * <p>There is no plain "retry": throwing the exception does that. See ADR-0014.
 */
public sealed interface HandlerResult {

    /** Processed successfully. */
    record Ack() implements HandlerResult {
    }

    /**
     * Skips the immediate level and goes to the delayed level with {@code delay} instead of the computed
     * backoff, capped at {@code retry.delayed.max}. Consumes a delayed attempt.
     */
    record RetryAfter(Duration delay, Throwable cause) implements HandlerResult {
        public RetryAfter {
            Objects.requireNonNull(delay, "delay");
            if (delay.isNegative()) {
                throw new IllegalArgumentException("negative delay: " + delay);
            }
        }
    }

    /** Definitive failure: goes to the {@code failed} destination with {@code reason=rejected}, no retry. */
    record Reject(String reason, Throwable cause) implements HandlerResult {
        public Reject {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /** Acknowledged without further processing. Always counted as {@code outcome=discarded}. */
    record Discard(String reason) implements HandlerResult {
        public Discard {
            Objects.requireNonNull(reason, "reason");
        }
    }

    Ack ACK = new Ack();

    static HandlerResult ack() {
        return ACK;
    }

    static HandlerResult retryAfter(Duration delay) {
        return new RetryAfter(delay, null);
    }

    static HandlerResult retryAfter(Duration delay, Throwable cause) {
        return new RetryAfter(delay, cause);
    }

    static HandlerResult reject(String reason) {
        return new Reject(reason, null);
    }

    static HandlerResult reject(String reason, Throwable cause) {
        return new Reject(reason, cause);
    }

    static HandlerResult discard(String reason) {
        return new Discard(reason);
    }
}
