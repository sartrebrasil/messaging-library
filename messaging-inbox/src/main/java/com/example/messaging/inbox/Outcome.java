package com.example.messaging.inbox;

import com.example.messaging.inbox.retry.AttemptState;

import java.time.Duration;
import java.util.Objects;

/**
 * What the pipeline decided for one delivery; the consumer turns it into an effect on the broker (ADR-0001,
 * ADR-0009). Only the values the F2 pipeline produces exist; idempotency, circuit breaker and expiration add theirs.
 */
public sealed interface Outcome {

    enum AckReason { SUCCESS, DISCARDED, DUPLICATE }

    enum FailureReason { NON_RETRYABLE, RETRIES_EXHAUSTED, DESERIALIZATION, REJECTED, IDEMPOTENCY_CONFLICT }

    /** Why a delivery goes to the delayed level. */
    enum RetryReason {
        /** The handler (or the idempotency store) failed: spends a delayed attempt. */
        FAILURE,
        /**
         * Another instance holds the idempotency key (ADR-0004). Spends no attempt where the inbox counts
         * ({@code hold}, {@code retry-queues}); under {@code broker-delay} the broker's delivery count grows anyway.
         */
        IN_PROGRESS
    }

    /** Confirm the delivery. */
    record Ack(AckReason reason) implements Outcome {
        public Ack {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /** Delayed level (ADR-0002). {@code next} is the attempt state the redelivery carries. */
    record Retry(Duration delay, AttemptState next, Throwable cause, RetryReason reason) implements Outcome {
        public Retry {
            Objects.requireNonNull(delay, "delay");
            Objects.requireNonNull(next, "next");
            Objects.requireNonNull(reason, "reason");
        }

        public Retry(Duration delay, AttemptState next, Throwable cause) {
            this(delay, next, cause, RetryReason.FAILURE);
        }
    }

    /** Copy to the dead-letter destination, then ack. */
    record DeadLetter(AttemptState next, Throwable cause) implements Outcome {
        public DeadLetter {
            Objects.requireNonNull(next, "next");
        }
    }

    /**
     * Copy to {@code failed}, then ack.
     *
     * @param detail the handler's reason on {@code REJECTED} ({@code HandlerResult.reject(reason)}); {@code null}
     *               otherwise
     */
    record Failed(FailureReason reason, AttemptState state, Throwable cause, String detail) implements Outcome {
        public Failed {
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(state, "state");
        }
    }

    /** {@code nack(0)} without processing: shutdown, or an effect that could not run. */
    record Release() implements Outcome {
    }

    Release RELEASE = new Release();
}
