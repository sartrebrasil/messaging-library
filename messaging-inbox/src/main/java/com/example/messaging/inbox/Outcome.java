package com.example.messaging.inbox;

import com.example.messaging.inbox.retry.AttemptState;

import java.time.Duration;
import java.util.Objects;

/**
 * What the pipeline decided for one delivery; the consumer turns it into an effect on the broker (ADR-0001,
 * ADR-0009). Only the values the F2 pipeline produces exist; idempotency, circuit breaker and expiration add theirs.
 */
public sealed interface Outcome {

    enum AckReason { SUCCESS, DISCARDED }

    enum FailureReason { NON_RETRYABLE, RETRIES_EXHAUSTED, DESERIALIZATION, REJECTED }

    /** Confirm the delivery. */
    record Ack(AckReason reason) implements Outcome {
        public Ack {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /** Delayed level (ADR-0002). {@code next} is the attempt state the redelivery carries. */
    record Retry(Duration delay, AttemptState next, Throwable cause) implements Outcome {
        public Retry {
            Objects.requireNonNull(delay, "delay");
            Objects.requireNonNull(next, "next");
        }
    }

    /** Copy to the dead-letter destination, then ack. */
    record DeadLetter(AttemptState next, Throwable cause) implements Outcome {
        public DeadLetter {
            Objects.requireNonNull(next, "next");
        }
    }

    /** Copy to {@code failed}, then ack. */
    record Failed(FailureReason reason, AttemptState state, Throwable cause) implements Outcome {
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
