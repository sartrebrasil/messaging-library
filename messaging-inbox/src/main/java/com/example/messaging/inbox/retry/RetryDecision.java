package com.example.messaging.inbox.retry;

import java.time.Duration;
import java.util.Objects;

/**
 * What the retry engine decided for a delivery. The pipeline maps it to the adapter {@code Outcome}
 * and to the metrics of ADR-0007.
 */
public sealed interface RetryDecision {

    enum FailureReason {
        NON_RETRYABLE,
        RETRIES_EXHAUSTED,
        REJECTED
    }

    record Ack() implements RetryDecision {
    }

    /** Handler returned {@code Discard}: ack with {@code outcome=discarded}. */
    record Discard(String reason) implements RetryDecision {
    }

    /** Delayed level. {@code next} is the state to carry in {@link AttemptState#ATTRIBUTE}. */
    record Delay(Duration delay, AttemptState next, Throwable cause) implements RetryDecision {
        public Delay {
            Objects.requireNonNull(delay, "delay");
            Objects.requireNonNull(next, "next");
        }
    }

    /**
     * Copy to the DLQ: entering level 3 from the main destination, or going back to the end of the DLQ
     * during a pass ({@code next.deadLetter()} already incremented).
     */
    record DeadLetter(AttemptState next, Throwable cause) implements RetryDecision {
        public DeadLetter {
            Objects.requireNonNull(next, "next");
        }
    }

    record Failed(FailureReason reason, Throwable cause) implements RetryDecision {
        public Failed {
            Objects.requireNonNull(reason, "reason");
        }
    }

    Ack ACK = new Ack();
}
