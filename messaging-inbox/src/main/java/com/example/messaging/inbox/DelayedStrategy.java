package com.example.messaging.inbox;

import com.example.messaging.Capabilities;
import com.example.messaging.inbox.retry.RetryPolicy;

/** Strategy of the delayed level (ADR-0002). {@link #AUTO} is resolved once, at startup, from the capabilities. */
public enum DelayedStrategy {
    AUTO,
    BROKER_DELAY,
    RETRY_QUEUES,
    HOLD;

    /** A resolved strategy and why it was chosen, for the startup log. */
    record Choice(DelayedStrategy strategy, String reason) {
    }

    /**
     * Resolves {@code requested} and validates the combinations of ADR-0009.
     *
     * @throws IllegalStateException when the combination cannot work
     */
    static Choice resolve(DelayedStrategy requested, Ordering ordering, Capabilities caps, boolean hasRetryQueues,
                          RetryPolicy policy) {
        if (ordering == Ordering.STRICT && !caps.orderedDelivery()) {
            throw new IllegalStateException("ordering: strict needs a receiver with orderedDelivery");
        }
        Choice choice = switch (requested) {
            case AUTO -> auto(ordering, caps, hasRetryQueues);
            case BROKER_DELAY -> {
                if (!caps.delayedRedelivery()) {
                    throw new IllegalStateException("delayed.strategy: broker-delay needs delayedRedelivery");
                }
                yield new Choice(BROKER_DELAY, "configured");
            }
            case RETRY_QUEUES -> {
                if (!hasRetryQueues) {
                    throw new IllegalStateException("delayed.strategy: retry-queues without delayed.queues");
                }
                if (ordering == Ordering.STRICT) {
                    throw new IllegalStateException("delayed.strategy: retry-queues breaks ordering: strict");
                }
                yield new Choice(RETRY_QUEUES, "configured");
            }
            case HOLD -> new Choice(HOLD, "configured");
        };
        if (choice.strategy() == BROKER_DELAY && policy.delayed().maxAttempts() > 0
                && policy.delayed().max().compareTo(caps.maxRedeliveryDelay()) > 0) {
            throw new IllegalStateException("delayed.max " + policy.delayed().max()
                    + " is above the receiver maxRedeliveryDelay " + caps.maxRedeliveryDelay());
        }
        return choice;
    }

    private static Choice auto(Ordering ordering, Capabilities caps, boolean hasRetryQueues) {
        if (ordering == Ordering.STRICT) {
            return caps.delayedRedelivery()
                    ? new Choice(BROKER_DELAY, "auto: strict, receiver has delayedRedelivery and orderedDelivery")
                    : new Choice(HOLD, "auto: strict, receiver without delayedRedelivery");
        }
        if (caps.delayedRedelivery()) {
            return new Choice(BROKER_DELAY, "auto: receiver has delayedRedelivery");
        }
        return hasRetryQueues
                ? new Choice(RETRY_QUEUES, "auto: delayed.queues configured, receiver without delayedRedelivery")
                : new Choice(HOLD, "auto: receiver without delayedRedelivery nor delayed.queues");
    }
}
