package com.example.messaging.inbox;

import java.time.Duration;
import java.util.Objects;

/**
 * Loop settings of one consumer (ADR-0008, ADR-0009).
 *
 * @param consumer                consumer name: scope of metrics, logs and idempotency
 * @param destination             name of the main destination, as configured
 * @param concurrency             workers, each with its own receiver and platform thread
 * @param maxMessagesPerPoll      messages per {@code receive}, processed in sequence by the worker
 * @param pollWait                {@code receive} wait; capped at {@code shutdownTimeout}
 * @param maxProcessing           longest processing of one delivery; base of the lease budget
 * @param shutdownTimeout         how long {@code stop} waits for running handlers
 * @param includeExceptionMessage put the exception message in the {@code dead_letter} attribute of copies
 */
public record ConsumerSettings(String consumer,
                               String destination,
                               int concurrency,
                               int maxMessagesPerPoll,
                               Duration pollWait,
                               Duration maxProcessing,
                               Duration shutdownTimeout,
                               Ordering ordering,
                               DelayedStrategy strategy,
                               boolean includeExceptionMessage) {

    public ConsumerSettings {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(pollWait, "pollWait");
        Objects.requireNonNull(maxProcessing, "maxProcessing");
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        Objects.requireNonNull(ordering, "ordering");
        Objects.requireNonNull(strategy, "strategy");
        if (concurrency < 1 || maxMessagesPerPoll < 1) {
            throw new IllegalArgumentException("concurrency and maxMessagesPerPoll must be at least 1");
        }
        if (maxProcessing.isNegative() || maxProcessing.isZero()) {
            throw new IllegalArgumentException("maxProcessing must be positive: " + maxProcessing);
        }
        if (pollWait.isNegative() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("negative pollWait or shutdownTimeout");
        }
        if (pollWait.compareTo(shutdownTimeout) > 0) {
            pollWait = shutdownTimeout;
        }
    }

    /** Defaults of ADR-0008 for the {@code messaging} broker. */
    public static ConsumerSettings defaults(String consumer, String destination) {
        return new ConsumerSettings(consumer, destination, 1, 1, Duration.ofSeconds(20), Duration.ofMinutes(15),
                Duration.ofSeconds(30), Ordering.RELAXED, DelayedStrategy.AUTO, true);
    }
}
