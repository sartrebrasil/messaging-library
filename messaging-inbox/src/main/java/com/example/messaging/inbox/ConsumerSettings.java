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
 * @param keyLanes                lanes that process the messages of one {@code receive} in parallel (ADR-0016):
 *                                same {@code orderingKey}, same lane, in order; {@code 1} processes them in sequence
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
                               boolean includeExceptionMessage,
                               int keyLanes,
                               Duration deadLetterInterval,
                               int deadLetterMaxPerPass) {

    /** Level-3 passes (ADR-0002) with the defaults: every 15 min, up to 100 messages. */
    public ConsumerSettings(String consumer, String destination, int concurrency, int maxMessagesPerPoll,
                            Duration pollWait, Duration maxProcessing, Duration shutdownTimeout, Ordering ordering,
                            DelayedStrategy strategy, boolean includeExceptionMessage, int keyLanes) {
        this(consumer, destination, concurrency, maxMessagesPerPoll, pollWait, maxProcessing, shutdownTimeout,
                ordering, strategy, includeExceptionMessage, keyLanes, Duration.ofMinutes(15), 100);
    }

    public ConsumerSettings {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(pollWait, "pollWait");
        Objects.requireNonNull(maxProcessing, "maxProcessing");
        Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
        Objects.requireNonNull(ordering, "ordering");
        Objects.requireNonNull(strategy, "strategy");
        if (concurrency < 1 || maxMessagesPerPoll < 1 || keyLanes < 1) {
            throw new IllegalArgumentException("concurrency, maxMessagesPerPoll and keyLanes must be at least 1");
        }
        if (maxProcessing.isNegative() || maxProcessing.isZero()) {
            throw new IllegalArgumentException("maxProcessing must be positive: " + maxProcessing);
        }
        if (pollWait.isNegative() || shutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("negative pollWait or shutdownTimeout");
        }
        Objects.requireNonNull(deadLetterInterval, "deadLetterInterval");
        if (deadLetterInterval.isNegative() || deadLetterInterval.isZero() || deadLetterMaxPerPass < 1) {
            throw new IllegalArgumentException("deadLetterInterval must be positive and deadLetterMaxPerPass at least 1");
        }
        if (pollWait.compareTo(shutdownTimeout) > 0) {
            pollWait = shutdownTimeout;
        }
    }

    /** Without key lanes: the messages of one {@code receive} run in sequence. */
    public ConsumerSettings(String consumer, String destination, int concurrency, int maxMessagesPerPoll,
                            Duration pollWait, Duration maxProcessing, Duration shutdownTimeout, Ordering ordering,
                            DelayedStrategy strategy, boolean includeExceptionMessage) {
        this(consumer, destination, concurrency, maxMessagesPerPoll, pollWait, maxProcessing, shutdownTimeout,
                ordering, strategy, includeExceptionMessage, 1);
    }

    /** Defaults of ADR-0008 for the {@code messaging} broker. */
    public static ConsumerSettings defaults(String consumer, String destination) {
        return new ConsumerSettings(consumer, destination, 1, 1, Duration.ofSeconds(20), Duration.ofMinutes(15),
                Duration.ofSeconds(30), Ordering.RELAXED, DelayedStrategy.AUTO, true, 1);
    }
}
