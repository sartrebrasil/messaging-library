package com.example.messaging.inbox;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Broker side of a consumer. {@code receivers} must return a new receiver per call: one per worker (ADR-0009).
 * The consumer closes the receivers it creates, never the senders.
 *
 * @param deadLetter  {@code null} without a dead-letter destination
 * @param retryQueues bands of {@code retry-queues}; empty when not used
 */
public record Destinations(Supplier<MessageReceiver> receivers,
                           MessageSender failed,
                           MessageSender deadLetter,
                           List<RetryQueue> retryQueues,
                           Supplier<MessageReceiver> deadLetterReceivers) {

    /** Without level-3 passes: no receiver on the DLQ. */
    public Destinations(Supplier<MessageReceiver> receivers, MessageSender failed, MessageSender deadLetter,
                        List<RetryQueue> retryQueues) {
        this(receivers, failed, deadLetter, retryQueues, null);
    }

    public Destinations {
        Objects.requireNonNull(receivers, "receivers");
        Objects.requireNonNull(failed, "failed destination is mandatory (ADR-0002)");
        retryQueues = retryQueues == null ? List.of()
                : retryQueues.stream().sorted(Comparator.comparing(RetryQueue::delay)).toList();
    }

    /** Smallest band whose delay covers {@code delay}; above the largest, the largest (ADR-0002). */
    RetryQueue band(java.time.Duration delay) {
        for (RetryQueue queue : retryQueues) {
            if (queue.delay().compareTo(delay) >= 0) {
                return queue;
            }
        }
        return retryQueues.getLast();
    }
}
