package com.example.messaging.inbox;

import com.example.messaging.MessageSender;

import java.time.Duration;
import java.util.Objects;

/**
 * One band of the {@code retry-queues} strategy (ADR-0002): a destination whose infrastructure returns messages to
 * the main destination after {@code delay}. The library cannot check that TTL.
 */
public record RetryQueue(Duration delay, MessageSender sender) {

    public RetryQueue {
        Objects.requireNonNull(delay, "delay");
        Objects.requireNonNull(sender, "sender");
        if (delay.isNegative() || delay.isZero()) {
            throw new IllegalArgumentException("retry queue delay must be positive: " + delay);
        }
    }
}
