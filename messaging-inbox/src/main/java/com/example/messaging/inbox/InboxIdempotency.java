package com.example.messaging.inbox;

import com.example.idempotency.spi.IdempotencyStore;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Idempotency of one consumer (ADR-0004), over the {@link IdempotencyStore} of idempotency-core. Key = the envelope
 * id, scope = the consumer name.
 *
 * @param reservationTtl how long an {@code IN_PROGRESS} reservation lives: above {@code maxProcessing} (plus the
 *                       {@code hold} delays), so a crashed consumer frees the key soon
 * @param completedTtl   how long a completed record deduplicates (default {@code P7D}): must cover redeliveries,
 *                       the DLQ passes and redrive
 */
public record InboxIdempotency(IdempotencyStore store, Duration reservationTtl, Duration completedTtl, Clock clock) {

    public InboxIdempotency {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(reservationTtl, "reservationTtl");
        Objects.requireNonNull(completedTtl, "completedTtl");
        Objects.requireNonNull(clock, "clock");
        if (reservationTtl.isNegative() || reservationTtl.isZero() || completedTtl.isNegative() || completedTtl.isZero()) {
            throw new IllegalArgumentException("idempotency TTLs must be positive");
        }
    }
}
