package com.example.messaging.inbox;

import com.example.messaging.inbox.retry.AttemptState;

import java.util.Objects;

/**
 * Where a delivery is in the retry levels (ADR-0003). Read from the {@code inbox_attempts} attribute, which is not
 * exposed in {@link InboxMessage#headers()}.
 *
 * @param attempt retries already spent on {@code level}; {@code 0} on the first execution
 */
public record Delivery(Level level, int attempt) {

    public enum Level { IMMEDIATE, DELAYED, DEAD_LETTER }

    public Delivery {
        Objects.requireNonNull(level, "level");
        if (attempt < 0) {
            throw new IllegalArgumentException("negative attempt: " + attempt);
        }
    }

    public static Delivery first() {
        return new Delivery(Level.IMMEDIATE, 0);
    }

    /** {@code DELAYED} once a delayed attempt was spent, {@code IMMEDIATE} otherwise. */
    public static Delivery of(AttemptState state) {
        return state.delayed() > 0 ? new Delivery(Level.DELAYED, state.delayed()) : first();
    }
}
