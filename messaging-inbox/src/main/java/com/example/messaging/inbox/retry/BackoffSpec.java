package com.example.messaging.inbox.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with a cap and downward jitter (ADR-0002).
 *
 * <pre>delay(n) = min(initial × multiplier^(n-1), max) × random(1 - jitter, 1]</pre>
 *
 * Jitter only shortens the delay, so {@code max} is a real ceiling. That keeps lease and idempotency TTL
 * budgets exact.
 *
 * @param maxAttempts retries after the first execution; {@code 0} disables the level
 */
public record BackoffSpec(int maxAttempts, Duration initial, double multiplier, Duration max, double jitter) {

    public BackoffSpec {
        Objects.requireNonNull(initial, "initial");
        Objects.requireNonNull(max, "max");
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts < 0: " + maxAttempts);
        }
        if (initial.isNegative()) {
            throw new IllegalArgumentException("negative initial: " + initial);
        }
        if (max.compareTo(initial) < 0) {
            throw new IllegalArgumentException("max (" + max + ") < initial (" + initial + ")");
        }
        if (!(multiplier >= 1.0) || Double.isInfinite(multiplier)) {
            throw new IllegalArgumentException("multiplier must be >= 1: " + multiplier);
        }
        if (!(jitter >= 0.0 && jitter <= 1.0)) {
            throw new IllegalArgumentException("jitter must be in [0,1]: " + jitter);
        }
    }

    public static BackoffSpec none() {
        return new BackoffSpec(0, Duration.ZERO, 1.0, Duration.ZERO, 0.0);
    }

    /** Delay before retry {@code n} (n ≥ 1), without jitter. */
    public Duration baseDelay(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be >= 1: " + n);
        }
        double nanos = initial.toNanos() * Math.pow(multiplier, n - 1);
        return Duration.ofNanos((long) Math.min(nanos, (double) max.toNanos()));
    }

    /** Delay before retry {@code n} with jitter. {@code random} must return values in [0,1). */
    public Duration delayFor(int n, DoubleSupplier random) {
        long base = baseDelay(n).toNanos();
        if (jitter == 0.0 || base == 0L) {
            return Duration.ofNanos(base);
        }
        double factor = 1.0 - jitter * random.getAsDouble();
        return Duration.ofNanos(Math.round(base * factor));
    }

    /** Sum of the base delays of retries 1..{@code retries}. */
    public Duration totalBaseDelay(int retries) {
        Duration total = Duration.ZERO;
        for (int i = 1; i <= retries; i++) {
            total = total.plus(baseDelay(i));
        }
        return total;
    }
}
