package com.example.messaging.inbox;

import java.time.Duration;

/**
 * Exceptions implementing this interface suggest the delay of the next delayed retry (e.g. an HTTP
 * {@code Retry-After} header). The hint replaces the computed backoff, capped at {@code retry.delayed.max}.
 * It does not skip the immediate level by itself; combine with {@link SkipImmediate} for that.
 */
public interface RetryAfterHint {

    /** Suggested delay; {@code null} or negative means no suggestion. */
    Duration retryAfter();
}
