package com.example.messaging.inbox.retry;

public enum Classification {
    /** Immediate retries, then delayed. */
    RETRYABLE,
    /** Skips the immediate level, goes to delayed. */
    SKIP_IMMEDIATE,
    /** Straight to {@code failed}. */
    NON_RETRYABLE
}
