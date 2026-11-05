package com.example.messaging.inbox;

/** A payload that does not deserialize will never succeed: non-retryable (poison message). */
@NonRetryable
public class PayloadDeserializationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PayloadDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
