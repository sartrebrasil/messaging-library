package com.example.messaging.inbox.redrive;

/**
 * Moves messages from {@code failed} or the DLQ back to the main destination (or {@code failed} to the DLQ) with the
 * attempts reset (ADR-0012). Synchronous: returns when the run ends.
 */
public interface InboxRedrive {

    /**
     * @throws IllegalArgumentException       invalid request, or a source or target the receiver does not have
     * @throws java.util.NoSuchElementException unknown receiver
     * @throws IllegalStateException          a run over the same receiver and source is in progress on this instance
     */
    RedriveReport redrive(RedriveRequest request);
}
