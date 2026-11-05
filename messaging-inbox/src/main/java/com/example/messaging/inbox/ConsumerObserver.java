package com.example.messaging.inbox;

import com.example.messaging.ReceivedMessage;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Hook for metrics and tracing (ADR-0007); the starter implements it with Micrometer. Every method has a no-op
 * default, and an exception thrown here is logged and swallowed: observing never changes what happens to a message.
 */
public interface ConsumerObserver {

    ConsumerObserver NONE = new ConsumerObserver() {
    };

    /**
     * Around one execution of the pipeline after the envelope was read: handler, immediate retries and idempotency.
     * A delivery under {@code hold} has one execution per attempt. Must call {@code execution} exactly once.
     */
    default Outcome execution(InboxMessage message, Supplier<Outcome> execution) {
        return execution.get();
    }

    /** One retry of the immediate level, after {@code failure}. */
    default void immediateRetry(InboxMessage message, Throwable failure) {
    }

    /** A {@code hold} wait starts. */
    default void held(InboxMessage message, Outcome.Retry retry) {
    }

    /** Once per delivery, after its effect on the broker. */
    default void delivered(Report report) {
    }

    /** A {@code receive} failed; the worker backs off. */
    default void receiveFailed(RuntimeException failure) {
    }

    /**
     * @param inbox          the read message; {@code null} when the envelope could not be read
     * @param retrySkipped   the delayed level was skipped (no delivery count, or the delay was refused)
     * @param budgetExceeded a copy left out {@code inbox_*} attributes for lack of room
     * @param duration       from the start of the first execution to the end of the effect
     */
    record Report(ReceivedMessage message, InboxMessage inbox, Outcome outcome, Effect effect, DelayedStrategy strategy,
                  boolean retrySkipped, boolean budgetExceeded, Duration duration) {
    }
}
