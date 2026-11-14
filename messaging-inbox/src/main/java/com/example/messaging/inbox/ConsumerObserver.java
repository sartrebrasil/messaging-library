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

    /** A message was handed over by the broker, before any processing (redeliveries included). */
    default void received(ReceivedMessage message) {
    }

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

    /** A level-3 pass over the DLQ ended (ADR-0002), after {@code processed} messages. */
    default void passEnded(PassEnd end, int processed) {
    }

    /** A {@code receive} failed; the worker backs off. */
    default void receiveFailed(RuntimeException failure) {
    }

    /** The {@code keepAlive} of a message could not start: it is processed without lease renewal. */
    default void keepAliveFailed(ReceivedMessage message, RuntimeException failure) {
    }

    /** A failure nobody expected (an {@code Error}, a bug); the message was released or sent to failed. */
    default void unexpectedFailure(Stage stage, Throwable failure) {
    }

    /** One call of the handler, each immediate retry being one; {@code failure} is {@code null} when it returned. */
    default void handled(InboxMessage message, Duration duration, Throwable failure) {
    }

    /** One call to the idempotency store; {@code failure} is {@code null} when it worked. */
    default void idempotency(InboxMessage message, StoreCall call, Duration duration, RuntimeException failure) {
    }

    /**
     * @param inbox          the read message; {@code null} when the envelope could not be read
     * @param retrySkipped   the delayed level was skipped (no delivery count, or the delay was refused)
     * @param budgetExceeded a copy left out {@code inbox_*} attributes for lack of room
     * @param duration       from the start of the first execution to the end of the effect
     * @param failedCall     the broker call that failed when {@code effect} is {@code EFFECT_FAILED} or
     *                       {@code COPY_IMPOSSIBLE}; {@code null} otherwise
     */
    record Report(ReceivedMessage message, InboxMessage inbox, Outcome outcome, Effect effect, DelayedStrategy strategy,
                  boolean retrySkipped, boolean budgetExceeded, Duration duration, BrokerCall failedCall) {
    }

    /** A call the consumer makes on the broker to apply an outcome. */
    enum BrokerCall { ACK, NACK, COPY }

    /** Where an unexpected failure was caught. */
    enum Stage { LANE, DELIVERY, DEAD_LETTER_PASS }

    /** A call the consumer makes on the idempotency store. */
    enum StoreCall { RESERVE, COMPLETE, RELEASE }
}
