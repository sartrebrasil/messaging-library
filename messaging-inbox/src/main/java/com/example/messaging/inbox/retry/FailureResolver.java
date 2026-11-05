package com.example.messaging.inbox.retry;

import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.retry.RetryDecision.FailureReason;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.DoubleSupplier;

/** Turns the result of the immediate level into a {@link RetryDecision} (ADR-0002, ADR-0014). */
public final class FailureResolver {

    private final RetryPolicy policy;
    private final ExceptionClassifier classifier;
    private final DoubleSupplier random;

    public FailureResolver(RetryPolicy policy, ExceptionClassifier classifier, DoubleSupplier random) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.classifier = Objects.requireNonNull(classifier, "classifier");
        this.random = Objects.requireNonNull(random, "random");
    }

    /** Delivery from the main destination: levels 1 and 2, then DLQ (or {@code failed} without one). */
    public RetryDecision resolve(ImmediateRetrier.Result result, AttemptState state) {
        if (!result.failed()) {
            return switch (result.result()) {
                case HandlerResult.RetryAfter ra -> delayOrDeadLetter(Optional.of(ra.delay()), ra.cause(), state);
                case HandlerResult other -> terminal(other);
            };
        }
        if (result.classification() == Classification.NON_RETRYABLE) {
            return new RetryDecision.Failed(FailureReason.NON_RETRYABLE, result.failure());
        }
        return delayOrDeadLetter(classifier.retryAfterHint(result.failure()), result.failure(), state);
    }

    /** Delivery during a DLQ pass: no level 2; a retryable failure goes back to the end of the DLQ. */
    public RetryDecision resolveDeadLetterPass(ImmediateRetrier.Result result, AttemptState state) {
        Throwable cause;
        if (!result.failed()) {
            if (!(result.result() instanceof HandlerResult.RetryAfter ra)) {
                return terminal(result.result());
            }
            cause = ra.cause();
        } else if (result.classification() == Classification.NON_RETRYABLE) {
            return new RetryDecision.Failed(FailureReason.NON_RETRYABLE, result.failure());
        } else {
            cause = result.failure();
        }
        if (state.deadLetter() >= policy.deadLetterMaxAttempts()) {
            return new RetryDecision.Failed(FailureReason.RETRIES_EXHAUSTED, cause);
        }
        return new RetryDecision.DeadLetter(state.nextDeadLetter(), cause);
    }

    private static RetryDecision terminal(HandlerResult result) {
        return switch (result) {
            case HandlerResult.Ack a -> RetryDecision.ACK;
            case HandlerResult.Discard d -> new RetryDecision.Discard(d.reason());
            case HandlerResult.Reject r -> new RetryDecision.Failed(FailureReason.REJECTED, r.cause());
            case HandlerResult.RetryAfter ra -> throw new IllegalStateException("RetryAfter is not terminal");
        };
    }

    private RetryDecision delayOrDeadLetter(Optional<Duration> hint, Throwable cause, AttemptState state) {
        BackoffSpec delayed = policy.delayed();
        if (state.delayed() >= delayed.maxAttempts()) {
            return policy.deadLetterEnabled()
                    ? new RetryDecision.DeadLetter(state, cause)
                    : new RetryDecision.Failed(FailureReason.RETRIES_EXHAUSTED, cause);
        }
        int next = state.delayed() + 1;
        Duration delay = hint.map(h -> min(h, delayed.max())).orElseGet(() -> delayed.delayFor(next, random));
        return new RetryDecision.Delay(delay, state.nextDelayed(), cause);
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
