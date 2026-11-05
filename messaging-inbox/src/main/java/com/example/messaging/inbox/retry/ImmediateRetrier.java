package com.example.messaging.inbox.retry;

import com.example.messaging.inbox.HandlerResult;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.DoubleSupplier;

/**
 * Immediate level: re-runs the handler on the same thread, with the same message, without returning it to
 * the broker (ADR-0002). The lease is kept alive by the caller during the whole level.
 *
 * <p>Stops early when the failure is NON_RETRYABLE or SKIP_IMMEDIATE, or when the handler returns a
 * {@link HandlerResult}. {@link Error}s are not retried and propagate.
 */
public final class ImmediateRetrier {

    /** Waits between attempts. Interruption means shutdown (ADR-0009). */
    @FunctionalInterface
    public interface Sleeper {
        Sleeper THREAD = Thread::sleep;

        void sleep(Duration duration) throws InterruptedException;
    }

    /**
     * @param result         final result when there was no failure; {@code null} if {@code failure != null}
     * @param failure        last failure; {@code null} on success or explicit result
     * @param executions     handler executions in this delivery
     * @param classification classification of the last failure; {@code null} if {@code failure == null}
     */
    public record Result(HandlerResult result, Throwable failure, int executions, Classification classification) {
        public boolean failed() {
            return failure != null;
        }
    }

    private final BackoffSpec spec;
    private final ExceptionClassifier classifier;
    private final Sleeper sleeper;
    private final DoubleSupplier random;

    public ImmediateRetrier(BackoffSpec spec, ExceptionClassifier classifier, Sleeper sleeper, DoubleSupplier random) {
        this.spec = Objects.requireNonNull(spec, "spec");
        this.classifier = Objects.requireNonNull(classifier, "classifier");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.random = Objects.requireNonNull(random, "random");
    }

    /** @throws InterruptedException when interrupted while waiting or by the handler; the message is released */
    public Result execute(Callable<HandlerResult> handler) throws InterruptedException {
        for (int n = 1; ; n++) {
            Exception failure;
            try {
                HandlerResult result = handler.call();
                return new Result(result == null ? HandlerResult.ack() : result, null, n, null);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                failure = e;
            }
            Classification cls = classifier.classify(failure);
            if (cls != Classification.RETRYABLE || n > spec.maxAttempts()) {
                return new Result(null, failure, n, cls);
            }
            sleeper.sleep(spec.delayFor(n, random));
        }
    }
}
