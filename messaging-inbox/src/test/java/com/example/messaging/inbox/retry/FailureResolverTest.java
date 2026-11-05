package com.example.messaging.inbox.retry;

import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.RetryAfterHint;
import com.example.messaging.inbox.retry.RetryDecision.FailureReason;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class FailureResolverTest {

    static class Throttled extends RuntimeException implements RetryAfterHint {
        private static final long serialVersionUID = 1L;

        @Override
        public Duration retryAfter() {
            return Duration.ofHours(1);
        }
    }

    private final RetryPolicy policy = RetryPolicy.defaults();
    private final FailureResolver resolver = resolver(policy);

    private static FailureResolver resolver(RetryPolicy policy) {
        return new FailureResolver(policy, new ExceptionClassifier(policy), () -> 0.0);
    }

    private static ImmediateRetrier.Result failed(Throwable t, Classification c) {
        return new ImmediateRetrier.Result(null, t, 4, c);
    }

    private static ImmediateRetrier.Result returned(HandlerResult r) {
        return new ImmediateRetrier.Result(r, null, 1, null);
    }

    @Test
    void firstFailureDelays30s() {
        var d = resolver.resolve(failed(new RuntimeException(), Classification.RETRYABLE), AttemptState.initial());
        var delay = assertInstanceOf(RetryDecision.Delay.class, d);
        assertEquals(Duration.ofSeconds(30), delay.delay());
        assertEquals(new AttemptState(1, 0), delay.next());
    }

    @Test
    void exhaustedGoesToDeadLetter() {
        var state = new AttemptState(5, 0);
        var d = resolver.resolve(failed(new RuntimeException(), Classification.RETRYABLE), state);
        assertEquals(state, assertInstanceOf(RetryDecision.DeadLetter.class, d).next());
    }

    @Test
    void exhaustedWithoutDeadLetterGoesToFailed() {
        var noDlq = new RetryPolicy(policy.immediate(), policy.delayed(), false, 3, List.of(), List.of(), List.of());
        var d = resolver(noDlq).resolve(failed(new RuntimeException(), Classification.RETRYABLE), new AttemptState(5, 0));
        assertEquals(FailureReason.RETRIES_EXHAUSTED, assertInstanceOf(RetryDecision.Failed.class, d).reason());
    }

    @Test
    void nonRetryableGoesToFailed() {
        var d = resolver.resolve(failed(new RuntimeException(), Classification.NON_RETRYABLE), AttemptState.initial());
        assertEquals(FailureReason.NON_RETRYABLE, assertInstanceOf(RetryDecision.Failed.class, d).reason());
    }

    @Test
    void retryAfterReplacesBackoffAndIsCappedAtMax() {
        var d = resolver.resolve(returned(HandlerResult.retryAfter(Duration.ofSeconds(90))), new AttemptState(2, 0));
        var delay = assertInstanceOf(RetryDecision.Delay.class, d);
        assertEquals(Duration.ofSeconds(90), delay.delay());
        assertEquals(3, delay.next().delayed());

        var capped = resolver.resolve(returned(HandlerResult.retryAfter(Duration.ofHours(2))), AttemptState.initial());
        assertEquals(Duration.ofMinutes(15), assertInstanceOf(RetryDecision.Delay.class, capped).delay());
    }

    @Test
    void hintFromExceptionIsCappedAtMax() {
        var d = resolver.resolve(failed(new Throttled(), Classification.SKIP_IMMEDIATE), AttemptState.initial());
        assertEquals(Duration.ofMinutes(15), assertInstanceOf(RetryDecision.Delay.class, d).delay());
    }

    @Test
    void explicitTerminalResults() {
        assertSame(RetryDecision.ACK, resolver.resolve(returned(HandlerResult.ack()), AttemptState.initial()));
        assertEquals(new RetryDecision.Discard("irrelevant"),
                resolver.resolve(returned(HandlerResult.discard("irrelevant")), AttemptState.initial()));
        var rejected = resolver.resolve(returned(HandlerResult.reject("invalid")), AttemptState.initial());
        assertEquals(FailureReason.REJECTED, assertInstanceOf(RetryDecision.Failed.class, rejected).reason());
    }

    @Test
    void deadLetterPassRequeuesUntilExhausted() {
        var retryable = failed(new RuntimeException(), Classification.RETRYABLE);
        var d = resolver.resolveDeadLetterPass(retryable, new AttemptState(5, 1));
        assertEquals(new AttemptState(5, 2), assertInstanceOf(RetryDecision.DeadLetter.class, d).next());

        var exhausted = resolver.resolveDeadLetterPass(retryable, new AttemptState(5, 3));
        assertEquals(FailureReason.RETRIES_EXHAUSTED, assertInstanceOf(RetryDecision.Failed.class, exhausted).reason());
    }

    @Test
    void deadLetterPassIgnoresRetryAfterDelay() {
        var d = resolver.resolveDeadLetterPass(returned(HandlerResult.retryAfter(Duration.ofSeconds(90))),
                new AttemptState(5, 0));
        assertInstanceOf(RetryDecision.DeadLetter.class, d);
    }

    @Test
    void worstCaseExecutionsMatchAdr() {
        assertEquals(36, policy.maxExecutions());
    }
}
