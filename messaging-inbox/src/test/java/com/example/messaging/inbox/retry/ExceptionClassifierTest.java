package com.example.messaging.inbox.retry;

import com.example.messaging.inbox.NonRetryable;
import com.example.messaging.inbox.PayloadDeserializationException;
import com.example.messaging.inbox.RetryAfterHint;
import com.example.messaging.inbox.SkipImmediate;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExceptionClassifierTest {

    static class InvalidOrderException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    @NonRetryable
    static class AnnotatedFatal extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    static class SubOfAnnotatedFatal extends AnnotatedFatal {
        private static final long serialVersionUID = 1L;
    }

    @SkipImmediate
    static class RateLimited extends RuntimeException implements RetryAfterHint {
        private static final long serialVersionUID = 1L;

        @Override
        public Duration retryAfter() {
            return Duration.ofSeconds(90);
        }
    }

    private static ExceptionClassifier classifier(List<Class<? extends Throwable>> nonRetryable,
                                                  List<Class<? extends Throwable>> retryable) {
        var d = RetryPolicy.defaults();
        return new ExceptionClassifier(
                new RetryPolicy(d.immediate(), d.delayed(), true, 3, nonRetryable, List.of(), retryable));
    }

    @Test
    void listWalksTheCauseChain() {
        var c = classifier(List.of(InvalidOrderException.class), List.of());
        var wrapped = new CompletionException(new IllegalStateException(new InvalidOrderException()));
        assertEquals(Classification.NON_RETRYABLE, c.classify(wrapped));
    }

    @Test
    void annotationIsInherited() {
        var c = classifier(List.of(), List.of());
        assertEquals(Classification.NON_RETRYABLE, c.classify(new SubOfAnnotatedFatal()));
        assertEquals(Classification.NON_RETRYABLE, c.classify(new PayloadDeserializationException("bad json", null)));
    }

    @Test
    void defaultsTreatIllegalArgumentAsNonRetryable() {
        var c = new ExceptionClassifier(RetryPolicy.defaults());
        assertEquals(Classification.NON_RETRYABLE, c.classify(new RuntimeException(new IllegalArgumentException())));
    }

    @Test
    void skipImmediateAndHint() {
        var c = classifier(List.of(), List.of());
        assertEquals(Classification.SKIP_IMMEDIATE, c.classify(new RuntimeException(new RateLimited())));
        assertEquals(Optional.of(Duration.ofSeconds(90)), c.retryAfterHint(new RuntimeException(new RateLimited())));
    }

    @Test
    void allowlistMode() {
        var c = classifier(List.of(), List.of(IOException.class));
        assertEquals(Classification.RETRYABLE, c.classify(new UncheckedIOException(new IOException("x"))));
        assertEquals(Classification.NON_RETRYABLE, c.classify(new IllegalStateException()));
        assertEquals(Classification.SKIP_IMMEDIATE, c.classify(new RateLimited()));
    }

    @Test
    void defaultIsRetryableAndCycleDoesNotHang() {
        var c = classifier(List.of(), List.of());
        var a = new RuntimeException("a");
        var b = new RuntimeException("b", a);
        a.initCause(b);
        assertEquals(Classification.RETRYABLE, c.classify(b));
    }
}
