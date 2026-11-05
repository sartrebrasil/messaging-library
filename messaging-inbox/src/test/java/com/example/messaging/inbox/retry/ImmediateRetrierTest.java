package com.example.messaging.inbox.retry;

import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.NonRetryable;
import com.example.messaging.inbox.SkipImmediate;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ImmediateRetrierTest {

    @NonRetryable
    static class Fatal extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    @SkipImmediate
    static class RateLimited extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private final List<Duration> sleeps = new ArrayList<>();
    private final AtomicInteger calls = new AtomicInteger();

    private ImmediateRetrier retrier(int maxAttempts) {
        var spec = new BackoffSpec(maxAttempts, Duration.ofMillis(100), 2, Duration.ofSeconds(2), 0);
        return new ImmediateRetrier(spec, new ExceptionClassifier(RetryPolicy.defaults()), sleeps::add, () -> 0.0);
    }

    @Test
    void succeedsOnSecondExecution() throws Exception {
        var r = retrier(3).execute(() -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("timeout");
            }
            return null;
        });
        assertEquals(2, r.executions());
        assertNull(r.failure());
        assertSame(HandlerResult.ack(), r.result());
        assertEquals(List.of(Duration.ofMillis(100)), sleeps);
    }

    @Test
    void maxAttemptsDoesNotCountTheFirstExecution() throws Exception {
        var r = retrier(3).execute(() -> {
            calls.incrementAndGet();
            throw new IllegalStateException("down");
        });
        assertEquals(4, calls.get());
        assertEquals(4, r.executions());
        assertEquals(Classification.RETRYABLE, r.classification());
        assertEquals(List.of(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400)), sleeps);
    }

    @Test
    void zeroAttemptsRunsOnce() throws Exception {
        var r = retrier(0).execute(() -> {
            calls.incrementAndGet();
            throw new IllegalStateException("down");
        });
        assertEquals(1, calls.get());
        assertEquals(1, r.executions());
    }

    @Test
    void nonRetryableAndSkipImmediateStopAtFirstExecution() throws Exception {
        assertEquals(Classification.NON_RETRYABLE, retrier(3).execute(() -> {
            throw new Fatal();
        }).classification());
        assertEquals(Classification.SKIP_IMMEDIATE, retrier(3).execute(() -> {
            throw new RateLimited();
        }).classification());
        assertEquals(List.of(), sleeps);
    }

    @Test
    void explicitResultIsNotRetried() throws Exception {
        var r = retrier(3).execute(() -> {
            calls.incrementAndGet();
            return HandlerResult.retryAfter(Duration.ofSeconds(90));
        });
        assertEquals(1, calls.get());
        assertInstanceOf(HandlerResult.RetryAfter.class, r.result());
    }

    @Test
    void interruptionPropagates() {
        var spec = new BackoffSpec(3, Duration.ofMillis(100), 2, Duration.ofSeconds(2), 0);
        var retrier = new ImmediateRetrier(spec, new ExceptionClassifier(RetryPolicy.defaults()), d -> {
            throw new InterruptedException();
        }, () -> 0.0);
        assertThrows(InterruptedException.class, () -> retrier.execute(() -> {
            throw new IllegalStateException("down");
        }));
    }

    @Test
    void errorsAreNotRetried() {
        assertThrows(StackOverflowError.class, () -> retrier(3).execute(() -> {
            calls.incrementAndGet();
            throw new StackOverflowError();
        }));
        assertEquals(1, calls.get());
    }
}
