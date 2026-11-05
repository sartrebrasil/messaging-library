package com.example.messaging.inbox;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class SafeObserverTest {

    private static final Outcome ACK = new Outcome.Ack(Outcome.AckReason.SUCCESS);

    private final AtomicInteger runs = new AtomicInteger();
    private final Supplier<Outcome> execution = () -> {
        runs.incrementAndGet();
        return ACK;
    };

    @Test
    void anObserverThatFailsAfterTheExecutionKeepsItsResult() {
        var observer = new SafeObserver(new ConsumerObserver() {
            @Override
            public Outcome execution(InboxMessage message, Supplier<Outcome> execution) {
                execution.get();
                throw new IllegalStateException("metrics down");
            }
        });

        assertEquals(ACK, observer.execution(null, execution));
        assertEquals(1, runs.get());
    }

    @Test
    void anObserverThatFailsBeforeOrSkipsTheExecutionStillRunsItOnce() {
        var failsFirst = new SafeObserver(new ConsumerObserver() {
            @Override
            public Outcome execution(InboxMessage message, Supplier<Outcome> execution) {
                throw new IllegalStateException("tracer down");
            }
        });
        var skips = new SafeObserver(new ConsumerObserver() {
            @Override
            public Outcome execution(InboxMessage message, Supplier<Outcome> execution) {
                return null;
            }
        });
        var callsTwice = new SafeObserver(new ConsumerObserver() {
            @Override
            public Outcome execution(InboxMessage message, Supplier<Outcome> execution) {
                execution.get();
                return execution.get();
            }
        });

        assertEquals(ACK, failsFirst.execution(null, execution));
        assertEquals(ACK, skips.execution(null, execution));
        assertEquals(ACK, callsTwice.execution(null, execution));
        assertEquals(3, runs.get());
    }

    @Test
    void aFailureOfTheExecutionItselfPropagates() {
        var observer = new SafeObserver(ConsumerObserver.NONE);

        assertThrows(IllegalArgumentException.class, () -> observer.execution(null, () -> {
            runs.incrementAndGet();
            throw new IllegalArgumentException("pipeline bug");
        }));
        assertEquals(1, runs.get());
    }

    @Test
    void callbackFailuresAreSwallowed() {
        var observer = new SafeObserver(new ConsumerObserver() {
            @Override
            public void delivered(Report report) {
                throw new IllegalStateException("registry closed");
            }
        });

        assertDoesNotThrow(() -> observer.delivered(null));
    }
}
