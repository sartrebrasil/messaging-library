package com.example.messaging.inbox;

import com.example.messaging.inbox.retry.ImmediateRetrier;

import java.time.Duration;

/**
 * Stop signal of one consumer, and the only way its waits end early (ADR-0009). Every wait (immediate backoff,
 * {@code hold}, {@code receive} backoff) sleeps here instead of using {@link Thread#interrupt()}, so a handler in the
 * middle of a JDBC or HTTP call is never interrupted by shutdown.
 */
public final class StopSignal implements ImmediateRetrier.Sleeper {

    private boolean stopping;

    /** @throws InterruptedException when the signal fires before {@code duration} ends */
    @Override
    public synchronized void sleep(Duration duration) throws InterruptedException {
        long deadline = System.nanoTime() + duration.toNanos();
        while (!stopping) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            wait(Math.max(1, remaining / 1_000_000));
        }
        throw new InterruptedException("consumer stopping");
    }

    public synchronized void stop() {
        stopping = true;
        notifyAll();
    }

    public synchronized boolean stopping() {
        return stopping;
    }
}
