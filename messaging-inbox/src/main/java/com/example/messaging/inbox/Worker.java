package com.example.messaging.inbox;

import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessagingException;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.BackoffSpec;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

/**
 * One worker of a consumer: its own receiver, a loop of {@code receive}, and the messages of each poll in sequence
 * (ADR-0009). Nothing that one message does stops the loop: only {@link OutOfMemoryError} ends the worker.
 */
final class Worker implements Runnable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    // ADR-0009: 1 s, x2, up to 30 s (retryable) or 1 min (not retryable), jitter 0.2
    private static final BackoffSpec RECEIVE_RETRYABLE =
            new BackoffSpec(Integer.MAX_VALUE, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), 0.2);
    private static final BackoffSpec RECEIVE_NOT_RETRYABLE =
            new BackoffSpec(Integer.MAX_VALUE, Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.2);

    private final ConsumerSettings settings;
    private final MessageReceiver receiver;
    private final Pipeline<?> pipeline;
    private final OutcomeApplier applier;
    private final DelayedStrategy strategy;
    private final Duration leaseBudget;
    private final StopSignal stop;
    private final AtomicReference<Instant> lastReceive;
    private final Clock clock;
    private final DoubleSupplier random;

    Worker(ConsumerSettings settings, MessageReceiver receiver, Pipeline<?> pipeline, OutcomeApplier applier,
           DelayedStrategy strategy, Duration leaseBudget, StopSignal stop, AtomicReference<Instant> lastReceive,
           Clock clock, DoubleSupplier random) {
        this.settings = settings;
        this.receiver = receiver;
        this.pipeline = pipeline;
        this.applier = applier;
        this.strategy = strategy;
        this.leaseBudget = leaseBudget;
        this.stop = stop;
        this.lastReceive = lastReceive;
        this.clock = clock;
        this.random = random;
    }

    @Override
    public void run() {
        int receiveFailures = 0;
        try {
            while (!stop.stopping()) {
                List<ReceivedMessage> messages;
                try {
                    messages = receiver.receive(settings.maxMessagesPerPoll(), settings.pollWait());
                } catch (RuntimeException e) {
                    if (stop.stopping()) {
                        return;
                    }
                    receiveFailures++;
                    boolean retryable = !(e instanceof MessagingException me) || me.retryable();
                    Duration wait = (retryable ? RECEIVE_RETRYABLE : RECEIVE_NOT_RETRYABLE).delayFor(receiveFailures, random);
                    LOG.log(retryable ? System.Logger.Level.WARNING : System.Logger.Level.ERROR,
                            "Consumer " + settings.consumer() + ": receive failed, retrying in " + wait, e);
                    stop.sleep(wait);
                    continue;
                }
                receiveFailures = 0;
                lastReceive.set(clock.instant());
                handleAll(messages);
            }
        } catch (InterruptedException e) {
            // stop signal during a receive backoff
        } finally {
            receiver.close();
        }
    }

    private void handleAll(List<ReceivedMessage> messages) {
        List<KeepAlive> leases = new ArrayList<>(messages.size());
        try {
            for (int i = 0; i < messages.size(); i++) {
                leases.add(keepAlive(messages.get(i), leaseBudget.multipliedBy(i + 1)));
            }
            for (int i = 0; i < messages.size(); i++) {
                ReceivedMessage message = messages.get(i);
                try {
                    if (stop.stopping()) {
                        applier.release(receiver, message, OutcomeApplier.Effect.RELEASED);
                    } else {
                        handle(message);
                    }
                } finally {
                    leases.get(i).close();
                }
            }
        } finally {
            leases.forEach(KeepAlive::close);
        }
    }

    private KeepAlive keepAlive(ReceivedMessage message, Duration budget) {
        try {
            return receiver.keepAlive(message, budget);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": keepAlive of message "
                    + message.messageId() + " failed; processing without lease renewal", e);
            return NO_LEASE;
        }
    }

    private void handle(ReceivedMessage message) {
        try {
            int hold = 0;
            while (true) {
                Outcome outcome = pipeline.handle(message, settings.destination(), hold);
                if (outcome instanceof Outcome.Retry retry && strategy == DelayedStrategy.HOLD) {
                    try {
                        stop.sleep(retry.delay());
                    } catch (InterruptedException e) {
                        applier.release(receiver, message, OutcomeApplier.Effect.RELEASED);
                        return;
                    }
                    if (retry.reason() == Outcome.RetryReason.FAILURE) {
                        hold++; // in_progress waits do not spend an attempt (ADR-0004)
                    }
                    continue;
                }
                applier.apply(receiver, message, outcome);
                return;
            }
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (Throwable t) {
            // Errors too (StackOverflowError, AssertionError, NoClassDefFoundError): the worker must not die (T-88/B20)
            LOG.log(System.Logger.Level.ERROR, "Consumer " + settings.consumer() + ": unexpected failure on message "
                    + message.messageId() + "; released", t);
            applier.release(receiver, message, OutcomeApplier.Effect.RELEASED);
        }
    }

    private static final KeepAlive NO_LEASE = new KeepAlive() {
        @Override
        public boolean lost() {
            return false;
        }

        @Override
        public void close() {
        }
    };
}
