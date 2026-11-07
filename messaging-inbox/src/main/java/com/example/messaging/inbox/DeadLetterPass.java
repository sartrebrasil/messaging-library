package com.example.messaging.inbox;

import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Level 3 (ADR-0002): a slow pass over the DLQ with the same handler, level 1 and no level 2. One message at a time; the
 * pass ends, giving back the message that ended it, at a copy made during the pass ({@code enqueuedAt >= passStart}),
 * at a key already seen in the pass, at {@code maxPerPass}, at an empty receive or when the consumer stops.
 */
final class DeadLetterPass {

    private static final System.Logger LOG = System.getLogger(DeadLetterPass.class.getName());

    private final ConsumerSettings settings;
    private final Pipeline<?> pipeline;
    private final OutcomeApplier applier;
    private final Destinations destinations;
    private final StopSignal stop;
    private final Clock clock;
    private final ConsumerObserver observer;
    private final int maxPerPass;

    DeadLetterPass(ConsumerSettings settings, Pipeline<?> pipeline, OutcomeApplier applier, Destinations destinations,
                   StopSignal stop, Clock clock, ConsumerObserver observer, int maxPerPass) {
        this.settings = settings;
        this.pipeline = pipeline;
        this.applier = applier;
        this.destinations = destinations;
        this.stop = stop;
        this.clock = clock;
        this.observer = observer;
        this.maxPerPass = maxPerPass;
    }

    /** One pass, synchronously. Never throws: a broken DLQ ends the pass with {@link PassEnd#RECEIVE_FAILED}. */
    PassEnd runPass() {
        Instant passStart = clock.instant();
        long start = System.nanoTime();
        Set<String> seen = new HashSet<>();
        int processed = 0;
        int acked = 0;
        int requeued = 0;
        int failed = 0;
        PassEnd end;
        try (MessageReceiver receiver = destinations.deadLetterReceivers().get()) {
            while (true) {
                if (stop.stopping()) {
                    end = PassEnd.STOPPING;
                    break;
                }
                List<ReceivedMessage> received;
                try {
                    received = receiver.receive(1, settings.pollWait());
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer()
                            + ": receive from the dead-letter destination failed; pass ended", e);
                    end = PassEnd.RECEIVE_FAILED;
                    break;
                }
                if (received.isEmpty()) {
                    end = PassEnd.EMPTY;
                    break;
                }
                ReceivedMessage message = received.getFirst();
                end = endingAt(message, passStart, seen, processed);
                if (end != null) {
                    giveBack(receiver, message);
                    break;
                }
                Effect effect = process(receiver, message);
                processed++;
                switch (effect) {
                    case ACKED -> acked++;
                    case DEAD_LETTERED -> requeued++;
                    case FAILED -> failed++;
                    default -> { }
                }
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer()
                    + ": dead-letter destination unavailable; pass ended", e);
            end = PassEnd.RECEIVE_FAILED;
        }
        LOG.log(System.Logger.Level.INFO, "Consumer {0}: dead-letter pass ended ({1}) after {2} ms: {3} processed, "
                        + "{4} acked, {5} back to the dead-letter destination, {6} to failed", settings.consumer(),
                end.name().toLowerCase(), Duration.ofNanos(System.nanoTime() - start).toMillis(), processed, acked,
                requeued, failed);
        observer.passEnded(end, processed);
        return end;
    }

    /** {@code null} when the pass goes on with {@code message}. */
    private PassEnd endingAt(ReceivedMessage message, Instant passStart, Set<String> seen, int processed) {
        if (message.enqueuedAt() != null && !message.enqueuedAt().isBefore(passStart)) {
            return PassEnd.NEWER_THAN_PASS;
        }
        if (!seen.add(pipeline.idOf(message, settings.destination()))) {
            return PassEnd.REPEATED_KEY;
        }
        return processed >= maxPerPass ? PassEnd.MAX_PER_PASS : null;
    }

    private Effect process(MessageReceiver receiver, ReceivedMessage message) {
        long start = System.nanoTime();
        observer.received(message);
        KeepAlive lease = keepAlive(receiver, message);
        InboxMessage inbox = null;
        Outcome outcome;
        OutcomeApplier.Applied applied;
        try {
            Pipeline.Execution execution = pipeline.process(message, settings.destination(), 0, observer,
                    PassMode.DEAD_LETTER);
            inbox = execution.message();
            outcome = execution.outcome();
            if (outcome instanceof Outcome.Retry retry) {
                // in_progress on a pass: back to the end of the DLQ without spending l3 (ADR-0004)
                outcome = new Outcome.DeadLetter(retry.next(), retry.cause());
            }
            applied = applier.apply(receiver, message, outcome);
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (Throwable t) {
            // Errors too (StackOverflowError, AssertionError): to failed, not back to the DLQ, where a queue that
            // returns it to the head would stall every later pass on it (T-88/B20 in the Worker)
            LOG.log(System.Logger.Level.ERROR, "Consumer " + settings.consumer() + ": unexpected failure on message "
                    + message.messageId() + " in a dead-letter pass; sent to failed", t);
            outcome = new Outcome.Failed(Outcome.FailureReason.NON_RETRYABLE,
                    AttemptState.from(message.attributes().get(AttemptState.ATTRIBUTE), 0), t,
                    "unexpected failure in a dead-letter pass");
            applied = applier.apply(receiver, message, outcome);
        } finally {
            lease.close();
        }
        observer.delivered(new ConsumerObserver.Report(message, inbox, outcome, applied.effect(), settings.strategy(),
                applied.retrySkipped(), applied.budgetExceeded(), Duration.ofNanos(System.nanoTime() - start)));
        return applied.effect();
    }

    private KeepAlive keepAlive(MessageReceiver receiver, ReceivedMessage message) {
        try {
            return receiver.keepAlive(message, settings.maxProcessing());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": keepAlive of message "
                    + message.messageId() + " failed in a dead-letter pass; processing without lease renewal", e);
            return NO_LEASE;
        }
    }

    private void giveBack(MessageReceiver receiver, ReceivedMessage message) {
        try {
            receiver.nack(message, Duration.ZERO);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": nack of message "
                    + message.messageId() + " failed at the end of a dead-letter pass; it comes back when the lease "
                    + "expires", e);
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
