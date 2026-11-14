package com.example.messaging.inbox;

import com.example.messaging.Capabilities;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;

import java.time.Clock;
import java.time.Duration;

/**
 * Turns an {@link Outcome} into broker calls on one worker's receiver (ADR-0009, ADR-0002). Copies go out before
 * the {@code ack}: a failure between the two duplicates, never loses. Returns what it did, for metrics.
 */
final class OutcomeApplier {

    private static final System.Logger LOG = System.getLogger(OutcomeApplier.class.getName());


    /**
     * @param retrySkipped    the delayed level was skipped (no delivery count, or the delay was refused)
     * @param budgetExceeded  a copy left out {@code inbox_*} attributes for lack of room
     * @param failedCall      the broker call that failed, for {@code EFFECT_FAILED} and {@code COPY_IMPOSSIBLE}
     */
    record Applied(Effect effect, boolean retrySkipped, boolean budgetExceeded, ConsumerObserver.BrokerCall failedCall) {
        static Applied of(Effect effect) {
            return new Applied(effect, false, false, null);
        }

        static Applied failed(Effect effect, ConsumerObserver.BrokerCall call) {
            return new Applied(effect, false, false, call);
        }

        Applied withBudgetExceeded() {
            return new Applied(effect, retrySkipped, true, failedCall);
        }
    }

    private final ConsumerSettings settings;
    private final DelayedStrategy strategy;
    private final Destinations destinations;
    private final Clock clock;

    OutcomeApplier(ConsumerSettings settings, DelayedStrategy strategy, Destinations destinations, Clock clock) {
        this.settings = settings;
        this.strategy = strategy;
        this.destinations = destinations;
        this.clock = clock;
    }

    Applied apply(MessageReceiver receiver, ReceivedMessage message, Outcome outcome) {
        return switch (outcome) {
            case Outcome.Ack a -> ack(receiver, message, Effect.ACKED, false, false);
            case Outcome.Release r -> release(receiver, message, Effect.RELEASED);
            case Outcome.Retry r -> retry(receiver, message, r);
            case Outcome.DeadLetter d -> deadLetterOrFailed(receiver, message, d.next(), d.cause(), false);
            case Outcome.Failed f -> copy(receiver, message, destinations.failed(), f.state(), f.cause(),
                    f.reason().name().toLowerCase(), f.detail(), Effect.FAILED, false);
        };
    }

    private Applied retry(MessageReceiver receiver, ReceivedMessage message, Outcome.Retry retry) {
        return switch (strategy) {
            case BROKER_DELAY -> {
                if (message.deliveryCount().isEmpty()) {
                    LOG.log(System.Logger.Level.ERROR, "Consumer {0}: message {1} has no delivery count, so the "
                            + "delayed level cannot count attempts; sent to the dead-letter destination",
                            settings.consumer(), message.messageId());
                    yield deadLetterOrFailed(receiver, message, retry.next(), retry.cause(), true);
                }
                Capabilities caps = receiver.capabilities();
                Duration delay = min(retry.delay(), caps.maxRedeliveryDelay());
                try {
                    receiver.nack(message, delay);
                    yield Applied.of(Effect.REDELIVERY_DELAYED);
                } catch (IllegalArgumentException e) {
                    // SQS caps the delay per message (12 h since the first receive): no more delayed attempts
                    LOG.log(System.Logger.Level.WARNING, "Consumer {0}: delay refused for message {1}: {2}",
                            settings.consumer(), message.messageId(), e.getMessage());
                    yield deadLetterOrFailed(receiver, message, retry.next(), retry.cause(), true);
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": nack of message "
                            + message.messageId() + " failed; it comes back when the lease expires", e);
                    yield Applied.failed(Effect.EFFECT_FAILED, ConsumerObserver.BrokerCall.NACK);
                }
            }
            case RETRY_QUEUES -> {
                RetryQueue band = destinations.band(retry.delay());
                Copies.Copy copy;
                try {
                    copy = Copies.build(message, retry.next(), null);
                } catch (IllegalArgumentException e) {
                    yield impossible(receiver, message, e);
                }
                if (copy.attemptsDropped()) {
                    // a retry-queue copy without the count could loop forever (ADR-0002)
                    yield deadLetterOrFailed(receiver, message, retry.next(), retry.cause(), false)
                            .withBudgetExceeded();
                }
                yield send(receiver, message, band.sender(), copy, Effect.RETRY_QUEUED, false);
            }
            case HOLD, AUTO -> throw new IllegalStateException("strategy " + strategy + " is not applied here");
        };
    }

    private Applied deadLetterOrFailed(MessageReceiver receiver, ReceivedMessage message, AttemptState next,
                                       Throwable cause, boolean retrySkipped) {
        MessageSender deadLetter = destinations.deadLetter();
        return deadLetter != null
                ? copy(receiver, message, deadLetter, next, cause, "retries_exhausted", null, Effect.DEAD_LETTERED,
                retrySkipped)
                : copy(receiver, message, destinations.failed(), next, cause, "retries_exhausted", null, Effect.FAILED,
                retrySkipped);
    }

    private Applied copy(MessageReceiver receiver, ReceivedMessage message, MessageSender target, AttemptState attempts,
                         Throwable cause, String fallbackReason, String detail, Effect effect,
                         boolean retrySkipped) {
        Copies.Copy copy;
        try {
            copy = Copies.build(message, attempts, Copies.Failure.of(settings.consumer(), settings.destination(), cause,
                    fallbackReason, detail, settings.includeExceptionMessage(), clock.instant()));
        } catch (IllegalArgumentException e) {
            return impossible(receiver, message, e);
        }
        return send(receiver, message, target, copy, effect, retrySkipped);
    }

    private Applied send(MessageReceiver receiver, ReceivedMessage message, MessageSender target, Copies.Copy copy,
                         Effect effect, boolean retrySkipped) {
        try {
            target.send(copy.message());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": copy of message "
                    + message.messageId() + " failed; the message goes back", e);
            release(receiver, message, Effect.EFFECT_FAILED);
            return new Applied(Effect.EFFECT_FAILED, retrySkipped, copy.budgetExceeded(), ConsumerObserver.BrokerCall.COPY);
        }
        return ack(receiver, message, effect, retrySkipped, copy.budgetExceeded());
    }

    private Applied impossible(MessageReceiver receiver, ReceivedMessage message, IllegalArgumentException e) {
        LOG.log(System.Logger.Level.ERROR, "Consumer " + settings.consumer() + ": message " + message.messageId()
                + " cannot be copied (" + e.getMessage() + "); released, the broker's redrive policy must take it", e);
        release(receiver, message, Effect.COPY_IMPOSSIBLE);
        return Applied.failed(Effect.COPY_IMPOSSIBLE, ConsumerObserver.BrokerCall.COPY);
    }

    private Applied ack(MessageReceiver receiver, ReceivedMessage message, Effect effect, boolean retrySkipped,
                        boolean budgetExceeded) {
        try {
            receiver.ack(message);
            return new Applied(effect, retrySkipped, budgetExceeded, null);
        } catch (LeaseExpiredException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: lease of message {1} expired before the ack; it will "
                    + "be delivered again", settings.consumer(), message.messageId());
            return new Applied(Effect.ACK_LEASE_EXPIRED, retrySkipped, budgetExceeded, null);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": ack of message "
                    + message.messageId() + " failed", e);
            return new Applied(Effect.EFFECT_FAILED, retrySkipped, budgetExceeded, ConsumerObserver.BrokerCall.ACK);
        }
    }

    Applied release(MessageReceiver receiver, ReceivedMessage message, Effect effect) {
        try {
            receiver.nack(message, Duration.ZERO);
            return Applied.of(effect);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": nack of message "
                    + message.messageId() + " failed; it comes back when the lease expires", e);
            return Applied.failed(Effect.EFFECT_FAILED, ConsumerObserver.BrokerCall.NACK);
        }
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
