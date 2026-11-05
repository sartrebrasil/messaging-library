package com.example.messaging.inbox.spring;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.Effect;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.Outcome;
import com.example.messaging.inbox.retry.ExceptionClassifier;
import io.micrometer.common.KeyValue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.ReceiverContext;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Metrics {@code inbox.*} (Prometheus {@code inbox_*}) and the {@code inbox.consume} observation of ADR-0007, for one
 * consumer. Tags are bounded: consumer, destination, outcome, level, strategy, reason, exception class.
 *
 * <p>The observation is a {@code CONSUMER} receiver context whose carrier is the read message: {@code traceparent}
 * comes from {@link InboxMessage#traceparent()} (outbox body first, ADR-0007), {@code tracestate} and
 * {@code baggage} from its headers. The span stays open across the immediate retries, so everything the handler
 * does joins the producer's trace.</p>
 */
final class MicrometerConsumerObserver implements ConsumerObserver {

    private final String consumer;
    private final String destination;
    private final MeterRegistry meters;
    private final ObservationRegistry observations;
    private final Clock clock;

    /** @param meters or {@code observations} may be {@code null} */
    MicrometerConsumerObserver(String consumer, String destination, MeterRegistry meters,
                               ObservationRegistry observations, Clock clock) {
        this.consumer = consumer;
        this.destination = destination;
        this.meters = meters;
        this.observations = observations;
        this.clock = clock;
    }

    @Override
    public void received(ReceivedMessage message) {
        if (meters != null) {
            counter("inbox.messages.received").increment();
        }
    }

    @Override
    public Outcome execution(InboxMessage message, Supplier<Outcome> execution) {
        if (observations == null) {
            return execution.get();
        }
        ReceiverContext<InboxMessage> context = new ReceiverContext<>((carrier, key) ->
                "traceparent".equals(key) ? carrier.traceparent() : carrier.headers().get(key), Kind.CONSUMER);
        context.setCarrier(message);
        context.setRemoteServiceName(destination);
        Observation observation = Observation.createNotStarted("inbox.consume", () -> context, observations)
                .contextualName(destination + " process")
                .lowCardinalityKeyValue("messaging.system", "messaging")
                .lowCardinalityKeyValue("messaging.destination.name", destination)
                .lowCardinalityKeyValue("messaging.consumer.group.name", consumer)
                .highCardinalityKeyValue("messaging.message.id", message.id())
                .highCardinalityKeyValue("inbox.attempt", Integer.toString(message.delivery().attempt()))
                .lowCardinalityKeyValue("inbox.level", message.delivery().level().name().toLowerCase())
                // same key set even when the execution throws: meter handlers build one timer from these keys
                .lowCardinalityKeyValue("inbox.outcome", "released");
        return observation.observe(() -> {
            Outcome outcome = execution.get();
            observation.lowCardinalityKeyValue(KeyValue.of("inbox.outcome", outcomeTag(outcome, null)));
            return outcome;
        });
    }

    @Override
    public void immediateRetry(InboxMessage message, Throwable failure) {
        if (meters != null) {
            retry("immediate", "none", failure);
        }
    }

    @Override
    public void held(InboxMessage message, Outcome.Retry retry) {
        if (meters != null) {
            retry("delayed", "hold", retry.cause());
        }
    }

    @Override
    public void delivered(Report report) {
        if (meters == null) {
            return;
        }
        String outcome = outcomeTag(report.outcome(), report.effect());
        Counter.builder("inbox.messages.processed").description("deliveries by final outcome")
                .tags("consumer", consumer, "destination", destination, "outcome", outcome).register(meters).increment();
        Timer.builder("inbox.processing.duration").description("from the first execution to the effect on the broker")
                .tags("consumer", consumer, "destination", destination, "outcome", outcome).register(meters)
                .record(report.duration());
        Throwable cause = cause(report.outcome());
        switch (report.effect()) {
            case REDELIVERY_DELAYED -> retry("delayed", "broker_delay", cause);
            case RETRY_QUEUED -> retry("delayed", "retry_queues", cause);
            case DEAD_LETTERED -> Counter.builder("inbox.dead.letter").description("copies sent to the DLQ")
                    .tags("consumer", consumer, "destination", destination, "exception", exception(cause))
                    .register(meters).increment();
            case FAILED -> Counter.builder("inbox.failed").description("copies sent to failed")
                    .tags("consumer", consumer, "destination", destination, "reason", failureReason(report.outcome()),
                            "exception", exception(cause))
                    .register(meters).increment();
            case EFFECT_FAILED, COPY_IMPOSSIBLE -> effectFailed(effectOf(report.outcome()));
            default -> {
            }
        }
        if (report.retrySkipped()) {
            Counter.builder("inbox.retry.skipped").tags("consumer", consumer, "reason",
                            report.message().deliveryCount().isEmpty() ? "delivery_count_unavailable" : "redelivery_limit")
                    .register(meters).increment();
        }
        if (report.budgetExceeded()) {
            Counter.builder("inbox.attribute.budget.exceeded").tags("consumer", consumer).register(meters).increment();
        }
        if (report.inbox() != null && report.inbox().time() != null) {
            Instant start = clock.instant().minus(report.duration());
            Duration age = Duration.between(report.inbox().time(), start);
            if (!age.isNegative()) {
                Timer.builder("inbox.message.age").description("from the event time to the start of consumption")
                        .tags("consumer", consumer, "destination", destination).register(meters).record(age);
            }
        }
    }

    @Override
    public void receiveFailed(RuntimeException failure) {
        if (meters != null) {
            effectFailed("receive");
        }
    }

    /** ADR-0007 values; the effect wins when it differs from the decision. */
    static String outcomeTag(Outcome outcome, Effect effect) {
        if (effect == Effect.DEAD_LETTERED) {
            return "dead_letter";
        }
        if (effect == Effect.FAILED) {
            return "failed";
        }
        if (effect == Effect.EFFECT_FAILED || effect == Effect.COPY_IMPOSSIBLE || effect == Effect.RELEASED) {
            return "released";
        }
        return switch (outcome) {
            case Outcome.Ack a -> a.reason().name().toLowerCase();
            case Outcome.Retry r -> r.reason() == Outcome.RetryReason.IN_PROGRESS ? "in_progress" : "retry_delayed";
            case Outcome.DeadLetter d -> "dead_letter";
            case Outcome.Failed f -> "failed";
            case Outcome.Release r -> "released";
        };
    }

    private void retry(String level, String strategy, Throwable cause) {
        Counter.builder("inbox.retry").description("retries scheduled, by level and strategy")
                .tags("consumer", consumer, "destination", destination, "level", level, "strategy", strategy,
                        "exception", exception(cause))
                .register(meters).increment();
    }

    private void effectFailed(String effect) {
        Counter.builder("inbox.effect.failed").description("broker calls that failed")
                .tags("consumer", consumer, "effect", effect).register(meters).increment();
    }

    private Counter counter(String name) {
        return Counter.builder(name).tags("consumer", consumer, "destination", destination).register(meters);
    }

    private static String failureReason(Outcome outcome) {
        return outcome instanceof Outcome.Failed f ? f.reason().name().toLowerCase() : "retries_exhausted";
    }

    private static String effectOf(Outcome outcome) {
        return switch (outcome) {
            case Outcome.Ack a -> "ack";
            case Outcome.Retry r -> "nack";
            case Outcome.Release r -> "nack";
            case Outcome.DeadLetter d -> "copy";
            case Outcome.Failed f -> "copy";
        };
    }

    private static Throwable cause(Outcome outcome) {
        return switch (outcome) {
            case Outcome.Retry r -> r.cause();
            case Outcome.DeadLetter d -> d.cause();
            case Outcome.Failed f -> f.cause();
            default -> null;
        };
    }

    private static String exception(Throwable cause) {
        return cause == null ? "none" : ExceptionClassifier.rootCause(cause).getClass().getSimpleName();
    }
}
