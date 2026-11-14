package com.example.messaging.inbox.spring;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.Effect;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.Outcome;
import com.example.messaging.inbox.retry.ExceptionClassifier;
import io.micrometer.common.KeyValue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.ReceiverContext;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final String system;
    private final MeterRegistry meters;
    private final ObservationRegistry observations;
    private final Clock clock;
    private final boolean pass;
    private final AtomicInteger inFlight = new AtomicInteger();

    /**
     * @param system the {@code messaging.system} of the spans: the provider type, as the transport meters tag it
     * @param meters or {@code observations} may be {@code null}
     * @param pass   a level-3 pass over the DLQ: a failure there goes back to the end of the DLQ, which is a retry
     *               and not a new entry into it
     */
    MicrometerConsumerObserver(String consumer, String destination, String system, MeterRegistry meters,
                               ObservationRegistry observations, Clock clock, boolean pass) {
        this.consumer = consumer;
        this.destination = destination;
        this.system = system;
        this.meters = meters;
        this.observations = observations;
        this.clock = clock;
        this.pass = pass;
        if (meters != null) {
            inFlightGauge();
        }
    }

    /**
     * Received and not yet delivered: unlike {@code received - processed}, it survives a restart of the counters.
     * The consumer is built again on each start, so the gauge of the previous one (which holds a dead counter) is
     * replaced.
     */
    private void inFlightGauge() {
        Gauge previous = meters.find("inbox.messages.in.flight").tags("consumer", consumer, "destination", destination)
                .gauge();
        if (previous != null) {
            meters.remove(previous);
        }
        Gauge.builder("inbox.messages.in.flight", inFlight, AtomicInteger::get).strongReference(true)
                .description("messages handed over by the broker and not yet acked, nacked or copied")
                .tags("consumer", consumer, "destination", destination).register(meters);
    }

    @Override
    public void received(ReceivedMessage message) {
        if (meters != null) {
            counter("inbox.messages.received").increment();
            inFlight.incrementAndGet();
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
                // Low cardinality keys become the tags of the timers the meter handler builds from the observation, so
                // they use the names of the inbox.* meters. The OpenTelemetry names stay on the span, as high
                // cardinality keys, which no meter takes (ADR-0007).
                .lowCardinalityKeyValue("consumer", consumer)
                .lowCardinalityKeyValue("destination", destination)
                .lowCardinalityKeyValue("level", message.delivery().level().name().toLowerCase())
                // same key set even when the execution throws: meter handlers build one timer from these keys
                .lowCardinalityKeyValue("outcome", "released")
                .highCardinalityKeyValue("messaging.system", system)
                .highCardinalityKeyValue("messaging.destination.name", destination)
                .highCardinalityKeyValue("messaging.consumer.group.name", consumer)
                .highCardinalityKeyValue("messaging.message.id", message.id())
                .highCardinalityKeyValue("inbox.attempt", Integer.toString(message.delivery().attempt()));
        return observation.observe(() -> {
            Outcome outcome = execution.get();
            observation.lowCardinalityKeyValue(KeyValue.of("outcome", outcomeTag(outcome, null)));
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
            Timer.builder("inbox.hold.wait.duration").description("planned wait of a message held in memory")
                    .tags("consumer", consumer, "destination", destination).register(meters).record(retry.delay());
        }
    }

    @Override
    public void keepAliveFailed(ReceivedMessage message, RuntimeException failure) {
        if (meters != null) {
            Counter.builder("inbox.keepalives.failed")
                    .description("messages processed without lease renewal because the keepAlive could not start")
                    .tags("consumer", consumer, "destination", destination).register(meters).increment();
        }
    }

    @Override
    public void unexpectedFailure(Stage stage, Throwable failure) {
        if (meters != null) {
            Counter.builder("inbox.failures.unexpected").description("failures the worker did not expect (Errors, bugs)")
                    .tags("consumer", consumer, "destination", destination, "stage", stage.name().toLowerCase(),
                            "exception", exception(failure))
                    .register(meters).increment();
        }
    }

    @Override
    public void handled(InboxMessage message, Duration duration, Throwable failure) {
        if (meters != null) {
            Timer.builder("inbox.handler.duration").description("one call of the handler, immediate retries included")
                    .tags("consumer", consumer, "destination", destination, "outcome",
                            failure == null ? "success" : "error")
                    .register(meters).record(duration);
        }
    }

    @Override
    public void idempotency(InboxMessage message, StoreCall call, Duration duration, RuntimeException failure) {
        if (meters != null) {
            Timer.builder("inbox.idempotency.duration").description("one call to the idempotency store")
                    .tags("consumer", consumer, "destination", destination, "operation", call.name().toLowerCase(),
                            "outcome", failure == null ? "success" : "error")
                    .register(meters).record(duration);
        }
    }

    @Override
    public void delivered(Report report) {
        if (meters == null) {
            return;
        }
        inFlight.decrementAndGet();
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
            case DEAD_LETTERED -> {
                if (pass) {
                    retry("dead_letter", "none", cause);
                } else {
                    Counter.builder("inbox.messages.dead.lettered").description("copies sent to the DLQ")
                            .tags("consumer", consumer, "destination", destination, "exception", exception(cause))
                            .register(meters).increment();
                }
            }
            case FAILED -> Counter.builder("inbox.messages.failed").description("copies sent to failed")
                    .tags("consumer", consumer, "destination", destination, "reason", failureReason(report.outcome()),
                            "exception", exception(cause))
                    .register(meters).increment();
            case EFFECT_FAILED, COPY_IMPOSSIBLE -> effectFailed(
                    report.failedCall() == null ? "unknown" : report.failedCall().name().toLowerCase());
            default -> {
            }
        }
        if (report.inbox() != null) {
            DistributionSummary.builder("inbox.delivery.attempts")
                    .description("attempts already spent on the level the delivery was in; 0 on the first one")
                    .tags("consumer", consumer, "destination", destination, "level",
                            report.inbox().delivery().level().name().toLowerCase())
                    .register(meters).record(report.inbox().delivery().attempt());
        }
        if (report.retrySkipped()) {
            Counter.builder("inbox.retries.skipped").description("deliveries that skipped the delayed level")
                    .tags("consumer", consumer, "destination", destination, "reason",
                            report.message().deliveryCount().isEmpty() ? "delivery_count_unavailable" : "redelivery_limit")
                    .register(meters).increment();
        }
        if (report.budgetExceeded()) {
            Counter.builder("inbox.attributes.budget.exceeded").description("copies that left out inbox attributes for lack of room")
                    .tags("consumer", consumer, "destination", destination).register(meters).increment();
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
    public void passEnded(com.example.messaging.inbox.PassEnd end, int processed) {
        if (meters != null) {
            Counter.builder("inbox.dead.letter.passes").description("level-3 passes over the DLQ, by how they ended")
                    .tags("consumer", consumer, "end", end.name().toLowerCase()).register(meters).increment();
        }
    }

    @Override
    public void receiveFailed(RuntimeException failure) {
        if (meters != null) {
            effectFailed("receive");
        }
    }

    /** ADR-0007 values; the effect wins when it differs from the decision. */
    String outcomeTag(Outcome outcome, Effect effect) {
        if (effect == Effect.ACK_LEASE_EXPIRED) {
            return "lease_expired"; // the copy or the work is done, but the message comes back as a duplicate
        }
        if (effect == Effect.DEAD_LETTERED) {
            return pass ? "retry_dead_letter" : "dead_letter";
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
            case Outcome.DeadLetter d -> pass ? "retry_dead_letter" : "dead_letter";
            case Outcome.Failed f -> "failed";
            case Outcome.Release r -> "released";
        };
    }

    private void retry(String level, String strategy, Throwable cause) {
        Counter.builder("inbox.messages.retried").description("retries scheduled, by level and strategy")
                .tags("consumer", consumer, "destination", destination, "level", level, "strategy", strategy,
                        "exception", exception(cause))
                .register(meters).increment();
    }

    private void effectFailed(String effect) {
        Counter.builder("inbox.effects.failed").description("broker calls that failed")
                .tags("consumer", consumer, "destination", destination, "effect", effect)
                .register(meters).increment();
    }

    private Counter counter(String name) {
        return Counter.builder(name).description("messages handed over by the broker, redeliveries included")
                .tags("consumer", consumer, "destination", destination).register(meters);
    }

    private static String failureReason(Outcome outcome) {
        return outcome instanceof Outcome.Failed f ? f.reason().name().toLowerCase() : "retries_exhausted";
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
