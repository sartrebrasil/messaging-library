package com.example.messaging.spring;

import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Métricas e {@code traceparent} no envio (ADR-0008). Com {@link ObservationRegistry}, cada envio
 * abre a observation {@code messaging.publish} com um {@link SenderContext} sobre um mapa; os
 * handlers de tracing do Boot gravam o {@code traceparent} nele, e o adapter o envia como atributo
 * reservado. Mensagem que já tem {@code traceparent} segue como está.
 *
 * <p>Com {@link MeterRegistry}: timer {@code messaging.send.duration} com {@code provider},
 * {@code destination}, {@code batch} e {@code outcome} ({@code success}, {@code error} ou, num lote em que só
 * parte das mensagens falhou, {@code partial}). O nome é diferente do da
 * observation para não colidir com o timer que o {@code DefaultMeterObservationHandler} cria.</p>
 */
final class ObservedMessageSender implements MessageSender {

    static final String TRACEPARENT = "traceparent";

    private final MessageSender delegate;
    private final String provider;
    private final String destination;
    private final MeterRegistry meters;
    private final ObservationRegistry observations;

    ObservedMessageSender(MessageSender delegate, String provider, String destination, MeterRegistry meters,
                          ObservationRegistry observations) {
        this.delegate = delegate;
        this.provider = provider;
        this.destination = destination;
        this.meters = meters;
        this.observations = observations;
    }

    @Override
    public SendResult send(OutgoingMessage message) {
        recordSize(message);
        return observe(traceparent -> timed("false", 1, r -> "success",
                () -> delegate.send(withTraceparent(message, traceparent))));
    }

    /** Uma observation para o lote: todas as mensagens levam o mesmo {@code traceparent}. */
    @Override
    public BatchSendResult sendAll(List<OutgoingMessage> messages) {
        messages.forEach(this::recordSize);
        BatchSendResult result = observe(traceparent -> timed("true", messages.size(),
                r -> TransportTags.batchOutcome(r.failures().size(), messages.size()),
                () -> delegate.sendAll(messages.stream().map(message -> withTraceparent(message, traceparent)).toList())));
        if (meters != null) {
            result.failures().values().forEach(e -> TransportTags.failed(meters, provider, destination, "send", e, 1));
        }
        return result;
    }

    private void recordSize(OutgoingMessage message) {
        if (meters != null && message.body() != null) {
            DistributionSummary.builder("messaging.message.size").baseUnit("bytes")
                    .description("body size of the messages sent").tags("provider", provider, "destination", destination)
                    .register(meters).record(message.body().length);
        }
    }

    private <T> T observe(Function<String, T> action) {
        if (observations == null || observations.isNoop()) {
            return action.apply(null);
        }
        SenderContext<Map<String, String>> context = new SenderContext<>(Map::put, Kind.PRODUCER);
        context.setCarrier(new HashMap<>());
        context.setRemoteServiceName(provider);
        Observation observation = Observation.createNotStarted("messaging.publish", () -> context, observations)
                // the low cardinality keys are the tags of the timer the meter handler builds: those of the
                // messaging.* meters. The OpenTelemetry names stay on the span only (ADR-0007 of the inbox)
                .lowCardinalityKeyValue("provider", provider)
                .lowCardinalityKeyValue("destination", destination)
                .highCardinalityKeyValue("messaging.system", provider)
                .highCardinalityKeyValue("messaging.destination.name", destination)
                .start();
        try (Observation.Scope ignored = observation.openScope()) {
            return action.apply(context.getCarrier().get(TRACEPARENT));
        } catch (RuntimeException e) {
            observation.error(e);
            throw e;
        } finally {
            observation.stop();
        }
    }

    private <T> T timed(String batch, int items, Function<T, String> outcomeOf, Supplier<T> action) {
        if (meters == null) {
            return action.get();
        }
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        Throwable thrown = null;
        try {
            T result = action.get();
            outcome = outcomeOf.apply(result);
            return result;
        } catch (RuntimeException | Error e) {
            thrown = e;
            TransportTags.failed(meters, provider, destination, "send", e, items);
            throw e;
        } finally {
            sample.stop(Timer.builder("messaging.send.duration").description("call to the broker").tag("provider", provider).tag("destination", destination)
                    .tag("batch", batch).tag("outcome", outcome).tag("exception", TransportTags.exception(thrown))
                    .register(meters));
        }
    }

    private static OutgoingMessage withTraceparent(OutgoingMessage message, String traceparent) {
        return traceparent == null || message.traceparent() != null ? message : message.withTraceparent(traceparent);
    }

    @Override
    public void checkAccess() {
        delegate.checkAccess();
    }

    @Override
    public Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
