package com.example.messaging.spring;

import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
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
 * <p>Com {@link MeterRegistry}: timer {@code messaging.send} com {@code provider},
 * {@code destination} e {@code outcome} ({@code success}/{@code error}). O nome é diferente do da
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
        return observe(traceparent -> timed(() -> delegate.send(withTraceparent(message, traceparent))));
    }

    /** Uma observation para o lote: todas as mensagens levam o mesmo {@code traceparent}. */
    @Override
    public BatchSendResult sendAll(List<OutgoingMessage> messages) {
        return observe(traceparent -> timed(() -> delegate.sendAll(
                messages.stream().map(message -> withTraceparent(message, traceparent)).toList())));
    }

    private <T> T observe(Function<String, T> action) {
        if (observations == null || observations.isNoop()) {
            return action.apply(null);
        }
        SenderContext<Map<String, String>> context = new SenderContext<>(Map::put, Kind.PRODUCER);
        context.setCarrier(new HashMap<>());
        context.setRemoteServiceName(provider);
        Observation observation = Observation.createNotStarted("messaging.publish", () -> context, observations)
                .lowCardinalityKeyValue("messaging.system", provider)
                .lowCardinalityKeyValue("messaging.destination.name", destination)
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

    private <T> T timed(Supplier<T> action) {
        if (meters == null) {
            return action.get();
        }
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        try {
            T result = action.get();
            outcome = "success";
            return result;
        } finally {
            sample.stop(Timer.builder("messaging.send").tag("provider", provider).tag("destination", destination)
                    .tag("outcome", outcome).register(meters));
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
