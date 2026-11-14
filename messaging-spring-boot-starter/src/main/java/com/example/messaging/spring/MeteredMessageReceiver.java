package com.example.messaging.spring;

import com.example.messaging.AckResult;
import com.example.messaging.Capabilities;
import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.ReceivedMessage;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Timers {@code messaging.receive.duration}, {@code messaging.ack.duration} (com {@code batch}), {@code messaging.nack.duration} e
 * {@code messaging.dead.letter.duration} com {@code provider}, {@code destination} e {@code outcome}, e
 * a distribuição {@code messaging.receive.messages} com o tamanho de cada lote (ADR-0008).
 * Abrir o span de processamento é de quem consome: {@link ReceivedMessage#traceparent()} traz o
 * contexto de quem publicou.
 */
final class MeteredMessageReceiver implements MessageReceiver {

    private final MessageReceiver delegate;
    private final String provider;
    private final String destination;
    private final MeterRegistry meters;
    private final DistributionSummary batchSize;

    MeteredMessageReceiver(MessageReceiver delegate, String provider, String destination, MeterRegistry meters) {
        this.delegate = delegate;
        this.provider = provider;
        this.destination = destination;
        this.meters = meters;
        this.batchSize = DistributionSummary.builder("messaging.receive.messages").description("messages returned by each receive")
                .tag("provider", provider).tag("destination", destination).register(meters);
    }

    @Override
    public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
        List<ReceivedMessage> messages = timed("messaging.receive.duration", null, () -> delegate.receive(maxMessages, maxWait));
        batchSize.record(messages.size());
        return messages;
    }

    @Override
    public void ack(ReceivedMessage message) {
        timed("messaging.ack.duration", "ack", "false", 1, r -> "success", () -> {
            delegate.ack(message);
            return null;
        });
    }

    @Override
    public AckResult ackAll(List<ReceivedMessage> messages) {
        AckResult result = timed("messaging.ack.duration", "ack", "true", messages.size(),
                r -> TransportTags.batchOutcome(r.failures().size(), messages.size()), () -> delegate.ackAll(messages));
        result.failures().values().forEach(e -> TransportTags.failed(meters, provider, destination, "ack", e, 1));
        return result;
    }

    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        timed("messaging.nack.duration", "nack", () -> {
            delegate.nack(message, redeliverAfter);
            return null;
        });
    }

    @Override
    public void deadLetter(ReceivedMessage message, String reason) {
        timed("messaging.dead.letter.duration", "dead_letter", () -> {
            delegate.deadLetter(message, reason);
            return null;
        });
    }

    private <T> T timed(String name, String operation, Supplier<T> action) {
        return timed(name, operation, null, 1, r -> "success", action);
    }

    /**
     * @param operation the {@code operation} of {@code messaging.messages.failed}; {@code null} for a call that does
     *                  not act on messages ({@code receive})
     * @param batch     {@code "true"} or {@code "false"} for the calls that have a batch form; {@code null} for the rest
     * @param items     messages the call acts on, counted as failed when it throws
     */
    private <T> T timed(String name, String operation, String batch, int items, Function<T, String> outcomeOf,
                        Supplier<T> action) {
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        Throwable thrown = null;
        try {
            T result = action.get();
            outcome = outcomeOf.apply(result);
            return result;
        } catch (RuntimeException | Error e) {
            thrown = e;
            if (operation != null) {
                TransportTags.failed(meters, provider, destination, operation, e, items);
            }
            throw e;
        } finally {
            Timer.Builder timer = Timer.builder(name).description("call to the broker").tag("provider", provider)
                    .tag("destination", destination);
            if (batch != null) {
                timer.tag("batch", batch);
            }
            sample.stop(timer.tag("outcome", outcome).tag("exception", TransportTags.exception(thrown)).register(meters));
        }
    }

    @Override
    public void extendLease(ReceivedMessage message, Duration lease) {
        delegate.extendLease(message, lease);
    }

    @Override
    public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
        return delegate.keepAlive(message, maxTotal);
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
