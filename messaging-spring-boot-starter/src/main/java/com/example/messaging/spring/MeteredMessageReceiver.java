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
import java.util.function.Supplier;

/**
 * Timers {@code messaging.receive}, {@code messaging.ack}, {@code messaging.nack} e
 * {@code messaging.dead.letter} com {@code provider}, {@code destination} e {@code outcome}, e
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
        this.batchSize = DistributionSummary.builder("messaging.receive.messages")
                .tag("provider", provider).tag("destination", destination).register(meters);
    }

    @Override
    public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
        List<ReceivedMessage> messages = timed("messaging.receive", () -> delegate.receive(maxMessages, maxWait));
        batchSize.record(messages.size());
        return messages;
    }

    @Override
    public void ack(ReceivedMessage message) {
        timed("messaging.ack", () -> {
            delegate.ack(message);
            return null;
        });
    }

    @Override
    public AckResult ackAll(List<ReceivedMessage> messages) {
        return timed("messaging.ack", () -> delegate.ackAll(messages));
    }

    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        timed("messaging.nack", () -> {
            delegate.nack(message, redeliverAfter);
            return null;
        });
    }

    @Override
    public void deadLetter(ReceivedMessage message, String reason) {
        timed("messaging.dead.letter", () -> {
            delegate.deadLetter(message, reason);
            return null;
        });
    }

    private <T> T timed(String name, Supplier<T> action) {
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        try {
            T result = action.get();
            outcome = "success";
            return result;
        } finally {
            sample.stop(Timer.builder(name).tag("provider", provider).tag("destination", destination)
                    .tag("outcome", outcome).register(meters));
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
