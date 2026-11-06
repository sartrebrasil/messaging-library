package com.example.messaging.rabbitmq;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.contract.InboxContract;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;

/**
 * Contrato da inbox (F3) no RabbitMQ: filas quorum {@code inbox-*}; sem atraso no nack ({@code broker-delay}
 * pulado). Pulado sem Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class RabbitMqInboxContractTest extends InboxContract {

    private static final Duration LEASE = Duration.ofSeconds(5);

    @BeforeAll
    static void queues() throws Exception {
        RabbitMq.start();
        List.of("inbox-main", "inbox-failed", "inbox-dlq", "inbox-retry").forEach(q -> RabbitMq.queue(q, "quorum"));
    }

    private static MessageSender sender(String queue) {
        return RabbitMessageSender.forQueue(RabbitMq.connection(), queue, RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
    }

    private static MessageReceiver receiver(String queue) {
        return new RabbitMessageReceiver(RabbitMq.connection(), queue);
    }

    @Override
    protected MessageSender newSender() {
        return sender("inbox-main");
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new RabbitMessageReceiver(RabbitMq.connection(), "inbox-main", LEASE, sender("inbox-dlq"));
    }

    @Override
    protected MessageSender failedSender() {
        return sender("inbox-failed");
    }

    @Override
    protected MessageReceiver failedReceiver() {
        return receiver("inbox-failed");
    }

    @Override
    protected MessageSender deadLetterSender() {
        return sender("inbox-dlq");
    }

    @Override
    protected MessageReceiver deadLetterReceiver() {
        return receiver("inbox-dlq");
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected List<RetryQueue> retryQueues() {
        return List.of(new RetryQueue(lease(), sender("inbox-retry")));
    }

    @Override
    protected MessageReceiver retryQueueReceiver() {
        return receiver("inbox-retry");
    }

    /**
     * Medido: 21 entregas. O {@code delivery-limit} padrão das filas quorum no RabbitMQ 4 é 20 e conta as
     * devoluções; a mensagem sai (descartada, sem DLX) depois da 21ª entrega.
     */
    @Override
    protected Integer nativeDeliveryLimit() {
        return 21;
    }

    @Override
    protected Duration settle() {
        return Duration.ofSeconds(30);
    }
}
