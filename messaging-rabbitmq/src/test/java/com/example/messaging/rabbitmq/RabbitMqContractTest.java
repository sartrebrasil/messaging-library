package com.example.messaging.rabbitmq;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.testkit.MessagingContract;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contrato contra o RabbitMQ 4.1: quorum queue, DLQ por cópia e destino inexistente. Sem ordem
 * nem atraso no {@code nack}, os testes desses recursos são pulados. Pulado sem Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class RabbitMqContractTest extends MessagingContract {

    private static final Duration LEASE = Duration.ofSeconds(2);

    private static String queue;
    private static String deadLetterQueue;

    @BeforeAll
    static void setUp() throws Exception {
        RabbitMq.start();
        queue = RabbitMq.queue("contract", "quorum");
        deadLetterQueue = RabbitMq.queue("contract-dlq", "quorum");
    }

    /** Limite de 1 MiB para o contrato exercitar {@code MessageTooLargeException}. */
    @Override
    protected MessageSender newSender() {
        return RabbitMessageSender.forQueue(RabbitMq.connection(), queue, 1024 * 1024);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new RabbitMessageReceiver(RabbitMq.connection(), queue, LEASE,
                RabbitMessageSender.forQueue(RabbitMq.connection(), deadLetterQueue, 1024 * 1024));
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return new RabbitMessageReceiver(RabbitMq.connection(), deadLetterQueue);
    }

    @Override
    protected MessageSender newMissingDestinationSender() {
        return RabbitMessageSender.forQueue(RabbitMq.connection(), "nao-existe", 1024);
    }

    @Override
    protected MessageReceiver newMissingDestinationReceiver() {
        return new RabbitMessageReceiver(RabbitMq.connection(), "nao-existe");
    }

    /** Exchange fanout: cada fila ligada recebe a sua cópia; sem fila ligada, o envio não falha. */
    @Test
    void fanoutExchangeDeliversToEachBoundQueue() {
        String exchange = "eventos-" + UUID.randomUUID();
        String billing = RabbitMq.queue("faturamento-" + exchange, "quorum");
        String audit = RabbitMq.queue("auditoria-" + exchange, "classic");
        RabbitMq.fanout(exchange, billing);
        RabbitMq.fanout(exchange, audit);
        try (MessageSender sender = RabbitMessageSender.forExchange(RabbitMq.connection(), exchange, "", 1024);
             MessageReceiver first = new RabbitMessageReceiver(RabbitMq.connection(), billing);
             MessageReceiver second = new RabbitMessageReceiver(RabbitMq.connection(), audit)) {
            sender.checkAccess();
            sender.send(OutgoingMessage.ofText("evento").withAttribute("tipo", "pedido"));

            for (MessageReceiver receiver : List.of(first, second)) {
                ReceivedMessage received = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                assertEquals("evento", received.bodyAsString());
                assertEquals(Map.of("tipo", "pedido"), received.attributes());
                assertEquals(OptionalInt.of(1), received.deliveryCount());
                receiver.ack(received);
            }
        }
    }

    /** Classic queue não conta entregas: na reentrega, {@code deliveryCount} fica vazio. */
    @Test
    void classicQueueRedeliveryHasNoCount() {
        String classic = RabbitMq.queue("contract-classic-" + UUID.randomUUID(), "classic");
        try (MessageSender sender = RabbitMessageSender.forQueue(RabbitMq.connection(), classic, 1024);
             MessageReceiver receiver = new RabbitMessageReceiver(RabbitMq.connection(), classic)) {
            sender.send(OutgoingMessage.ofText("x"));
            ReceivedMessage first = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
            receiver.nack(first, Duration.ZERO);

            ReceivedMessage second = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
            assertEquals(first.messageId(), second.messageId());
            assertTrue(second.deliveryCount().isEmpty());
            receiver.ack(second);
        }
    }
}
