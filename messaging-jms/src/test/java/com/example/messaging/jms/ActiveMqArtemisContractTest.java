package com.example.messaging.jms;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.testkit.MessagingContract;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contrato contra o ActiveMQ Artemis sem auto-create: fila com atraso agendado, fila ordenada por
 * grupo, DLQ por cópia e destino inexistente. Pulado sem Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class ActiveMqArtemisContractTest extends MessagingContract {

    private static final JmsDialect ARTEMIS = JmsDialect.ARTEMIS;
    private static final Duration LEASE = Duration.ofSeconds(2);

    private static String queue;
    private static String deadLetterQueue;
    private static String orderedQueue;

    @BeforeAll
    static void setUp() throws JMSException {
        ActiveMqArtemis.start();
        queue = ActiveMqArtemis.queue("contract");
        deadLetterQueue = ActiveMqArtemis.queue("contract-dlq");
        orderedQueue = ActiveMqArtemis.queue("contract-ordered");
    }

    private static Connection connection() {
        return ActiveMqArtemis.connection();
    }

    /** Limite de 1 MiB para o contrato exercitar {@code MessageTooLargeException}. */
    @Override
    protected MessageSender newSender() {
        return JmsMessageSender.forQueue(connection(), ARTEMIS, queue, 1024 * 1024);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new JmsMessageReceiver(connection(), ARTEMIS, queue, LEASE, 10, JmsMessageReceiver.Redelivery.SCHEDULED,
                JmsMessageSender.forQueue(connection(), ARTEMIS, deadLetterQueue, 1024 * 1024));
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected MessageSender newOrderedSender() {
        return JmsMessageSender.forQueue(connection(), ARTEMIS, orderedQueue, 1024 * 1024);
    }

    @Override
    protected MessageReceiver newOrderedReceiver() {
        return new JmsMessageReceiver(connection(), ARTEMIS, orderedQueue, LEASE, 10,
                JmsMessageReceiver.Redelivery.IMMEDIATE, null);
    }

    @Override
    protected boolean orderingKeyRequired() {
        return false;
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return new JmsMessageReceiver(connection(), ARTEMIS, deadLetterQueue);
    }

    @Override
    protected MessageSender newMissingDestinationSender() {
        return JmsMessageSender.forQueue(connection(), ARTEMIS, "nao-existe", 1024);
    }

    @Override
    protected MessageReceiver newMissingDestinationReceiver() {
        return new JmsMessageReceiver(connection(), ARTEMIS, "nao-existe");
    }

    /** Endereço multicast: cada fila do endereço (FQQN) recebe a sua cópia. */
    @Test
    void multicastAddressDeliversToEachSubscription() {
        String topic = "eventos-" + UUID.randomUUID();
        String billing = ActiveMqArtemis.subscription(topic, "faturamento");
        String audit = ActiveMqArtemis.subscription(topic, "auditoria");
        try (MessageSender sender = JmsMessageSender.forTopic(connection(), ARTEMIS, topic, 1024);
             MessageReceiver first = new JmsMessageReceiver(connection(), ARTEMIS, billing);
             MessageReceiver second = new JmsMessageReceiver(connection(), ARTEMIS, audit)) {
            sender.checkAccess();
            first.checkAccess();
            sender.send(OutgoingMessage.ofText("evento").withAttribute("tipo", "pedido"));

            for (MessageReceiver receiver : List.of(first, second)) {
                ReceivedMessage received = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                assertEquals("evento", received.bodyAsString());
                // propriedades internas do Artemis (_AMQ_*) não aparecem como atributos
                assertEquals(java.util.Map.of("tipo", "pedido"), received.attributes());
                receiver.ack(received);
            }
        }
    }

    /** {@code _AMQ_DUPL_ID}: o segundo envio com o mesmo {@code deduplicationId} é descartado. */
    @Test
    void duplicateIdIsDiscardedByTheBroker() {
        String dedupQueue = ActiveMqArtemis.queue("contract-dedup-" + UUID.randomUUID());
        String id = UUID.randomUUID().toString();
        try (MessageSender sender = JmsMessageSender.forQueue(connection(), ARTEMIS, dedupQueue, 1024);
             MessageReceiver receiver = new JmsMessageReceiver(connection(), ARTEMIS, dedupQueue)) {
            assertTrue(sender.capabilities().publisherDeduplication());
            sender.send(OutgoingMessage.ofText("1").withDeduplicationId(id));
            sender.send(OutgoingMessage.ofText("2").withDeduplicationId(id));

            ReceivedMessage received = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
            assertEquals("1", received.bodyAsString());
            receiver.ack(received);
            assertTrue(receiver.receive(1, Duration.ofSeconds(1)).isEmpty());
        }
    }

    @Test
    void receiverRejectsConnectionWithConsumerWindow() throws JMSException {
        try (Connection windowed = ActiveMqArtemis.connectionWithWindow()) {
            assertThrows(IllegalArgumentException.class, () -> new JmsMessageReceiver(windowed, ARTEMIS, queue));
        }
    }
}
