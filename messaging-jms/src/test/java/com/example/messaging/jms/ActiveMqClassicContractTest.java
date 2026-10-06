package com.example.messaging.jms;

import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.testkit.MessagingContract;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Contrato contra o ActiveMQ Classic: fila com atraso agendado, fila ordenada por grupo e DLQ por
 * cópia. Pulado sem Docker.
 *
 * <p>O Classic cria destinos no primeiro uso, então envio e receive para destino inexistente não
 * falham; só o {@code checkAccess} é testado com destino inexistente.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class ActiveMqClassicContractTest extends MessagingContract {

    private static final JmsDialect CLASSIC = JmsDialect.ACTIVEMQ_CLASSIC;

    private static final Duration LEASE = Duration.ofSeconds(2);

    private static String queue;
    private static String deadLetterQueue;
    private static String orderedQueue;

    @BeforeAll
    static void setUp() throws JMSException {
        ActiveMqClassic.start();
        queue = ActiveMqClassic.queue("contract");
        deadLetterQueue = ActiveMqClassic.queue("contract-dlq");
        orderedQueue = ActiveMqClassic.queue("contract-ordered");
    }

    /** Limite de 1 MiB para o contrato exercitar {@code MessageTooLargeException} sem alocar 100 MiB. */
    @Override
    protected MessageSender newSender() {
        return JmsMessageSender.forQueue(ActiveMqClassic.connection(), CLASSIC, queue, 1024 * 1024);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new JmsMessageReceiver(ActiveMqClassic.connection(), CLASSIC, queue, LEASE, 10,
                JmsMessageReceiver.Redelivery.SCHEDULED, JmsMessageSender.forQueue(ActiveMqClassic.connection(),
                CLASSIC, deadLetterQueue, JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES));
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected MessageSender newOrderedSender() {
        return JmsMessageSender.forQueue(ActiveMqClassic.connection(), CLASSIC, orderedQueue,
                JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newOrderedReceiver() {
        return new JmsMessageReceiver(ActiveMqClassic.connection(), CLASSIC, orderedQueue, LEASE, 10,
                JmsMessageReceiver.Redelivery.IMMEDIATE, null);
    }

    /** O Classic aceita mensagem sem {@code JMSXGroupID} na mesma fila. */
    @Override
    protected boolean orderingKeyRequired() {
        return false;
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return new JmsMessageReceiver(ActiveMqClassic.connection(), CLASSIC, deadLetterQueue);
    }

    @Test
    void checkAccessReportsMissingDestination() {
        String missing = "nao-existe-" + UUID.randomUUID();
        try (MessageSender sender = JmsMessageSender.forQueue(ActiveMqClassic.connection(), CLASSIC, missing, 1024);
             MessageReceiver receiver = new JmsMessageReceiver(ActiveMqClassic.connection(), CLASSIC, missing)) {
            assertThrows(DestinationNotFoundException.class, sender::checkAccess);
            assertThrows(DestinationNotFoundException.class, receiver::checkAccess);
        }
    }

    /** Virtual Topic: a fila {@code Consumer.<s>.VirtualTopic.<t>} recebe o que vai ao tópico. */
    @Test
    void virtualTopicDeliversToConsumerQueue() {
        String topic = "VirtualTopic.contract-" + UUID.randomUUID();
        String subscription = ActiveMqClassic.queue("Consumer.faturamento." + topic);
        try (MessageSender sender = JmsMessageSender.forTopic(ActiveMqClassic.connection(), CLASSIC, topic, 1024);
             MessageReceiver receiver = new JmsMessageReceiver(ActiveMqClassic.connection(), CLASSIC, subscription)) {
            sender.send(OutgoingMessage.ofText("evento").withAttribute("tipo", "pedido"));

            List<ReceivedMessage> received = receiver.receive(1, Duration.ofSeconds(5));
            assertEquals("evento", received.getFirst().bodyAsString());
            assertEquals("pedido", received.getFirst().attributes().get("tipo"));
            receiver.ack(received.getFirst());
        }
    }
}
