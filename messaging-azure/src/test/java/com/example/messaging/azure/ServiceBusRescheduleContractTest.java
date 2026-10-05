package com.example.messaging.azure;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.testkit.MessagingContract;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static com.example.messaging.azure.ServiceBusEmulator.MAX_MESSAGE_BYTES;
import static com.example.messaging.azure.ServiceBusEmulator.builder;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Contrato da fila em {@code RESCHEDULE}, numa fila com duplicate detection: a cópia agendada
 * precisa de {@code MessageId} novo para não ser descartada. Pulado sem Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class ServiceBusRescheduleContractTest extends MessagingContract {

    @Override
    protected MessageSender newSender() {
        return ServiceBusMessageSender.forQueue(builder(), "contract-reschedule", false, MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return ServiceBusMessageReceiver.forQueue(builder(), "contract-reschedule",
                ServiceBusMessageReceiver.Redelivery.RESCHEDULE, MAX_MESSAGE_BYTES);
    }

    @Override
    protected Duration lease() {
        return ServiceBusEmulator.LEASE;
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return ServiceBusMessageReceiver.forDeadLetterQueue(builder(), "contract-reschedule", MAX_MESSAGE_BYTES);
    }

    /** A cópia agendada continua a contagem de entregas da original. */
    @Test
    void rescheduledCopyKeepsDeliveryCount() {
        String marker = UUID.randomUUID().toString();
        try (MessageSender sender = newSender(); MessageReceiver receiver = newReceiver()) {
            sender.send(OutgoingMessage.ofText("x").withAttribute("marker", marker));
            ReceivedMessage first = awaitMarked(receiver, marker);
            assertEquals(1, first.deliveryCount().orElseThrow());
            receiver.nack(first, Duration.ofSeconds(2));

            ReceivedMessage copy = awaitMarked(receiver, marker);
            assertEquals(2, copy.deliveryCount().orElseThrow());
            assertNotEquals(first.messageId(), copy.messageId());
            assertEquals(List.of(), copy.attributes().keySet().stream().filter(k -> k.equals("delivery_count")).toList());
            receiver.ack(copy);
        }
    }

    private static ReceivedMessage awaitMarked(MessageReceiver receiver, String marker) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            for (ReceivedMessage message : receiver.receive(10, Duration.ofSeconds(2))) {
                if (marker.equals(message.attributes().get("marker"))) {
                    return message;
                }
                receiver.ack(message);
            }
        }
        return fail("mensagem não chegou");
    }
}
