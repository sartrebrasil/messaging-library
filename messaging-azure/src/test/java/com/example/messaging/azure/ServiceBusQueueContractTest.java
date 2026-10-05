package com.example.messaging.azure;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.testkit.MessagingContract;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static com.example.messaging.azure.ServiceBusEmulator.MAX_MESSAGE_BYTES;
import static com.example.messaging.azure.ServiceBusEmulator.builder;

/**
 * Contrato da fila do Service Bus em {@code ABANDON}, com DLQ nativa; o par ordenado é a fila com
 * sessions. Pulado sem Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class ServiceBusQueueContractTest extends MessagingContract {

    @Override
    protected MessageSender newSender() {
        return ServiceBusMessageSender.forQueue(builder(), "contract", false, MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return ServiceBusMessageReceiver.forQueue(builder(), "contract", ServiceBusMessageReceiver.Redelivery.ABANDON,
                MAX_MESSAGE_BYTES);
    }

    @Override
    protected Duration lease() {
        return ServiceBusEmulator.LEASE;
    }

    @Override
    protected MessageSender newOrderedSender() {
        return ServiceBusMessageSender.forQueue(builder(), "contract-sessions", true, MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newOrderedReceiver() {
        return ServiceBusSessionMessageReceiver.forQueue(builder(), "contract-sessions", MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return ServiceBusMessageReceiver.forDeadLetterQueue(builder(), "contract", MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageSender newMissingDestinationSender() {
        return ServiceBusMessageSender.forQueue(builder(), "nao-existe", false, MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newMissingDestinationReceiver() {
        return ServiceBusMessageReceiver.forQueue(builder(), "nao-existe", ServiceBusMessageReceiver.Redelivery.ABANDON,
                MAX_MESSAGE_BYTES);
    }
}
