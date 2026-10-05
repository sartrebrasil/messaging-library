package com.example.messaging.azure;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.testkit.MessagingContract;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static com.example.messaging.azure.ServiceBusEmulator.MAX_MESSAGE_BYTES;
import static com.example.messaging.azure.ServiceBusEmulator.builder;

/** Contrato do tópico do Service Bus lido por uma subscription, com a DLQ da subscription. Pulado sem Docker. */
@Testcontainers(disabledWithoutDocker = true)
class ServiceBusSubscriptionContractTest extends MessagingContract {

    @Override
    protected MessageSender newSender() {
        return ServiceBusMessageSender.forTopic(builder(), "contract-topic", false, MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return ServiceBusMessageReceiver.forSubscription(builder(), "contract-topic", "contract-sub", MAX_MESSAGE_BYTES);
    }

    @Override
    protected Duration lease() {
        return ServiceBusEmulator.LEASE;
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return ServiceBusMessageReceiver.forSubscriptionDeadLetterQueue(builder(), "contract-topic", "contract-sub",
                MAX_MESSAGE_BYTES);
    }
}
