package com.example.messaging.azure;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.contract.InboxContract;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;

import static com.example.messaging.azure.ServiceBusEmulator.MAX_MESSAGE_BYTES;
import static com.example.messaging.azure.ServiceBusEmulator.builder;

/**
 * Contrato da inbox (F3) no emulator: fila {@code inbox} com {@code RESCHEDULE} (atraso do nível 2 por cópia
 * agendada) e cópias em filas comuns. Pulado sem Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class ServiceBusInboxContractTest extends InboxContract {

    @Override
    protected MessageSender newSender() {
        return ServiceBusMessageSender.forQueue(builder(), "inbox", false, MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return ServiceBusMessageReceiver.forQueue(builder(), "inbox", ServiceBusMessageReceiver.Redelivery.RESCHEDULE,
                MAX_MESSAGE_BYTES);
    }

    @Override
    protected MessageSender failedSender() {
        return queueSender("inbox-failed");
    }

    @Override
    protected MessageReceiver failedReceiver() {
        return queueReceiver("inbox-failed");
    }

    @Override
    protected MessageSender deadLetterSender() {
        return queueSender("inbox-dlq");
    }

    @Override
    protected MessageReceiver deadLetterReceiver() {
        return queueReceiver("inbox-dlq");
    }

    @Override
    protected Duration lease() {
        return ServiceBusEmulator.LEASE;
    }

    @Override
    protected List<RetryQueue> retryQueues() {
        return List.of(new RetryQueue(lease(), queueSender("inbox-retry")));
    }

    @Override
    protected MessageReceiver retryQueueReceiver() {
        return queueReceiver("inbox-retry");
    }

    /** {@code MaxDeliveryCount} das entidades do {@code service-bus-config.json}, o padrão do Service Bus. */
    @Override
    protected Integer nativeDeliveryLimit() {
        return 10;
    }

    @Override
    protected boolean delayCopies() {
        return true;
    }

    @Override
    protected Duration settle() {
        return Duration.ofSeconds(30);
    }

    static MessageSender queueSender(String queue) {
        return ServiceBusMessageSender.forQueue(builder(), queue, false, MAX_MESSAGE_BYTES);
    }

    static MessageReceiver queueReceiver(String queue) {
        return ServiceBusMessageReceiver.forQueue(builder(), queue, ServiceBusMessageReceiver.Redelivery.ABANDON,
                MAX_MESSAGE_BYTES);
    }
}
