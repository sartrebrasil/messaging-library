package com.example.messaging.gcp;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.contract.InboxContract;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;

/**
 * Contrato da inbox (F3) no emulator do Pub/Sub: um par tópico/subscription por destino; o atraso do nível 2 é o
 * ack deadline. Sem limite nativo padrão (só com dead-letter policy). Pulado sem Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class PubSubInboxContractTest extends InboxContract {

    private static String main;
    private static String failed;
    private static String deadLetter;
    private static String retry;

    @BeforeAll
    static void setUp() {
        PubSubEmulator.start();
        main = PubSubEmulator.topicWithSubscription("inbox-main", "inbox-main-sub", false);
        failed = PubSubEmulator.topicWithSubscription("inbox-failed", "inbox-failed-sub", false);
        deadLetter = PubSubEmulator.topicWithSubscription("inbox-dlq", "inbox-dlq-sub", false);
        retry = PubSubEmulator.topicWithSubscription("inbox-retry", "inbox-retry-sub", false);
    }

    private static MessageReceiver receiver(String subscription) {
        return PubSubEmulator.receiver(subscription, PubSubMessageReceiver.Options.ackDeadline(PubSubEmulator.LEASE));
    }

    @Override
    protected MessageSender newSender() {
        return PubSubEmulator.sender("inbox-main");
    }

    @Override
    protected MessageReceiver newReceiver() {
        return receiver(main);
    }

    @Override
    protected MessageSender failedSender() {
        return PubSubEmulator.sender("inbox-failed");
    }

    @Override
    protected MessageReceiver failedReceiver() {
        return receiver(failed);
    }

    @Override
    protected MessageSender deadLetterSender() {
        return PubSubEmulator.sender("inbox-dlq");
    }

    @Override
    protected MessageReceiver deadLetterReceiver() {
        return receiver(deadLetter);
    }

    @Override
    protected Duration lease() {
        return PubSubEmulator.LEASE;
    }

    @Override
    protected List<RetryQueue> retryQueues() {
        return List.of(new RetryQueue(lease(), PubSubEmulator.sender("inbox-retry")));
    }

    @Override
    protected MessageReceiver retryQueueReceiver() {
        return receiver(retry);
    }

    /** Sem dead-letter policy, o Pub/Sub não informa {@code deliveryAttempt}. */
    @Override
    protected boolean reportsDeliveryCount() {
        return false;
    }

    @Override
    protected Duration settle() {
        return Duration.ofSeconds(40);
    }
}
