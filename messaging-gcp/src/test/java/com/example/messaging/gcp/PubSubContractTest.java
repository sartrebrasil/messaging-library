package com.example.messaging.gcp;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.testkit.MessagingContract;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static com.example.messaging.gcp.PubSubEmulator.LEASE;

/**
 * Contrato contra o Pub/Sub emulator: tópico com subscription, par ordenado e DLQ por cópia.
 * Pulado sem Docker. O emulator não implementa IAM nem exactly-once: o {@code checkAccess} cai
 * para {@code GetTopic}/{@code GetSubscription}, e lease vencido no {@code ack} não é testado.
 */
@Testcontainers(disabledWithoutDocker = true)
class PubSubContractTest extends MessagingContract {

    private static String subscription;
    private static String orderedSubscription;
    private static String deadLetterSubscription;

    @BeforeAll
    static void setUp() {
        PubSubEmulator.start();
        subscription = PubSubEmulator.topicWithSubscription("contract", "contract-sub", false);
        orderedSubscription = PubSubEmulator.topicWithSubscription("contract-ordered", "contract-ordered-sub", true);
        deadLetterSubscription = PubSubEmulator.topicWithSubscription("contract-dlq", "contract-dlq-sub", false);
    }

    @Override
    protected MessageSender newSender() {
        return PubSubEmulator.sender("contract");
    }

    @Override
    protected MessageReceiver newReceiver() {
        return PubSubEmulator.receiver(subscription, PubSubMessageReceiver.Options.ackDeadline(LEASE)
                .withDeadLetterSender(PubSubEmulator.sender("contract-dlq")));
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected MessageSender newOrderedSender() {
        return PubSubEmulator.sender("contract-ordered");
    }

    @Override
    protected MessageReceiver newOrderedReceiver() {
        return PubSubEmulator.receiver(orderedSubscription,
                PubSubMessageReceiver.Options.ackDeadline(LEASE).withOrderedDelivery(true));
    }

    @Override
    protected boolean orderingKeyRequired() {
        return false;
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return PubSubEmulator.receiver(deadLetterSubscription, PubSubMessageReceiver.Options.ackDeadline(LEASE));
    }

    @Override
    protected MessageSender newMissingDestinationSender() {
        return PubSubEmulator.sender("nao-existe");
    }

    @Override
    protected MessageReceiver newMissingDestinationReceiver() {
        return PubSubEmulator.receiver("projects/" + PubSubEmulator.PROJECT + "/subscriptions/nao-existe",
                PubSubMessageReceiver.Options.ackDeadline(LEASE));
    }
}
