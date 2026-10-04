package com.example.messaging.memory;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.testkit.MessagingContract;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryMessagingContractTest extends MessagingContract {

    private static final Duration LEASE = Duration.ofMillis(600);

    private final InMemoryMessaging broker = new InMemoryMessaging();

    InMemoryMessagingContractTest() {
        InMemoryMessaging.QueueOptions options = InMemoryMessaging.QueueOptions.defaults().withLease(LEASE);
        broker.createQueue("dlq", options);
        broker.createQueue("pedidos", options.withDeadLetterQueue("dlq"));
        broker.createQueue("pedidos-ordenados", options.withOrdered(true));
    }

    @Override
    protected MessageSender newSender() {
        return broker.queueSender("pedidos");
    }

    @Override
    protected MessageReceiver newReceiver() {
        return broker.receiver("pedidos");
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected Duration maxWait() {
        return Duration.ofMillis(300);
    }

    @Override
    protected Duration settle() {
        return Duration.ofSeconds(2);
    }

    @Override
    protected MessageSender newOrderedSender() {
        return broker.queueSender("pedidos-ordenados");
    }

    @Override
    protected MessageReceiver newOrderedReceiver() {
        return broker.receiver("pedidos-ordenados");
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return broker.receiver("dlq");
    }

    @Override
    protected MessageSender newMissingDestinationSender() {
        return broker.queueSender("nao-existe");
    }

    @Override
    protected MessageReceiver newMissingDestinationReceiver() {
        return broker.receiver("nao-existe");
    }

    @Test
    void topicFansOutToSubscribedQueues() {
        broker.createQueue("faturamento", InMemoryMessaging.QueueOptions.defaults());
        broker.createQueue("estoque", InMemoryMessaging.QueueOptions.defaults());
        broker.createTopic("eventos");
        broker.subscribe("eventos", "faturamento");
        broker.subscribe("eventos", "estoque");

        try (MessageSender sender = broker.topicSender("eventos");
             MessageReceiver billing = broker.receiver("faturamento");
             MessageReceiver stock = broker.receiver("estoque")) {
            sender.send(OutgoingMessage.ofText("pedido criado"));

            List<ReceivedMessage> fromBilling = billing.receive(10, Duration.ofSeconds(1));
            List<ReceivedMessage> fromStock = stock.receive(10, Duration.ofSeconds(1));
            assertEquals("pedido criado", fromBilling.getFirst().bodyAsString());
            assertEquals("pedido criado", fromStock.getFirst().bodyAsString());
        }
    }
}
