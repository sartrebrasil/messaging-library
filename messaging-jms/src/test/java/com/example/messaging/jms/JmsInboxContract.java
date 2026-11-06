package com.example.messaging.jms;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.contract.InboxContract;
import jakarta.jms.Connection;

import java.time.Duration;
import java.util.List;

/**
 * Contrato da inbox (F3) num broker JMS: filas {@code inbox-*}, principal com {@code SCHEDULED} (atraso do nível 2
 * por cópia agendada). As subclasses dizem o dialeto, a conexão e o limite nativo.
 */
abstract class JmsInboxContract extends InboxContract {

    static final Duration LEASE = Duration.ofSeconds(5);

    protected abstract JmsDialect dialect();

    protected abstract Connection connection();

    private MessageSender sender(String queue) {
        return JmsMessageSender.forQueue(connection(), dialect(), queue, JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
    }

    private MessageReceiver receiver(String queue) {
        return new JmsMessageReceiver(connection(), dialect(), queue);
    }

    @Override
    protected MessageSender newSender() {
        return sender("inbox-main");
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new JmsMessageReceiver(connection(), dialect(), "inbox-main", LEASE, 10,
                JmsMessageReceiver.Redelivery.SCHEDULED, null);
    }

    @Override
    protected MessageSender failedSender() {
        return sender("inbox-failed");
    }

    @Override
    protected MessageReceiver failedReceiver() {
        return receiver("inbox-failed");
    }

    @Override
    protected MessageSender deadLetterSender() {
        return sender("inbox-dlq");
    }

    @Override
    protected MessageReceiver deadLetterReceiver() {
        return receiver("inbox-dlq");
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected List<RetryQueue> retryQueues() {
        return List.of(new RetryQueue(lease(), sender("inbox-retry")));
    }

    @Override
    protected MessageReceiver retryQueueReceiver() {
        return receiver("inbox-retry");
    }

    @Override
    protected boolean delayCopies() {
        return true;
    }

    @Override
    protected Duration settle() {
        return Duration.ofSeconds(30);
    }
}
