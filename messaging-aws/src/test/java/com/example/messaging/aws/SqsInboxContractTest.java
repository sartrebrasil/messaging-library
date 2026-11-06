package com.example.messaging.aws;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.contract.InboxContract;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;

/** Contrato da inbox (F3) no LocalStack: fila standard, cópias em filas comuns. Pulado sem Docker. */
@Testcontainers(disabledWithoutDocker = true)
class SqsInboxContractTest extends InboxContract {

    private static String main;
    private static String failed;
    private static String deadLetter;
    private static String retry;

    @BeforeAll
    static void queues() {
        LocalStack.start();
        main = LocalStack.queue("inbox-main");
        failed = LocalStack.queue("inbox-failed");
        deadLetter = LocalStack.queue("inbox-dlq");
        retry = LocalStack.queue("inbox-retry");
    }

    @Override
    protected MessageSender newSender() {
        return new SqsMessageSender(LocalStack.sqs(), main);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), main);
    }

    @Override
    protected MessageSender failedSender() {
        return new SqsMessageSender(LocalStack.sqs(), failed);
    }

    @Override
    protected MessageReceiver failedReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), failed);
    }

    @Override
    protected MessageSender deadLetterSender() {
        return new SqsMessageSender(LocalStack.sqs(), deadLetter);
    }

    @Override
    protected MessageReceiver deadLetterReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), deadLetter);
    }

    @Override
    protected Duration lease() {
        return LocalStack.LEASE;
    }

    @Override
    protected List<RetryQueue> retryQueues() {
        return List.of(new RetryQueue(lease(), new SqsMessageSender(LocalStack.sqs(), retry)));
    }

    @Override
    protected MessageReceiver retryQueueReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), retry);
    }
}
