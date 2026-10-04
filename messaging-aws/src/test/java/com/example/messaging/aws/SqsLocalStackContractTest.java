package com.example.messaging.aws;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.testkit.MessagingContract;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Contrato contra o LocalStack: fila standard, fila FIFO e DLQ por cópia. Pulado sem Docker. */
@Testcontainers(disabledWithoutDocker = true)
class SqsLocalStackContractTest extends MessagingContract {

    private static String queue;
    private static String deadLetterQueue;
    private static String fifoQueue;

    @BeforeAll
    static void setUp() {
        LocalStack.start();
        queue = LocalStack.queue("contract");
        deadLetterQueue = LocalStack.queue("contract-dlq");
        fifoQueue = LocalStack.queue("contract.fifo");
    }

    @Override
    protected MessageSender newSender() {
        return new SqsMessageSender(LocalStack.sqs(), queue);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), queue, new SqsMessageSender(LocalStack.sqs(), deadLetterQueue));
    }

    @Override
    protected Duration lease() {
        return LocalStack.LEASE;
    }

    @Override
    protected MessageSender newOrderedSender() {
        return new SqsMessageSender(LocalStack.sqs(), fifoQueue);
    }

    @Override
    protected MessageReceiver newOrderedReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), fifoQueue);
    }

    @Override
    protected MessageReceiver newDeadLetterReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), deadLetterQueue);
    }

    @Override
    protected MessageSender newMissingDestinationSender() {
        return new SqsMessageSender(LocalStack.sqs(), LocalStack.missingQueueUrl());
    }

    @Override
    protected MessageReceiver newMissingDestinationReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), LocalStack.missingQueueUrl());
    }

    /** Pendência do F0: o LocalStack fixado aceita o limite novo de 1 MiB. */
    @Test
    void acceptsMessageCloseToOneMebibyte() {
        byte[] body = "x".repeat(1000 * 1024).getBytes();
        String bigQueue = LocalStack.queue("contract-grande");
        try (MessageSender sender = new SqsMessageSender(LocalStack.sqs(), bigQueue);
             MessageReceiver receiver = new SqsMessageReceiver(LocalStack.sqs(), bigQueue)) {
            sender.send(OutgoingMessage.of(body).withContentType("text/plain"));

            List<ReceivedMessage> received = receiver.receive(1, Duration.ofSeconds(5));
            assertArrayEquals(body, received.getFirst().body());
            receiver.ack(received.getFirst());
        }
    }

    /** Corpo binário vai em Base64 e nove atributos vão empacotados; o formato no fio é estável. */
    @Test
    void wireFormatForBinaryBodyAndPackedAttributes() {
        String wireQueue = LocalStack.queue("contract-fio");
        OutgoingMessage message = OutgoingMessage.of(new byte[]{0, 1, 2}).withContentType("application/avro");
        for (int i = 0; i < 9; i++) {
            message = message.withAttribute("k" + i, "v");
        }
        try (MessageSender sender = new SqsMessageSender(LocalStack.sqs(), wireQueue)) {
            sender.send(message);
        }

        var raw = LocalStack.sqs().receiveMessage(b -> b.queueUrl(wireQueue).waitTimeSeconds(5)
                .messageAttributeNames("All")).messages().getFirst();
        assertEquals("AAEC", raw.body());
        assertEquals(java.util.Set.of("attributes", "content_type"), raw.messageAttributes().keySet());
        assertNotEquals(-1, raw.messageAttributes().get("attributes").stringValue().indexOf("\"k8\":\"v\""));
    }
}
