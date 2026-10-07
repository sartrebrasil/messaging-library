package com.example.messaging.aws;

import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Envio para uma fila SQS (SDK v2, cliente síncrono). Fila FIFO é reconhecida pelo sufixo
 * {@code .fifo} e exige {@code orderingKey} ({@code MessageGroupId}). Em fila standard,
 * {@code orderingKey} ativa fair queues (sem ordem) e {@code deduplicationId} é ignorado.
 *
 * <p>FIFO sem {@code deduplicationId} só funciona com {@code ContentBasedDeduplication} na fila.</p>
 */
public final class SqsMessageSender implements MessageSender {

    /** 1 MiB desde 08/2025, contando corpo e atributos. */
    public static final long MAX_MESSAGE_BYTES = 1024 * 1024;
    static final int MAX_BATCH = 10;

    private final SqsClient sqs;
    private final String queueUrl;
    private final boolean fifo;
    private volatile boolean closed;

    public SqsMessageSender(SqsClient sqs, String queueUrl) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.fifo = AwsCodec.isFifo(queueUrl);
    }

    @Override
    public SendResult send(OutgoingMessage message) {
        ensureOpen();
        AwsCodec.Encoded encoded = encode(message);
        try {
            String id = sqs.sendMessage(b -> b
                        .queueUrl(queueUrl)
                        .messageBody(encoded.body())
                        //.delaySeconds()
                        .messageAttributes(attributes(encoded.attributes()))
                        .messageGroupId(message.orderingKey())
                        .messageDeduplicationId(fifo ? message.deduplicationId() : null))
                    .messageId();
            return new SendResult(id);
        } catch (SdkException e) {
            throw AwsErrors.map("SendMessage " + queueUrl, e);
        }
    }

    /** Lotes de até 10 mensagens e 1 MiB, como o {@code SendMessageBatch} aceita. */
    @Override
    public BatchSendResult sendAll(List<OutgoingMessage> messages) {
        ensureOpen();
        Map<Integer, SendResult> sent = new HashMap<>();
        Map<Integer, MessagingException> failures = new HashMap<>();
        List<SendMessageBatchRequestEntry> batch = new ArrayList<>();
        long batchBytes = 0;
        for (int i = 0; i < messages.size(); i++) {
            OutgoingMessage message = messages.get(i);
            AwsCodec.Encoded encoded;
            try {
                encoded = encode(message);
            } catch (MessagingException e) {
                failures.put(i, e);
                continue;
            }
            if (batch.size() == MAX_BATCH || batchBytes + encoded.size() > MAX_MESSAGE_BYTES) {
                flush(batch, sent, failures);
                batchBytes = 0;
            }
            batch.add(SendMessageBatchRequestEntry.builder()
                    .id(Integer.toString(i))
                    .messageBody(encoded.body())
                    .messageAttributes(attributes(encoded.attributes()))
                    .messageGroupId(message.orderingKey())
                    .messageDeduplicationId(fifo ? message.deduplicationId() : null)
                    .build());
            batchBytes += encoded.size();
        }
        flush(batch, sent, failures);
        return new BatchSendResult(sent, failures);
    }

    private void flush(List<SendMessageBatchRequestEntry> batch, Map<Integer, SendResult> sent,
                       Map<Integer, MessagingException> failures) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            SendMessageBatchResponse response = sqs.sendMessageBatch(b -> b.queueUrl(queueUrl).entries(batch));
            for (SendMessageBatchResultEntry ok : response.successful()) {
                sent.put(Integer.parseInt(ok.id()), new SendResult(ok.messageId()));
            }
            for (BatchResultErrorEntry failed : response.failed()) {
                failures.put(Integer.parseInt(failed.id()), AwsErrors.map("SendMessageBatch " + queueUrl,
                        failed.code(), failed.message(), Boolean.TRUE.equals(failed.senderFault()) ? 400 : 500, null));
            }
        } catch (SdkException e) {
            MessagingException mapped = AwsErrors.map("SendMessageBatch " + queueUrl, e);
            batch.forEach(entry -> failures.put(Integer.parseInt(entry.id()), mapped));
        }
        batch.clear();
    }

    /** {@code GetQueueAttributes(QueueArn)}: exige {@code sqs:GetQueueAttributes}. */
    @Override
    public void checkAccess() {
        ensureOpen();
        try {
            sqs.getQueueAttributes(b -> b.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN));
        } catch (SdkException e) {
            throw AwsErrors.map("GetQueueAttributes " + queueUrl, e);
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(MAX_MESSAGE_BYTES, MAX_BATCH, false, Duration.ZERO, false, fifo, fifo, false);
    }

    @Override
    public void close() {
        closed = true;
    }

    private AwsCodec.Encoded encode(OutgoingMessage message) {
        if (fifo && message.orderingKey() == null) {
            throw new IllegalArgumentException("Fila FIFO exige orderingKey (MessageGroupId): " + queueUrl);
        }
        return AwsCodec.encode(message, MAX_MESSAGE_BYTES);
    }

    static Map<String, MessageAttributeValue> attributes(Map<String, String> attributes) {
        Map<String, MessageAttributeValue> result = new HashMap<>();
        attributes.forEach((key, value) -> result.put(key,
                MessageAttributeValue.builder().dataType(AwsCodec.STRING).stringValue(value).build()));
        return result;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("SqsMessageSender fechado: " + queueUrl);
        }
    }
}
