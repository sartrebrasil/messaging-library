package com.example.messaging.aws;

import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.PublishBatchResultEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Publicação num tópico SNS (SDK v2, cliente síncrono). Quem recebe é uma fila SQS inscrita no
 * tópico, lida com {@link SqsMessageReceiver}; a subscription precisa de
 * {@code RawMessageDelivery=true}, senão o SNS embrulha corpo e atributos num JSON (ADR-0003).
 *
 * <p>O limite de tamanho é o {@code MaximumMessageSize} do tópico: 256 KiB por padrão, até 1 MiB.
 * Tópico FIFO ({@code .fifo}) exige {@code orderingKey}.</p>
 */
public final class SnsMessageSender implements MessageSender {

    public static final long DEFAULT_MAX_MESSAGE_BYTES = 256 * 1024;
    static final int MAX_BATCH = 10;

    private final SnsClient sns;
    private final String topicArn;
    private final long maxMessageBytes;
    private final boolean fifo;
    private volatile boolean closed;

    public SnsMessageSender(SnsClient sns, String topicArn) {
        this(sns, topicArn, DEFAULT_MAX_MESSAGE_BYTES);
    }

    /** @param maxMessageBytes o {@code MaximumMessageSize} configurado no tópico */
    public SnsMessageSender(SnsClient sns, String topicArn, long maxMessageBytes) {
        this.sns = sns;
        this.topicArn = topicArn;
        this.maxMessageBytes = maxMessageBytes;
        this.fifo = AwsCodec.isFifo(topicArn);
    }

    @Override
    public SendResult send(OutgoingMessage message) {
        ensureOpen();
        AwsCodec.Encoded encoded = encode(message);
        try {
            String id = sns.publish(b -> b
                    .topicArn(topicArn)
                    .message(encoded.body())
                    .messageGroupId(message.orderingKey())
                    .messageDeduplicationId(fifo ? message.deduplicationId() : null)
                    .messageAttributes(attributes(encoded.attributes()))
            ).messageId();
            return new SendResult(id);
        } catch (SdkException e) {
            throw AwsErrors.map("Publish " + topicArn, e);
        }
    }

    /** Lotes de até 10 mensagens e {@code maxMessageBytes}, como o {@code PublishBatch} aceita. */
    @Override
    public BatchSendResult sendAll(List<OutgoingMessage> messages) {
        ensureOpen();
        Map<Integer, SendResult> sent = new HashMap<>();
        Map<Integer, MessagingException> failures = new HashMap<>();
        List<PublishBatchRequestEntry> batch = new ArrayList<>();
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
            if (batch.size() == MAX_BATCH || batchBytes + encoded.size() > maxMessageBytes) {
                flush(batch, sent, failures);
                batchBytes = 0;
            }
            batch.add(PublishBatchRequestEntry.builder()
                    .id(Integer.toString(i))
                    .message(encoded.body())
                    .messageAttributes(attributes(encoded.attributes()))
                    .messageGroupId(message.orderingKey())
                    .messageDeduplicationId(fifo ? message.deduplicationId() : null)
                    .build());
            batchBytes += encoded.size();
        }
        flush(batch, sent, failures);
        return new BatchSendResult(sent, failures);
    }

    private void flush(List<PublishBatchRequestEntry> batch, Map<Integer, SendResult> sent,
                       Map<Integer, MessagingException> failures) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            PublishBatchResponse response = sns.publishBatch(b -> b.topicArn(topicArn).publishBatchRequestEntries(batch));
            for (PublishBatchResultEntry ok : response.successful()) {
                sent.put(Integer.parseInt(ok.id()), new SendResult(ok.messageId()));
            }
            for (BatchResultErrorEntry failed : response.failed()) {
                failures.put(Integer.parseInt(failed.id()), AwsErrors.map("PublishBatch " + topicArn,
                        failed.code(), failed.message(), Boolean.TRUE.equals(failed.senderFault()) ? 400 : 500, null));
            }
        } catch (SdkException e) {
            MessagingException mapped = AwsErrors.map("PublishBatch " + topicArn, e);
            batch.forEach(entry -> failures.put(Integer.parseInt(entry.id()), mapped));
        }
        batch.clear();
    }

    /** {@code GetTopicAttributes}: exige {@code sns:GetTopicAttributes}, além de {@code sns:Publish}. */
    @Override
    public void checkAccess() {
        ensureOpen();
        try {
            sns.getTopicAttributes(b -> b.topicArn(topicArn));
        } catch (SdkException e) {
            throw AwsErrors.map("GetTopicAttributes " + topicArn, e);
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(maxMessageBytes, MAX_BATCH, false, Duration.ZERO, false, fifo, fifo, false);
    }

    @Override
    public void close() {
        closed = true;
    }

    private AwsCodec.Encoded encode(OutgoingMessage message) {
        if (fifo && message.orderingKey() == null) {
            throw new IllegalArgumentException("Tópico FIFO exige orderingKey (MessageGroupId): " + topicArn);
        }
        return AwsCodec.encode(message, maxMessageBytes);
    }

    private static Map<String, MessageAttributeValue> attributes(Map<String, String> attributes) {
        Map<String, MessageAttributeValue> result = new HashMap<>();
        attributes.forEach((key, value) -> result.put(key,
                MessageAttributeValue.builder().dataType(AwsCodec.STRING).stringValue(value).build()));
        return result;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("SnsMessageSender fechado: " + topicArn);
        }
    }
}
