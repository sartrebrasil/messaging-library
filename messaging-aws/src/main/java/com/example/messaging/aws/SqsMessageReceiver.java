package com.example.messaging.aws;

import com.example.messaging.AckResult;
import com.example.messaging.Capabilities;
import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.DeadLetterInfo;
import com.example.messaging.spi.LeaseKeeper;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Recebimento de uma fila SQS (SDK v2, cliente síncrono), inclusive de fila inscrita num tópico
 * SNS com {@code RawMessageDelivery=true}.
 *
 * <ul>
 *   <li>{@code maxWait} vira {@code WaitTimeSeconds}, arredondado para cima e limitado a 20 s. O
 *       read timeout do cliente HTTP precisa ser maior que isso.</li>
 *   <li>O lease é o visibility timeout da fila, lido uma vez com {@code GetQueueAttributes}.</li>
 *   <li>{@code nack} e {@code extendLease} usam {@code ChangeMessageVisibility}: no máximo 12 h
 *       contadas do primeiro receive da mensagem.</li>
 *   <li>{@code ack} com receipt handle antigo pode responder sucesso sem apagar: a mensagem volta
 *       e quem consome precisa ser idempotente ({@link Capabilities#reportsLeaseExpiredOnAck()} é
 *       {@code false}).</li>
 *   <li>{@code deadLetter} envia uma cópia ao {@code deadLetterSender} e depois confirma; sem ele,
 *       lança {@link UnsupportedOperationException}. A DLQ por {@code maxReceiveCount} (redrive
 *       policy) continua sendo configuração da fila.</li>
 * </ul>
 */
public final class SqsMessageReceiver implements MessageReceiver {

    static final Duration MAX_VISIBILITY = Duration.ofHours(12);
    private static final int MAX_WAIT_SECONDS = 20;

    private final SqsClient sqs;
    private final String queueUrl;
    private final MessageSender deadLetterSender;
    private final boolean fifo;
    private final LeaseKeeper keeper = new LeaseKeeper(this::extendLease);
    private volatile Duration visibilityTimeout;
    private volatile boolean closed;

    public SqsMessageReceiver(SqsClient sqs, String queueUrl) {
        this(sqs, queueUrl, null);
    }

    /** @param deadLetterSender destino de {@link #deadLetter}; {@code null} sem DLQ */
    public SqsMessageReceiver(SqsClient sqs, String queueUrl, MessageSender deadLetterSender) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.deadLetterSender = deadLetterSender;
        this.fifo = AwsCodec.isFifo(queueUrl);
    }

    private record Handle(SqsMessageReceiver owner, String receiptHandle, Instant firstReceivedAt) {
    }

    @Override
    public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
        ensureOpen();
        if (maxMessages < 1) {
            throw new IllegalArgumentException("maxMessages precisa ser >= 1: " + maxMessages);
        }
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait negativo: " + maxWait);
        }
        Duration lease = visibilityTimeout();
        int waitSeconds = (int) Math.min(MAX_WAIT_SECONDS, ceilSeconds(maxWait));
        try {
            Instant receivedAt = Instant.now();
            List<Message> messages = sqs.receiveMessage(b -> b.queueUrl(queueUrl)
                    .maxNumberOfMessages(Math.min(maxMessages, SqsMessageSender.MAX_BATCH))
                    .waitTimeSeconds(waitSeconds)
                    .messageAttributeNames("All")
                    .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT,
                            MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
                            MessageSystemAttributeName.SENT_TIMESTAMP,
                            MessageSystemAttributeName.MESSAGE_GROUP_ID)).messages();
            return messages.stream().map(message -> toReceived(message, receivedAt, lease)).toList();
        } catch (SdkException e) {
            throw AwsErrors.map("ReceiveMessage " + queueUrl, e);
        }
    }

    private ReceivedMessage toReceived(Message message, Instant receivedAt, Duration lease) {
        Map<String, String> nativeAttributes = new HashMap<>();
        for (Map.Entry<String, MessageAttributeValue> attribute : message.messageAttributes().entrySet()) {
            if (attribute.getValue().stringValue() != null) {
                nativeAttributes.put(attribute.getKey(), attribute.getValue().stringValue());
            }
        }
        AwsCodec.Decoded decoded = AwsCodec.decode(message.messageId(), message.body(), nativeAttributes);
        Map<MessageSystemAttributeName, String> system = message.attributes();
        String count = system.get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
        Instant enqueuedAt = epochMillis(system.get(MessageSystemAttributeName.SENT_TIMESTAMP));
        Instant firstReceivedAt = epochMillis(system.get(MessageSystemAttributeName.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP));
        return new ReceivedMessage(message.messageId(), decoded.body(), decoded.attributes(), decoded.contentType(),
                system.get(MessageSystemAttributeName.MESSAGE_GROUP_ID),
                count == null ? OptionalInt.empty() : OptionalInt.of(Integer.parseInt(count)),
                enqueuedAt, receivedAt, receivedAt.plus(lease), decoded.traceparent(), decoded.deadLetter(),
                new Handle(this, message.receiptHandle(), firstReceivedAt == null ? receivedAt : firstReceivedAt));
    }

    @Override
    public void ack(ReceivedMessage message) {
        Handle handle = own(message);
        keeper.release(message);
        try {
            sqs.deleteMessage(b -> b.queueUrl(queueUrl).receiptHandle(handle.receiptHandle()));
        } catch (SdkException e) {
            throw AwsErrors.map("DeleteMessage " + queueUrl, e);
        }
    }

    /** Lotes de 10 com {@code DeleteMessageBatch}. */
    @Override
    public AckResult ackAll(List<ReceivedMessage> messages) {
        Map<String, MessagingException> failures = new HashMap<>();
        List<ReceivedMessage> batch = new ArrayList<>();
        for (ReceivedMessage message : messages) {
            own(message);
            keeper.release(message);
            batch.add(message);
            if (batch.size() == SqsMessageSender.MAX_BATCH) {
                deleteBatch(batch, failures);
            }
        }
        deleteBatch(batch, failures);
        return new AckResult(failures);
    }

    private void deleteBatch(List<ReceivedMessage> batch, Map<String, MessagingException> failures) {
        if (batch.isEmpty()) {
            return;
        }
        List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < batch.size(); i++) {
            entries.add(DeleteMessageBatchRequestEntry.builder().id(Integer.toString(i))
                    .receiptHandle(((Handle) batch.get(i).handle()).receiptHandle()).build());
        }
        try {
            DeleteMessageBatchResponse response = sqs.deleteMessageBatch(b -> b.queueUrl(queueUrl).entries(entries));
            for (BatchResultErrorEntry failed : response.failed()) {
                ReceivedMessage message = batch.get(Integer.parseInt(failed.id()));
                failures.put(message.messageId(), AwsErrors.map("DeleteMessageBatch " + queueUrl, failed.code(),
                        failed.message(), Boolean.TRUE.equals(failed.senderFault()) ? 400 : 500, null));
            }
        } catch (SdkException e) {
            MessagingException mapped = AwsErrors.map("DeleteMessageBatch " + queueUrl, e);
            batch.forEach(message -> failures.put(message.messageId(), mapped));
        }
        batch.clear();
    }

    /**
     * @throws IllegalArgumentException atraso negativo ou além das 12 h contadas do primeiro receive
     */
    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        Handle handle = own(message);
        if (redeliverAfter.isNegative()) {
            throw new IllegalArgumentException("Atraso negativo: " + redeliverAfter);
        }
        Duration remaining = MAX_VISIBILITY.minus(Duration.between(handle.firstReceivedAt(), Instant.now()));
        if (redeliverAfter.compareTo(remaining) > 0) {
            throw new IllegalArgumentException("Atraso " + redeliverAfter + " passa das 12 h desde o primeiro receive; "
                    + "restam " + remaining);
        }
        keeper.release(message);
        changeVisibility(handle, redeliverAfter);
    }

    @Override
    public void extendLease(ReceivedMessage message, Duration lease) {
        changeVisibility(own(message), lease);
    }

    private void changeVisibility(Handle handle, Duration timeout) {
        try {
            sqs.changeMessageVisibility(b -> b.queueUrl(queueUrl).receiptHandle(handle.receiptHandle())
                    .visibilityTimeout((int) ceilSeconds(timeout)));
        } catch (SdkException e) {
            throw AwsErrors.map("ChangeMessageVisibility " + queueUrl, e);
        }
    }

    @Override
    public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
        own(message);
        return keeper.keep(message, maxTotal);
    }

    @Override
    public void deadLetter(ReceivedMessage message, String reason) {
        own(message);
        if (deadLetterSender == null) {
            throw new UnsupportedOperationException("Receiver de " + queueUrl + " sem sender de DLQ configurado");
        }
        OutgoingMessage copy = new OutgoingMessage(message.body(), message.attributes(), message.contentType(),
                message.orderingKey(), fifo ? message.messageId() : null, message.traceparent(),
                new DeadLetterInfo(message.messageId(), reason));
        // Envio antes do ack: falha entre os dois gera duplicata na DLQ, nunca perda (ADR-0005)
        deadLetterSender.send(copy);
        ack(message);
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
        return new Capabilities(SqsMessageSender.MAX_MESSAGE_BYTES, SqsMessageSender.MAX_BATCH, true, MAX_VISIBILITY,
                false, fifo, fifo, false);
    }

    @Override
    public void close() {
        closed = true;
        keeper.close();
    }

    private Duration visibilityTimeout() {
        Duration timeout = visibilityTimeout;
        if (timeout == null) {
            try {
                String seconds = sqs.getQueueAttributes(b -> b.queueUrl(queueUrl)
                                .attributeNames(QueueAttributeName.VISIBILITY_TIMEOUT))
                        .attributes().get(QueueAttributeName.VISIBILITY_TIMEOUT);
                timeout = Duration.ofSeconds(Long.parseLong(seconds));
            } catch (SdkException e) {
                throw AwsErrors.map("GetQueueAttributes " + queueUrl, e);
            }
            visibilityTimeout = timeout;
        }
        return timeout;
    }

    private Handle own(ReceivedMessage message) {
        ensureOpen();
        if (!(message.handle() instanceof Handle handle) || handle.owner() != this) {
            throw new IllegalArgumentException("Mensagem recebida por outro receiver: " + message.messageId());
        }
        return handle;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("SqsMessageReceiver fechado: " + queueUrl);
        }
    }

    private static long ceilSeconds(Duration duration) {
        long seconds = duration.getSeconds();
        return duration.getNano() > 0 ? seconds + 1 : seconds;
    }

    private static Instant epochMillis(String value) {
        return value == null ? null : Instant.ofEpochMilli(Long.parseLong(value));
    }
}
