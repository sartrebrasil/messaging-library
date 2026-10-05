package com.example.messaging.azure;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.MessageSender;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Envio para uma fila ou um tópico do Service Bus (cliente síncrono).
 *
 * <p>Com {@code sessions = true} (entidade com sessions no Terraform), {@code orderingKey} vira
 * {@code SessionId} e é obrigatório. Sem sessions, {@code orderingKey} é ignorado.
 * {@code deduplicationId} vira {@code MessageId}, que a duplicate detection da entidade usa.</p>
 *
 * <p>{@code maxMessageBytes}: 256 KB no Standard; no Premium, o limite configurado na entidade
 * (1 MB por padrão).</p>
 */
public final class ServiceBusMessageSender implements MessageSender {

    public static final long STANDARD_MAX_MESSAGE_BYTES = 256 * 1024;

    private final ServiceBusSenderClient sender;
    private final boolean sessions;
    private final long maxMessageBytes;
    private final boolean ownsClient;
    private volatile boolean closed;

    /** O cliente continua de quem o criou: {@link #close()} não o fecha. */
    public ServiceBusMessageSender(ServiceBusSenderClient sender, boolean sessions, long maxMessageBytes) {
        this(sender, sessions, maxMessageBytes, false);
    }

    private ServiceBusMessageSender(ServiceBusSenderClient sender, boolean sessions, long maxMessageBytes,
                                    boolean ownsClient) {
        this.sender = sender;
        this.sessions = sessions;
        this.maxMessageBytes = maxMessageBytes;
        this.ownsClient = ownsClient;
    }

    /** Cria o cliente a partir do builder (que compartilha a conexão) e o fecha no {@link #close()}. */
    public static ServiceBusMessageSender forQueue(ServiceBusClientBuilder builder, String queue, boolean sessions,
                                                   long maxMessageBytes) {
        return new ServiceBusMessageSender(builder.sender().queueName(queue).buildClient(), sessions, maxMessageBytes,
                true);
    }

    public static ServiceBusMessageSender forTopic(ServiceBusClientBuilder builder, String topic, boolean sessions,
                                                   long maxMessageBytes) {
        return new ServiceBusMessageSender(builder.sender().topicName(topic).buildClient(), sessions, maxMessageBytes,
                true);
    }

    @Override
    public SendResult send(OutgoingMessage message) {
        ensureOpen();
        ServiceBusMessage wire = encode(message);
        try {
            sender.sendMessage(wire);
        } catch (RuntimeException e) {
            throw AzureErrors.map("send " + sender.getEntityPath(), e);
        }
        return new SendResult(wire.getMessageId());
    }

    /** Agrupa em lotes que cabem no limite de lote do provedor ({@code createMessageBatch}). */
    @Override
    public BatchSendResult sendAll(List<OutgoingMessage> messages) {
        ensureOpen();
        Map<Integer, SendResult> sent = new HashMap<>();
        Map<Integer, MessagingException> failures = new HashMap<>();
        Map<Integer, ServiceBusMessage> pending = new HashMap<>();
        ServiceBusMessageBatch batch = null;
        try {
            for (int i = 0; i < messages.size(); i++) {
                ServiceBusMessage wire;
                try {
                    wire = encode(messages.get(i));
                } catch (MessagingException e) {
                    failures.put(i, e);
                    continue;
                }
                if (batch == null) {
                    batch = sender.createMessageBatch();
                }
                if (!batch.tryAddMessage(wire)) {
                    flush(batch, pending, sent, failures);
                    batch = sender.createMessageBatch();
                    if (!batch.tryAddMessage(wire)) {
                        failures.put(i, new MessageTooLargeException(AzureErrors.PROVIDER,
                                "Mensagem não cabe num lote do Service Bus", null));
                        continue;
                    }
                }
                pending.put(i, wire);
            }
            if (batch != null) {
                flush(batch, pending, sent, failures);
            }
        } catch (RuntimeException e) {
            MessagingException mapped = AzureErrors.map("sendMessages " + sender.getEntityPath(), e);
            for (int i = 0; i < messages.size(); i++) {
                if (!sent.containsKey(i) && !failures.containsKey(i)) {
                    failures.put(i, mapped);
                }
            }
        }
        return new BatchSendResult(sent, failures);
    }

    private void flush(ServiceBusMessageBatch batch, Map<Integer, ServiceBusMessage> pending,
                       Map<Integer, SendResult> sent, Map<Integer, MessagingException> failures) {
        if (pending.isEmpty()) {
            return;
        }
        try {
            sender.sendMessages(batch);
            pending.forEach((index, wire) -> sent.put(index, new SendResult(wire.getMessageId())));
        } catch (RuntimeException e) {
            MessagingException mapped = AzureErrors.map("sendMessages " + sender.getEntityPath(), e);
            pending.keySet().forEach(index -> failures.put(index, mapped));
        }
        pending.clear();
    }

    /** {@code createMessageBatch}: abre o link e autentica sem enviar (exige Send). */
    @Override
    public void checkAccess() {
        ensureOpen();
        try {
            sender.createMessageBatch();
        } catch (RuntimeException e) {
            throw AzureErrors.map("createMessageBatch " + sender.getEntityPath(), e);
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(maxMessageBytes, 100, false, Duration.ZERO, false, sessions, true, false);
    }

    @Override
    public void close() {
        closed = true;
        if (ownsClient) {
            sender.close();
        }
    }

    private ServiceBusMessage encode(OutgoingMessage message) {
        if (sessions && message.orderingKey() == null) {
            throw new IllegalArgumentException("Entidade com sessions exige orderingKey (SessionId): "
                    + sender.getEntityPath());
        }
        return ServiceBusCodec.encode(message, sessions, maxMessageBytes);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ServiceBusMessageSender fechado: " + sender.getEntityPath());
        }
    }
}
