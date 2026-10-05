package com.example.messaging.azure;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.messaging.servicebus.models.SubQueue;
import com.example.messaging.AccessDeniedException;
import com.example.messaging.Capabilities;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessagingException;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.LeaseKeeper;
import com.example.messaging.spi.ReservedAttributes;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Recebimento de uma fila ou subscription do Service Bus sem sessions, em {@code PEEK_LOCK}
 * (cliente síncrono). Para entidade com sessions, use {@link ServiceBusSessionMessageReceiver}.
 *
 * <p>O cliente recebido no construtor precisa estar em {@code PEEK_LOCK}, com
 * {@code maxAutoLockRenewDuration(Duration.ZERO)} e {@code prefetchCount(0)}: renovação automática
 * do SDK esconderia o lease, e prefetch deixaria mensagens com o lock correndo fora do
 * {@code receive}. Os métodos {@code for*} já criam o cliente assim.</p>
 *
 * <p>{@code nack} conforme {@link Redelivery} (ADR-0005). {@code deadLetter} é nativo; a DLQ é lida
 * com {@link #forDeadLetterQueue} ou {@link #forSubscriptionDeadLetterQueue}.</p>
 */
public final class ServiceBusMessageReceiver implements MessageReceiver {

    /** O Service Bus não documenta horizonte de agendamento; a lib limita o atraso a 7 dias. */
    static final Duration MAX_RESCHEDULE_DELAY = Duration.ofDays(7);

    /** Como {@code nack} trata o atraso, já que o Service Bus não tem devolução com atraso. */
    public enum Redelivery {
        /** {@code abandon}: devolução imediata, atraso ignorado. */
        ABANDON,
        /**
         * Cópia agendada para depois do atraso e {@code complete} da original, nessa ordem: falha
         * entre os dois gera duplicata, nunca perda. A cópia ganha {@code MessageId} novo (senão a
         * duplicate detection a descartaria) e carrega o contador de entregas em
         * {@code delivery_count}. Só para filas: reenviar a um tópico entregaria a todas as subscriptions.
         */
        RESCHEDULE
    }

    private final ServiceBusReceiverClient receiver;
    private final Redelivery redelivery;
    private final ServiceBusSenderClient rescheduleSender;
    private final long maxMessageBytes;
    private final boolean ownsClients;
    private final LeaseKeeper keeper = new LeaseKeeper(this::extendLease);
    private volatile boolean closed;
    /** Destino inexistente ou sem acesso: o SDK encerra o cliente e as chamadas seguintes só dizem "terminated". */
    private volatile MessagingException terminal;

    /**
     * Com {@link Redelivery#ABANDON}. Os clientes continuam de quem os criou.
     */
    public ServiceBusMessageReceiver(ServiceBusReceiverClient receiver, long maxMessageBytes) {
        this(receiver, Redelivery.ABANDON, null, maxMessageBytes, false);
    }

    /**
     * Com {@link Redelivery#RESCHEDULE}: {@code rescheduleSender} envia para a mesma fila.
     */
    public ServiceBusMessageReceiver(ServiceBusReceiverClient receiver, ServiceBusSenderClient rescheduleSender,
                                     long maxMessageBytes) {
        this(receiver, Redelivery.RESCHEDULE, rescheduleSender, maxMessageBytes, false);
    }

    private ServiceBusMessageReceiver(ServiceBusReceiverClient receiver, Redelivery redelivery,
                                      ServiceBusSenderClient rescheduleSender, long maxMessageBytes,
                                      boolean ownsClients) {
        if (redelivery == Redelivery.RESCHEDULE && rescheduleSender == null) {
            throw new IllegalArgumentException("RESCHEDULE exige o sender da mesma fila");
        }
        this.receiver = receiver;
        this.redelivery = redelivery;
        this.rescheduleSender = rescheduleSender;
        this.maxMessageBytes = maxMessageBytes;
        this.ownsClients = ownsClients;
    }

    public static ServiceBusMessageReceiver forQueue(ServiceBusClientBuilder builder, String queue,
                                                     Redelivery redelivery, long maxMessageBytes) {
        ServiceBusReceiverClient client = configure(builder.receiver().queueName(queue)).buildClient();
        ServiceBusSenderClient sender = redelivery == Redelivery.RESCHEDULE
                ? builder.sender().queueName(queue).buildClient() : null;
        return new ServiceBusMessageReceiver(client, redelivery, sender, maxMessageBytes, true);
    }

    public static ServiceBusMessageReceiver forSubscription(ServiceBusClientBuilder builder, String topic,
                                                            String subscription, long maxMessageBytes) {
        ServiceBusReceiverClient client = configure(builder.receiver().topicName(topic)
                .subscriptionName(subscription)).buildClient();
        return new ServiceBusMessageReceiver(client, Redelivery.ABANDON, null, maxMessageBytes, true);
    }

    /** Lê a DLQ da fila; {@link ReceivedMessage#deadLetter()} traz o motivo. */
    public static ServiceBusMessageReceiver forDeadLetterQueue(ServiceBusClientBuilder builder, String queue,
                                                               long maxMessageBytes) {
        ServiceBusReceiverClient client = configure(builder.receiver().queueName(queue)
                .subQueue(SubQueue.DEAD_LETTER_QUEUE)).buildClient();
        return new ServiceBusMessageReceiver(client, Redelivery.ABANDON, null, maxMessageBytes, true);
    }

    public static ServiceBusMessageReceiver forSubscriptionDeadLetterQueue(ServiceBusClientBuilder builder, String topic,
                                                                           String subscription, long maxMessageBytes) {
        ServiceBusReceiverClient client = configure(builder.receiver().topicName(topic).subscriptionName(subscription)
                .subQueue(SubQueue.DEAD_LETTER_QUEUE)).buildClient();
        return new ServiceBusMessageReceiver(client, Redelivery.ABANDON, null, maxMessageBytes, true);
    }

    static ServiceBusClientBuilder.ServiceBusReceiverClientBuilder configure(
            ServiceBusClientBuilder.ServiceBusReceiverClientBuilder builder) {
        return builder.receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                .disableAutoComplete()
                .maxAutoLockRenewDuration(Duration.ZERO)
                .prefetchCount(0);
    }

    private record Handle(ServiceBusMessageReceiver owner, ServiceBusReceivedMessage message) {
    }

    @Override
    public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
        ensureOpen();
        validate(maxMessages, maxWait);
        try {
            Instant receivedAt = Instant.now();
            List<ReceivedMessage> result = new ArrayList<>();
            for (ServiceBusReceivedMessage message : receiver.receiveMessages(maxMessages, positive(maxWait))) {
                result.add(ServiceBusCodec.decode(message, receivedAt, new Handle(this, message)));
            }
            return result;
        } catch (RuntimeException e) {
            throw remember(AzureErrors.map("receiveMessages " + receiver.getEntityPath(), e));
        }
    }

    @Override
    public void ack(ReceivedMessage message) {
        Handle handle = own(message);
        keeper.release(message);
        try {
            receiver.complete(handle.message());
        } catch (RuntimeException e) {
            throw AzureErrors.map("complete " + receiver.getEntityPath(), e);
        }
    }

    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        Handle handle = own(message);
        if (redeliverAfter.isNegative()) {
            throw new IllegalArgumentException("Atraso negativo: " + redeliverAfter);
        }
        if (redelivery == Redelivery.RESCHEDULE && redeliverAfter.compareTo(MAX_RESCHEDULE_DELAY) > 0) {
            throw new IllegalArgumentException("Atraso acima de " + MAX_RESCHEDULE_DELAY + ": " + redeliverAfter);
        }
        keeper.release(message);
        try {
            if (redelivery == Redelivery.ABANDON || redeliverAfter.isZero()) {
                receiver.abandon(handle.message());
                return;
            }
            ServiceBusMessage copy = new ServiceBusMessage(handle.message());
            copy.setMessageId(UUID.randomUUID().toString());
            copy.getApplicationProperties().put(ReservedAttributes.DELIVERY_COUNT,
                    Integer.toString(message.deliveryCount().orElse(1)));
            rescheduleSender.scheduleMessage(copy, OffsetDateTime.now(ZoneOffset.UTC).plus(redeliverAfter));
            receiver.complete(handle.message());
        } catch (RuntimeException e) {
            throw AzureErrors.map("nack " + receiver.getEntityPath(), e);
        }
    }

    /** {@code renewMessageLock}: estende pelo lock duration da entidade, não por {@code lease}. */
    @Override
    public void extendLease(ReceivedMessage message, Duration lease) {
        Handle handle = own(message);
        try {
            receiver.renewMessageLock(handle.message());
        } catch (RuntimeException e) {
            throw AzureErrors.map("renewMessageLock " + receiver.getEntityPath(), e);
        }
    }

    @Override
    public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
        own(message);
        return keeper.keep(message, maxTotal);
    }

    @Override
    public void deadLetter(ReceivedMessage message, String reason) {
        Handle handle = own(message);
        keeper.release(message);
        try {
            receiver.deadLetter(handle.message(), new DeadLetterOptions().setDeadLetterReason(reason));
        } catch (RuntimeException e) {
            throw AzureErrors.map("deadLetter " + receiver.getEntityPath(), e);
        }
    }

    /** {@code peekMessage}: sem lock e sem efeito (exige Listen). */
    @Override
    public void checkAccess() {
        ensureOpen();
        try {
            receiver.peekMessage();
        } catch (RuntimeException e) {
            throw remember(AzureErrors.map("peekMessage " + receiver.getEntityPath(), e));
        }
    }

    private MessagingException remember(MessagingException mapped) {
        if (mapped instanceof DestinationNotFoundException || mapped instanceof AccessDeniedException) {
            terminal = mapped;
        }
        return mapped;
    }

    @Override
    public Capabilities capabilities() {
        boolean delayed = redelivery == Redelivery.RESCHEDULE;
        return new Capabilities(maxMessageBytes, 100, delayed, delayed ? MAX_RESCHEDULE_DELAY : Duration.ZERO,
                true, false, true, true);
    }

    @Override
    public void close() {
        closed = true;
        keeper.close();
        if (ownsClients) {
            receiver.close();
            if (rescheduleSender != null) {
                rescheduleSender.close();
            }
        }
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
            throw new IllegalStateException("ServiceBusMessageReceiver fechado: " + receiver.getEntityPath());
        }
        if (terminal != null) {
            throw terminal;
        }
    }

    static void validate(int maxMessages, Duration maxWait) {
        if (maxMessages < 1) {
            throw new IllegalArgumentException("maxMessages precisa ser >= 1: " + maxMessages);
        }
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait negativo: " + maxWait);
        }
    }

    /** O SDK recusa espera zero. */
    static Duration positive(Duration maxWait) {
        return maxWait.isZero() ? Duration.ofMillis(1) : maxWait;
    }
}
