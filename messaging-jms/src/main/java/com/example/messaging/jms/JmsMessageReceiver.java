package com.example.messaging.jms;

import com.example.messaging.Capabilities;
import com.example.messaging.DeadLetterInfo;
import com.example.messaging.KeepAlive;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.LeaseKeeper;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Recebimento de uma fila do ActiveMQ Classic por Jakarta Messaging, com lease controlado pela
 * lib (ADR-0009). Serve também para a subscription de um Virtual Topic
 * ({@code Consumer.<s>.VirtualTopic.<t>}).
 *
 * <ul>
 *   <li>Um pool de até {@code maxInFlight} sessions {@code CLIENT_ACKNOWLEDGE}, cada uma com um
 *       consumer de prefetch 0 e no máximo uma mensagem pendente. O ack confirma só aquela
 *       mensagem, e o ack de uma não espera o {@code receive} de outra. Com o pool cheio,
 *       {@code receive} espera uma confirmação até {@code maxWait}.</li>
 *   <li>O broker não tem lease: a mensagem fica presa até a session fechar. O receiver conta o
 *       prazo ({@code lease}) e, quando ele vence, fecha a session. O broker reentrega com o mesmo
 *       {@code JMSMessageID} e soma uma entrega. {@code extendLease} só move o prazo local, e
 *       {@code ack} depois do prazo sempre lança {@link LeaseExpiredException}.</li>
 *   <li>{@code nack(0)} também fecha a session. Com {@link Redelivery#SCHEDULED}, {@code nack} com
 *       atraso envia uma cópia com {@code AMQ_SCHEDULED_DELAY} e depois confirma a original: a cópia
 *       tem {@code JMSMessageID} novo e o atributo {@code delivery_count}. Exige
 *       {@code schedulerSupport="true"} no broker; sem ele, o broker entrega a cópia na hora.</li>
 *   <li>Ordem por {@code JMSXGroupID} (message groups): vale com {@link Redelivery#IMMEDIATE}, em
 *       que toda reentrega volta à frente do grupo. A cópia agendada iria para o fim da fila.</li>
 *   <li>{@code deadLetter} envia uma cópia ao {@code deadLetterSender} e depois confirma. A DLQ do
 *       broker ({@code maximumRedeliveries}) continua valendo para reentregas por session fechada.</li>
 *   <li>Java 21: o cliente do Classic espera mensagens com {@code Object.wait} dentro de
 *       {@code synchronized}, o que prende a carrier thread de uma virtual thread durante o
 *       {@code maxWait}. No Java 24+ (JEP 491) não prende.</li>
 * </ul>
 */
public final class JmsMessageReceiver implements MessageReceiver {

    /** Como {@code nack} com atraso devolve a mensagem. */
    public enum Redelivery {
        /** Fecha a session: reentrega na hora, mesmo id, ordem do grupo mantida. O atraso é ignorado. */
        IMMEDIATE,
        /** Atraso por cópia agendada ({@code schedulerSupport} no broker); {@code nack(0)} continua imediato. */
        SCHEDULED
    }

    public static final Duration DEFAULT_LEASE = Duration.ofSeconds(60);
    public static final int DEFAULT_MAX_IN_FLIGHT = 10;
    static final Duration MAX_REDELIVERY_DELAY = Duration.ofHours(12);
    static final String SCHEDULED_DELAY = "AMQ_SCHEDULED_DELAY";

    private final Connection connection;
    private final String queue;
    private final Duration lease;
    private final int maxInFlight;
    private final Redelivery redelivery;
    private final MessageSender deadLetterSender;
    private final LeaseKeeper keeper = new LeaseKeeper(this::extendLease);
    private final BlockingQueue<Slot> idle = new LinkedBlockingQueue<>();
    private final List<Slot> slots = new CopyOnWriteArrayList<>();
    private final AtomicInteger created = new AtomicInteger();
    private final ScheduledExecutorService expirer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("messaging-jms-lease-", 0).factory());
    private volatile boolean closed;

    public JmsMessageReceiver(Connection connection, String queue) {
        this(connection, queue, DEFAULT_LEASE, DEFAULT_MAX_IN_FLIGHT, Redelivery.IMMEDIATE, null);
    }

    /**
     * A conexão continua de quem a criou: {@link #close()} não a fecha. O receiver chama
     * {@code connection.start()}.
     *
     * @param lease            prazo de cada mensagem até voltar ao broker
     * @param maxInFlight      mensagens pendentes por receiver (uma session cada)
     * @param deadLetterSender destino de {@link #deadLetter}; {@code null} sem DLQ
     */
    public JmsMessageReceiver(Connection connection, String queue, Duration lease, int maxInFlight,
                              Redelivery redelivery, MessageSender deadLetterSender) {
        if (lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("lease precisa ser positivo: " + lease);
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight precisa ser >= 1: " + maxInFlight);
        }
        this.connection = connection;
        this.queue = queue;
        this.lease = lease;
        this.maxInFlight = maxInFlight;
        this.redelivery = redelivery;
        this.deadLetterSender = deadLetterSender;
    }

    /** Uma session do pool. Campos protegidos por {@link #lock}. */
    private static final class Slot {
        final ReentrantLock lock = new ReentrantLock();
        Session session;
        MessageConsumer consumer;
        Message pending;
        Instant expiresAt;
        long generation;
    }

    private record Handle(JmsMessageReceiver owner, Slot slot, long generation) {
    }

    @FunctionalInterface
    private interface Settle {
        void run(Slot slot) throws JMSException;
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
        long deadline = System.nanoTime() + maxWait.toNanos();
        Slot slot = acquire(deadline);
        if (slot == null) {
            return List.of();
        }
        ReceivedMessage first = take(slot, deadline);
        if (first == null) {
            return List.of();
        }
        List<ReceivedMessage> result = new ArrayList<>();
        result.add(first);
        while (result.size() < Math.min(maxMessages, maxInFlight)) {
            Slot next = acquire(0);
            ReceivedMessage message = next == null ? null : take(next, 0);
            if (message == null) {
                break;
            }
            result.add(message);
        }
        return result;
    }

    /** Session livre ou nova; com o pool cheio, espera uma liberar até {@code deadline} (0 = não espera). */
    private Slot acquire(long deadline) {
        Slot slot = idle.poll();
        if (slot != null) {
            return slot;
        }
        if (created.incrementAndGet() <= maxInFlight) {
            slot = new Slot();
            slots.add(slot);
            return slot;
        }
        created.decrementAndGet();
        long remaining = deadline - System.nanoTime();
        if (deadline == 0 || remaining <= 0) {
            return null;
        }
        try {
            return idle.poll(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Recebe uma mensagem na session; sem mensagem, devolve a session ao pool. */
    private ReceivedMessage take(Slot slot, long deadline) {
        slot.lock.lock();
        try {
            if (slot.consumer == null) {
                connection.start();
                slot.session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                slot.consumer = slot.session.createConsumer(slot.session.createQueue(consumerQueue()));
            }
            long millis = deadline == 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            // receive(0) bloquearia para sempre
            Message message = millis <= 0 ? slot.consumer.receiveNoWait() : slot.consumer.receive(millis);
            if (message == null) {
                release(slot);
                return null;
            }
            Instant receivedAt = Instant.now();
            slot.pending = message;
            slot.expiresAt = receivedAt.plus(lease);
            scheduleExpiry(slot, slot.generation, slot.expiresAt);
            return JmsCodec.decode(message, receivedAt, slot.expiresAt, new Handle(this, slot, slot.generation));
        } catch (JMSException e) {
            recycle(slot);
            if (closed) {
                return null;
            }
            throw JmsErrors.map("receive " + queue, e);
        } finally {
            slot.lock.unlock();
        }
    }

    @Override
    public void ack(ReceivedMessage message) {
        Handle handle = own(message);
        keeper.release(message);
        settle(handle, "ack", slot -> {
            slot.pending.acknowledge();
            release(slot);
        });
    }

    /**
     * @throws IllegalArgumentException atraso negativo, ou acima de 12 h em {@link Redelivery#SCHEDULED}
     */
    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        Handle handle = own(message);
        if (redeliverAfter.isNegative()) {
            throw new IllegalArgumentException("Atraso negativo: " + redeliverAfter);
        }
        boolean delayed = redelivery == Redelivery.SCHEDULED && !redeliverAfter.isZero();
        if (delayed && redeliverAfter.compareTo(MAX_REDELIVERY_DELAY) > 0) {
            throw new IllegalArgumentException("Atraso acima de " + MAX_REDELIVERY_DELAY + ": " + redeliverAfter);
        }
        keeper.release(message);
        settle(handle, "nack", slot -> {
            if (!delayed) {
                recycle(slot);
                return;
            }
            BytesMessage copy = JmsCodec.copy(slot.session, message, Long.MAX_VALUE);
            copy.setLongProperty(SCHEDULED_DELAY, redeliverAfter.toMillis());
            try (MessageProducer producer = slot.session.createProducer(slot.session.createQueue(physicalQueue()))) {
                // Envio antes do ack: falha entre os dois gera duplicata, nunca perda (ADR-0005)
                producer.send(copy);
            }
            slot.pending.acknowledge();
            release(slot);
        });
    }

    /** Só move o prazo local: o broker não tem lease. */
    @Override
    public void extendLease(ReceivedMessage message, Duration extension) {
        Handle handle = own(message);
        settle(handle, "extendLease", slot -> {
            Instant until = Instant.now().plus(extension);
            if (until.isAfter(slot.expiresAt)) {
                slot.expiresAt = until;
                scheduleExpiry(slot, slot.generation, until);
            }
        });
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
            throw new UnsupportedOperationException("Receiver de " + queue + " sem sender de DLQ configurado");
        }
        OutgoingMessage copy = new OutgoingMessage(message.body(), message.attributes(), message.contentType(),
                message.orderingKey(), null, message.traceparent(), new DeadLetterInfo(message.messageId(), reason));
        // Envio antes do ack: falha entre os dois gera duplicata na DLQ, nunca perda (ADR-0005)
        deadLetterSender.send(copy);
        ack(message);
    }

    /** Lista a fila no {@code DestinationSource} do Classic, sem criá-la nem consumir (ADR-0009). */
    @Override
    public void checkAccess() {
        ensureOpen();
        ActiveMqDestinations.check(connection, physicalQueue(), false);
    }

    @Override
    public Capabilities capabilities() {
        boolean scheduled = redelivery == Redelivery.SCHEDULED;
        return new Capabilities(JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES, 1, scheduled,
                scheduled ? MAX_REDELIVERY_DELAY : Duration.ZERO, false, !scheduled, false, true);
    }

    /** Fecha as sessions: as mensagens pendentes voltam ao broker. Não fecha a conexão. */
    @Override
    public void close() {
        closed = true;
        keeper.close();
        expirer.shutdownNow();
        // close é o único método da session que o JMS permite chamar de outra thread
        for (Slot slot : slots) {
            closeQuietly(slot.session);
        }
        idle.clear();
    }

    /** Confere que a mensagem ainda é a pendente da session e que o prazo não venceu. */
    private void settle(Handle handle, String operation, Settle action) {
        Slot slot = handle.slot();
        slot.lock.lock();
        try {
            if (slot.generation != handle.generation() || slot.pending == null) {
                throw new LeaseExpiredException(JmsErrors.PROVIDER, operation + " " + queue
                        + ": lease vencido, a mensagem já voltou à fila", null);
            }
            if (!Instant.now().isBefore(slot.expiresAt)) {
                recycle(slot);
                throw new LeaseExpiredException(JmsErrors.PROVIDER, operation + " " + queue + ": lease vencido", null);
            }
            action.run(slot);
        } catch (JMSException e) {
            recycle(slot);
            throw JmsErrors.mapSettle(operation + " " + queue, e);
        } finally {
            slot.lock.unlock();
        }
    }

    private void scheduleExpiry(Slot slot, long generation, Instant at) {
        long delay = Math.max(0, Duration.between(Instant.now(), at).toMillis());
        try {
            expirer.schedule(() -> Thread.startVirtualThread(() -> expire(slot, generation)), delay,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // receiver fechado: o close devolve as mensagens
        }
    }

    /** Devolve a mensagem ao broker se o prazo venceu; um prazo estendido tem a própria verificação. */
    private void expire(Slot slot, long generation) {
        slot.lock.lock();
        try {
            if (slot.generation == generation && slot.pending != null && !Instant.now().isBefore(slot.expiresAt)) {
                recycle(slot);
            }
        } finally {
            slot.lock.unlock();
        }
    }

    /** Session livre de novo. Chamado com o lock da session. */
    private void release(Slot slot) {
        slot.pending = null;
        slot.generation++;
        if (!closed) {
            idle.offer(slot);
        }
    }

    /** Fecha a session (o broker reentrega a pendente) e devolve uma vaga ao pool. Chamado com o lock. */
    private void recycle(Slot slot) {
        closeQuietly(slot.session);
        slot.session = null;
        slot.consumer = null;
        release(slot);
    }

    private static void closeQuietly(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException ignored) {
                // a session já pode ter caído com a conexão
            }
        }
    }

    /** Prefetch 0: sem ele o broker manda mensagens a mais para o buffer do consumer, sem lease. */
    private String consumerQueue() {
        return queue + (queue.contains("?") ? "&" : "?") + "consumer.prefetchSize=0";
    }

    private String physicalQueue() {
        return queue.contains("?") ? queue.substring(0, queue.indexOf('?')) : queue;
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
            throw new IllegalStateException("JmsMessageReceiver fechado: " + queue);
        }
    }
}
