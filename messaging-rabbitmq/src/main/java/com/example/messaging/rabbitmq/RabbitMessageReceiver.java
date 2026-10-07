    package com.example.messaging.rabbitmq;

import com.example.messaging.Capabilities;
import com.example.messaging.DeadLetterInfo;
import com.example.messaging.KeepAlive;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.LeaseKeeper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Recebimento de uma fila do RabbitMQ (AMQP 0-9-1) com lease controlado pela lib (ADR-0009). Uma
 * subscription de exchange é a fila ligada a ele.
 *
 * <ul>
 *   <li>Um channel por receiver e pull de verdade ({@code basicGet}): sem consumer registrado, um
 *       receiver parado não segura mensagens, e a mensagem devolvida no fim do lease fica para
 *       qualquer receiver (com um consumer, o broker a mandaria de volta ao buffer do mesmo, F9).
 *       Fila vazia: novo {@code basicGet} a cada {@link #POLL_INTERVAL} até {@code maxWait}.</li>
 *   <li>O broker não tem lease: a mensagem fica com o channel até ele fechar. O receiver conta o
 *       prazo ({@code lease}) e, quando ele vence, faz {@code basicNack(requeue)}. {@code extendLease}
 *       só move o prazo local, e {@code ack} depois do prazo sempre lança
 *       {@link LeaseExpiredException}.</li>
 *   <li>O {@code consumer_timeout} do broker (padrão 30 min) precisa passar do maior
 *       {@code maxTotal} de {@code keepAlive}; senão o broker fecha o channel e devolve tudo.</li>
 *   <li>{@code nack} é sempre imediato: o RabbitMQ não tem atraso nativo
 *       ({@link Capabilities#delayedRedelivery()} é {@code false}).</li>
 *   <li>{@code deliveryCount} só em quorum queue ({@code x-delivery-count}); em classic queue, 1 na
 *       primeira entrega e desconhecido depois. No RabbitMQ 4.x, a quorum queue tem
 *       {@code delivery-limit} 20 por padrão: depois disso a mensagem vai para a DLX ou é descartada.</li>
 *   <li>{@code deadLetter} envia uma cópia ao {@code deadLetterSender} e depois confirma; um
 *       {@code basicReject} para a DLX perderia o motivo.</li>
 *   <li>Channel fechado (queda, reconexão automática, erro do broker): as mensagens pendentes já
 *       voltaram à fila e suas confirmações lançam {@link LeaseExpiredException}.</li>
 * </ul>
 */
public final class RabbitMessageReceiver implements MessageReceiver {

    public static final Duration DEFAULT_LEASE = Duration.ofSeconds(60);
    // ponytail: polling fixo; um consumer com basicCancel no fim do receive dá latência menor
    static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private final Connection connection;
    private final String queue;
    private final Duration lease;
    private final MessageSender deadLetterSender;
    private final LeaseKeeper keeper = new LeaseKeeper(this::extendLease);
    private final ReentrantLock lock = new ReentrantLock();
    /** Delivery tag → vencimento, das mensagens entregues por {@code receive} e não confirmadas. */
    private final Map<Long, Instant> pending = new ConcurrentHashMap<>();
    /** Muda a cada channel fechado: tags de antes não valem mais. */
    private final AtomicLong epoch = new AtomicLong();
    private final ScheduledExecutorService expirer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("messaging-rabbitmq-lease-", 0).factory());
    private Channel channel;
    private long channelEpoch = -1;
    private volatile boolean closed;

    public RabbitMessageReceiver(Connection connection, String queue) {
        this(connection, queue, DEFAULT_LEASE, null);
    }

    /**
     * A conexão continua de quem a criou: {@link #close()} não a fecha.
     *
     * @param lease            prazo de cada mensagem até voltar à fila
     * @param deadLetterSender destino de {@link #deadLetter}; {@code null} sem DLQ
     */
    public RabbitMessageReceiver(Connection connection, String queue, Duration lease,
                                 MessageSender deadLetterSender) {
        if (lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("lease precisa ser positivo: " + lease);
        }
        this.connection = connection;
        this.queue = queue;
        this.lease = lease;
        this.deadLetterSender = deadLetterSender;
    }

    private record Handle(RabbitMessageReceiver owner, long epoch, long tag) {
    }

    @FunctionalInterface
    private interface Settle {
        void run(Channel channel, long tag) throws IOException;
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
        List<ReceivedMessage> result = new ArrayList<>();
        long deadline = System.nanoTime() + maxWait.toNanos();
        while (true) {
            lock.lock();
            try {
                // close() durante o sleep: sem isso, channel() abriria outro channel, que ninguém fecha
                if (closed) {
                    return result;
                }
                Channel open = channel();
                while (result.size() < maxMessages) {
                    GetResponse response = open.basicGet(queue, false);
                    if (response == null) {
                        break;
                    }
                    result.add(take(response));
                }
            } catch (IOException | ShutdownSignalException e) {
                throw RabbitErrors.map("basicGet " + queue, e);
            } finally {
                lock.unlock();
            }
            long remaining = deadline - System.nanoTime();
            if (!result.isEmpty() || remaining <= 0) {
                return result;
            }
            try {
                Thread.sleep(Duration.ofNanos(Math.min(remaining, POLL_INTERVAL.toNanos())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return result;
            }
        }
    }

    /** Channel aberto deste receiver; outro no lugar se o anterior fechou. Chamado com o lock. */
    private Channel channel() throws IOException {
        if (channel != null && channel.isOpen() && channelEpoch == epoch.get()) {
            return channel;
        }
        // a reconexão automática reabriria o channel antigo: ele precisa sumir
        abortQuietly(channel);
        pending.clear();
        long current = epoch.incrementAndGet();
        Channel created = connection.createChannel();
        // Sem lock aqui: roda na thread da conexão, que um channel RPC com o lock pode estar
        // esperando. Só invalida se ainda for o channel atual (o abort acima chega depois).
        created.addShutdownListener(cause -> epoch.compareAndSet(current, current + 1));
        channel = created;
        channelEpoch = current;
        return created;
    }

    /** Chamado com o lock. */
    private ReceivedMessage take(GetResponse response) {
        Instant receivedAt = Instant.now();
        Instant expiresAt = receivedAt.plus(lease);
        long tag = response.getEnvelope().getDeliveryTag();
        pending.put(tag, expiresAt);
        scheduleExpiry(channelEpoch, tag, expiresAt);
        return RabbitCodec.decode(new Delivery(response.getEnvelope(), response.getProps(), response.getBody()),
                receivedAt, expiresAt, new Handle(this, channelEpoch, tag));
    }

    @Override
    public void ack(ReceivedMessage message) {
        Handle handle = own(message);
        keeper.release(message);
        settle(handle, "ack", (open, tag) -> open.basicAck(tag, false));
    }

    /** Sempre imediato: o atraso é ignorado ({@link Capabilities#delayedRedelivery()} é {@code false}). */
    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        Handle handle = own(message);
        if (redeliverAfter.isNegative()) {
            throw new IllegalArgumentException("Atraso negativo: " + redeliverAfter);
        }
        keeper.release(message);
        settle(handle, "nack", (open, tag) -> open.basicNack(tag, false, true));
    }

    /** Só move o prazo local: o broker não tem lease. */
    @Override
    public void extendLease(ReceivedMessage message, Duration extension) {
        Handle handle = own(message);
        lock.lock();
        try {
            Instant expiresAt = valid(handle, "extendLease");
            Instant until = Instant.now().plus(extension);
            if (until.isAfter(expiresAt)) {
                pending.put(handle.tag(), until);
                scheduleExpiry(handle.epoch(), handle.tag(), until);
            }
        } finally {
            lock.unlock();
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
            throw new UnsupportedOperationException("Receiver de " + queue + " sem sender de DLQ configurado");
        }
        OutgoingMessage copy = new OutgoingMessage(message.body(), message.attributes(), message.contentType(),
                null, null, message.traceparent(), new DeadLetterInfo(message.messageId(), reason));
        // Envio antes do ack: falha entre os dois gera duplicata na DLQ, nunca perda (ADR-0005)
        deadLetterSender.send(copy);
        ack(message);
    }

    /** {@code queueDeclarePassive} num channel descartável (o 404 o fecha). */
    @Override
    public void checkAccess() {
        ensureOpen();
        try (Channel check = connection.createChannel()) {
            check.queueDeclarePassive(queue);
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            throw RabbitErrors.map("checkAccess " + queue, e);
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES, 1, false, Duration.ZERO, false, false,
                false, true);
    }

    /** Fecha o channel: as mensagens pendentes voltam à fila. Não fecha a conexão. */
    @Override
    public void close() {
        closed = true;
        keeper.close();
        expirer.shutdownNow();
        lock.lock();
        try {
            abortQuietly(channel);
            channel = null;
            pending.clear();
        } finally {
            lock.unlock();
        }
    }

    /** Confirma ou devolve uma mensagem ainda pendente e dentro do prazo. */
    private void settle(Handle handle, String operation, Settle action) {
        lock.lock();
        try {
            valid(handle, operation);
            // remove antes: um tag confirmado duas vezes derrubaria o channel (406)
            pending.remove(handle.tag());
            action.run(channel, handle.tag());
        } catch (IOException | ShutdownSignalException e) {
            throw RabbitErrors.mapSettle(operation + " " + queue, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Vencimento da mensagem, se ela ainda é deste channel e está no prazo. Chamado com o lock.
     *
     * @throws LeaseExpiredException channel fechado, mensagem já devolvida ou prazo vencido
     */
    private Instant valid(Handle handle, String operation) {
        Instant expiresAt = pending.get(handle.tag());
        if (handle.epoch() != epoch.get() || channel == null || !channel.isOpen() || expiresAt == null) {
            throw new LeaseExpiredException(RabbitErrors.PROVIDER, operation + " " + queue
                    + ": lease vencido, a mensagem já voltou à fila", null);
        }
        if (!Instant.now().isBefore(expiresAt)) {
            requeue(handle.tag());
            throw new LeaseExpiredException(RabbitErrors.PROVIDER, operation + " " + queue + ": lease vencido", null);
        }
        return expiresAt;
    }

    private void scheduleExpiry(long tagEpoch, long tag, Instant at) {
        long delay = Math.max(0, Duration.between(Instant.now(), at).toMillis());
        try {
            expirer.schedule(() -> Thread.startVirtualThread(() -> expire(tagEpoch, tag)), delay,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // receiver fechado: o close devolve as mensagens
        }
    }

    /** Devolve a mensagem se o prazo venceu; um prazo estendido tem a própria verificação. */
    private void expire(long tagEpoch, long tag) {
        lock.lock();
        try {
            Instant expiresAt = pending.get(tag);
            if (tagEpoch == epoch.get() && expiresAt != null && !Instant.now().isBefore(expiresAt)) {
                requeue(tag);
            }
        } finally {
            lock.unlock();
        }
    }

    /** {@code basicNack(requeue)} de um tag pendente. Chamado com o lock. */
    private void requeue(long tag) {
        if (pending.remove(tag) == null || channel == null || !channel.isOpen()) {
            return;
        }
        try {
            channel.basicNack(tag, false, true);
        } catch (IOException | ShutdownSignalException ignored) {
            // channel caiu: o broker já devolveu a mensagem
        }
    }

    /** {@code abort} não lança por channel já fechado, mas declara {@link IOException}. */
    private static void abortQuietly(Channel open) {
        if (open != null && open.isOpen()) {
            try {
                open.abort();
            } catch (IOException ignored) {
                // fechar não deve falhar o close
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
            throw new IllegalStateException("RabbitMessageReceiver fechado: " + queue);
        }
    }
}
