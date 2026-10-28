package com.example.messaging.rabbitmq;

import com.example.messaging.Capabilities;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Envio para uma fila ou um exchange do RabbitMQ (ADR-0009), com publisher confirms.
 *
 * <ul>
 *   <li>Fila: exchange padrão com routing key igual ao nome da fila e {@code mandatory}. Uma fila
 *       inexistente volta como {@code NO_ROUTE} e vira {@link DestinationNotFoundException}; sem
 *       {@code mandatory}, o broker descartaria a mensagem em silêncio.</li>
 *   <li>Exchange (tópico): routing key fixa na construção ou, em
 *       {@link #forExchangeByOrderingKey}, o {@code orderingKey} de cada mensagem; sem
 *       {@code mandatory}. Exchange sem fila ligada descarta a mensagem, como um tópico sem
 *       subscriptions.</li>
 *   <li>Um channel por sender, usado por uma thread de cada vez. Paralelismo de envio = mais
 *       senders sobre a mesma {@link Connection}.</li>
 *   <li>Confirm que não chega no prazo ({@link #DEFAULT_CONFIRM_TIMEOUT}, ou o de
 *       {@link #withConfirmTimeout}; alarme de memória ou disco no broker,
 *       por exemplo) lança {@code MessagingException} com {@code retryable = true}.</li>
 * </ul>
 */
public final class RabbitMessageSender implements MessageSender {

    /** {@code max_message_size} padrão do RabbitMQ 4.x (16 MiB). */
    public static final long DEFAULT_MAX_MESSAGE_BYTES = 16L * 1024 * 1024;
    public static final Duration DEFAULT_CONFIRM_TIMEOUT = Duration.ofSeconds(30);

    private final Connection connection;
    private final String exchange;
    /** {@code null}: routing key de cada envio é o {@code orderingKey} da mensagem. */
    private final String routingKey;
    private final boolean mandatory;
    private final long maxMessageBytes;
    private final Duration confirmTimeout;
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<String> returned = ConcurrentHashMap.newKeySet();
    private Channel channel;
    private volatile boolean closed;

    private RabbitMessageSender(Connection connection, String exchange, String routingKey, boolean mandatory,
                                long maxMessageBytes, Duration confirmTimeout) {
        this.connection = connection;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.mandatory = mandatory;
        this.maxMessageBytes = maxMessageBytes;
        this.confirmTimeout = confirmTimeout;
    }

    /** A conexão continua de quem a criou: {@link #close()} não a fecha. */
    public static RabbitMessageSender forQueue(Connection connection, String queue, long maxMessageBytes) {
        return new RabbitMessageSender(connection, "", queue, true, maxMessageBytes, DEFAULT_CONFIRM_TIMEOUT);
    }

    /** @param routingKey routing key de todo envio; {@code ""} para fanout e headers exchange */
    public static RabbitMessageSender forExchange(Connection connection, String exchange, String routingKey,
                                                  long maxMessageBytes) {
        return new RabbitMessageSender(connection, exchange, routingKey, false, maxMessageBytes, DEFAULT_CONFIRM_TIMEOUT);
    }

    /**
     * Routing key de cada envio = {@code orderingKey}, para direct ou consistent-hash exchange.
     * Envio sem {@code orderingKey} lança {@link IllegalArgumentException}, como no SQS FIFO.
     * {@code orderedDelivery} continua {@code false}: a ordem depende de um consumidor por fila,
     * que é topologia.
     */
    public static RabbitMessageSender forExchangeByOrderingKey(Connection connection, String exchange,
                                                               long maxMessageBytes) {
        return new RabbitMessageSender(connection, exchange, null, false, maxMessageBytes, DEFAULT_CONFIRM_TIMEOUT);
    }

    /**
     * Cópia com outro prazo de espera do confirm, para caber no timeout de quem chama. Chame antes
     * do primeiro envio: a cópia abre o próprio channel.
     */
    public RabbitMessageSender withConfirmTimeout(Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("confirmTimeout precisa ser positivo: " + timeout);
        }
        return new RabbitMessageSender(connection, exchange, routingKey, mandatory, maxMessageBytes, timeout);
    }

    @Override
    public SendResult send(OutgoingMessage message) {
        ensureOpen();
        String key = routingKey != null ? routingKey : message.orderingKey();
        if (key == null) {
            throw new IllegalArgumentException("Exchange roteado por orderingKey exige orderingKey: " + exchange);
        }
        RabbitCodec.Encoded encoded = RabbitCodec.encode(message, maxMessageBytes);
        String id = encoded.properties().getMessageId();
        lock.lock();
        try {
            ensureOpen();
            Channel open = channel();
            open.basicPublish(exchange, key, mandatory, encoded.properties(), encoded.body());
            // o basic.return chega antes do confirm (F9)
            if (!awaitConfirm(open)) {
                throw new IOException("nack do broker");
            }
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            throw RabbitErrors.map("basicPublish " + destination(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RabbitErrors.map("basicPublish " + destination(), new IOException("interrompido", e));
        } finally {
            lock.unlock();
        }
        if (returned.remove(id)) {
            throw new DestinationNotFoundException(RabbitErrors.PROVIDER, "basicPublish " + destination()
                    + ": NO_ROUTE, a fila não existe", null);
        }
        return new SendResult(id);
    }

    /** {@code queueDeclarePassive} ou {@code exchangeDeclarePassive} num channel descartável (o 404 o fecha). */
    @Override
    public void checkAccess() {
        ensureOpen();
        try (Channel check = connection.createChannel()) {
            if (exchange.isEmpty()) {
                check.queueDeclarePassive(routingKey);
            } else {
                check.exchangeDeclarePassive(exchange);
            }
        } catch (IOException | TimeoutException | ShutdownSignalException e) {
            throw RabbitErrors.map("checkAccess " + destination(), e);
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(maxMessageBytes, 1, false, Duration.ZERO, false, false, false, false);
    }

    @Override
    public void close() {
        closed = true;
        lock.lock();
        try {
            abortQuietly(channel);
            channel = null;
        } finally {
            lock.unlock();
        }
    }

    /** Channel aberto, recriado depois que um erro do broker o fechou. Chamado com o lock. */
    private Channel channel() throws IOException {
        if (channel == null || !channel.isOpen()) {
            Channel created = connection.createChannel();
            created.confirmSelect();
            created.addReturnListener(r -> returned.add(r.getProperties().getMessageId()));
            channel = created;
        }
        return channel;
    }

    /**
     * Sem confirm no prazo ou com nack, o channel sai do sender e é abortado em outra thread:
     * {@code waitForConfirmsOrDie} e {@code abort} esperam o close-ok, que um broker em alarme de
     * memória ou disco não manda, e o envio passaria do prazo. Chamado com o lock.
     */
    private boolean awaitConfirm(Channel open) throws InterruptedException, TimeoutException {
        boolean acked;
        try {
            acked = open.waitForConfirms(confirmTimeout.toMillis());
        } catch (TimeoutException e) {
            discard(open);
            throw e;
        }
        if (!acked) {
            discard(open);
        }
        return acked;
    }

    private void discard(Channel open) {
        channel = null;
        Thread.ofVirtual().name("rabbit-sender-abort").start(() -> abortQuietly(open));
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

    private String destination() {
        return exchange.isEmpty() ? routingKey : exchange + "/" + (routingKey != null ? routingKey : "<orderingKey>");
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("RabbitMessageSender fechado: " + destination());
        }
    }
}
