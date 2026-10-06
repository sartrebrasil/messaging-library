package com.example.messaging.jms;

import com.example.messaging.Capabilities;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Envio para uma fila ou um tópico do ActiveMQ Classic ou Artemis, por Jakarta Messaging (ADR-0009).
 *
 * <ul>
 *   <li>Uma session por sender, usada por uma thread de cada vez (a session JMS não é thread-safe).
 *       Paralelismo de envio = mais senders sobre a mesma {@link Connection}.</li>
 *   <li>Envio persistente e síncrono: no Classic, a connection factory não pode ter
 *       {@code useAsyncSend=true}; no Artemis, {@code blockOnDurableSend} fica no padrão ({@code true}).</li>
 *   <li>{@code orderingKey} vira {@code JMSXGroupID}; {@code deduplicationId} vira {@code _AMQ_DUPL_ID}
 *       no Artemis e é ignorado no Classic.</li>
 *   <li>Tópico: no Classic, um Virtual Topic ({@code VirtualTopic.<t>}), com subscriptions nas filas
 *       {@code Consumer.<s>.VirtualTopic.<t>}; no Artemis, um endereço multicast, com subscriptions
 *       nas filas dele ({@code <endereço>::<fila>}). Ver {@link JmsDialect#subscriptionQueue}.</li>
 * </ul>
 */
public final class JmsMessageSender implements MessageSender {

    /** {@code wireFormat.maxFrameSize} padrão do Classic (100 MiB), com folga para os headers. */
    public static final long DEFAULT_MAX_MESSAGE_BYTES = 100L * 1024 * 1024 - 64 * 1024;

    private final Connection connection;
    private final JmsDialect dialect;
    private final String name;
    private final boolean topic;
    private final long maxMessageBytes;
    private final ReentrantLock lock = new ReentrantLock();
    private Session session;
    private MessageProducer producer;
    private volatile boolean closed;

    private JmsMessageSender(Connection connection, JmsDialect dialect, String name, boolean topic,
                             long maxMessageBytes) {
        this.connection = connection;
        this.dialect = dialect;
        this.name = name;
        this.topic = topic;
        this.maxMessageBytes = maxMessageBytes;
    }

    /** A conexão continua de quem a criou: {@link #close()} não a fecha. */
    public static JmsMessageSender forQueue(Connection connection, JmsDialect dialect, String queue,
                                            long maxMessageBytes) {
        return new JmsMessageSender(connection, dialect, queue, false, maxMessageBytes);
    }

    public static JmsMessageSender forTopic(Connection connection, JmsDialect dialect, String topic,
                                            long maxMessageBytes) {
        return new JmsMessageSender(connection, dialect, topic, true, maxMessageBytes);
    }

    @Override
    public SendResult send(OutgoingMessage message) {
        ensureOpen();
        lock.lock();
        try {
            ensureOpen();
            open();
            BytesMessage wire = JmsCodec.encode(session, dialect, message, maxMessageBytes);
            producer.send(wire);
            return new SendResult(wire.getJMSMessageID());
        } catch (JMSException e) {
            reset();
            throw JmsErrors.map("send " + name, e);
        } finally {
            lock.unlock();
        }
    }

    /** Confere que o destino existe sem criá-lo (ver {@link JmsDialect}). */
    @Override
    public void checkAccess() {
        ensureOpen();
        dialect.checkAccess(connection, name, topic);
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(maxMessageBytes, 1, false, Duration.ZERO, false, true, dialect.deduplicates(), false);
    }

    @Override
    public void close() {
        closed = true;
        lock.lock();
        try {
            reset();
        } finally {
            lock.unlock();
        }
    }

    private void open() throws JMSException {
        if (session == null) {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = topic ? session.createTopic(name) : session.createQueue(name);
            producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        }
    }

    /** Descarta a session depois de um erro; a próxima chamada abre outra. */
    private void reset() {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException ignored) {
                // a session já pode ter caído com a conexão
            }
            session = null;
            producer = null;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("JmsMessageSender fechado: " + name);
        }
    }
}
