package com.example.messaging.spring.jms;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.jms.JmsDialect;
import com.example.messaging.jms.JmsMessageReceiver;
import com.example.messaging.jms.JmsMessageSender;
import com.example.messaging.spring.ClientBeans;
import com.example.messaging.spring.MessagingProperties;
import com.example.messaging.spring.ProviderConnection;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.springframework.beans.factory.ListableBeanFactory;

/**
 * ActiveMQ Classic e Artemis (ADR-0009). Uma {@link Connection} por provider, compartilhada pelos senders e
 * receivers, criada pelo bean {@link ConnectionFactory} da aplicação (ADR-0011): endereço do broker, credenciais e
 * reconexão são da factory. A conexão é aberta no startup e fechada no {@link #close()}; a factory é da aplicação.
 *
 * <ul>
 *   <li>No Artemis, a factory precisa de {@code consumerWindowSize=0} ({@code JmsDialect.ARTEMIS.connectionFactory(url)}
 *       já aplica); sem a janela, o {@code receiver(nome)} lança {@link IllegalArgumentException}.</li>
 *   <li>O {@code checkAccess} precisa da conexão do cliente do broker: uma factory com pool por cima
 *       ({@code pooled-jms}, {@code CachingConnectionFactory}) derruba o health check.</li>
 * </ul>
 */
public final class JmsProviderConnection implements ProviderConnection {

    private final JmsDialect dialect;
    private final Connection connection;

    public JmsProviderConnection(String providerName, MessagingProperties.JmsProvider provider, ListableBeanFactory beans) {
        this.dialect = provider.type() == MessagingProperties.Type.ARTEMIS
                ? JmsDialect.ARTEMIS : JmsDialect.ACTIVEMQ_CLASSIC;
        ConnectionFactory factory = ClientBeans.require(beans, ConnectionFactory.class, providerName);
        Connection opened = null;
        try {
            opened = factory.createConnection();
            // com failover:, o Classic só conecta no primeiro uso; o start faz o startup falhar com o broker fora
            opened.start();
        } catch (JMSException e) {
            closeQuietly(opened);
            throw new IllegalStateException("messaging.providers." + providerName + ": não conectou ao broker: "
                    + e.getMessage(), e);
        }
        this.connection = opened;
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (JMSException ignored) {
            // a conexão pode já ter caído
        }
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        long max = destination.maxMessageBytes() != null
                ? destination.maxMessageBytes() : JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
        if (destination.topic() != null) {
            return JmsMessageSender.forTopic(connection, dialect, destination.topic(), max);
        }
        return JmsMessageSender.forQueue(connection, dialect, destination.queue(), max);
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        String queue = destination.subscription() != null
                ? dialect.subscriptionQueue(destination.topic(), destination.subscription())
                : destination.queue();
        JmsMessageReceiver.Redelivery redelivery = destination.redelivery() == MessagingProperties.Redelivery.RESCHEDULE
                ? JmsMessageReceiver.Redelivery.SCHEDULED : JmsMessageReceiver.Redelivery.IMMEDIATE;
        return new JmsMessageReceiver(connection, dialect, queue,
                destination.lease() != null ? destination.lease() : JmsMessageReceiver.DEFAULT_LEASE,
                JmsMessageReceiver.DEFAULT_MAX_IN_FLIGHT, redelivery, deadLetterSender);
    }

    @Override
    public void close() {
        closeQuietly(connection);
    }
}
