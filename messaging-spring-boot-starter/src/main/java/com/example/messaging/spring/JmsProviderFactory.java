package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.jms.JmsDialect;
import com.example.messaging.jms.JmsMessageReceiver;
import com.example.messaging.jms.JmsMessageSender;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.springframework.beans.factory.ListableBeanFactory;

/**
 * ActiveMQ Classic e Artemis (ADR-0009). Uma {@link Connection} por provider, compartilhada pelos
 * senders e receivers, criada com {@code broker-url} ou com o bean {@link ConnectionFactory} da
 * aplicação. A conexão é aberta no startup e fechada no {@link #close()}.
 *
 * <ul>
 *   <li>No Artemis, {@code broker-url} ganha {@code consumerWindowSize=0} quando não define a
 *       janela; um bean com janela faz o {@code receiver(nome)} lançar
 *       {@link IllegalArgumentException}.</li>
 *   <li>O {@code checkAccess} precisa da conexão do cliente do broker: um bean com pool por cima
 *       ({@code pooled-jms}, {@code CachingConnectionFactory}) derruba o health check.</li>
 * </ul>
 */
final class JmsProviderFactory implements ProviderFactory {

    private final JmsDialect dialect;
    private final Connection connection;

    JmsProviderFactory(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans) {
        this.dialect = provider.type() == MessagingProperties.Type.ARTEMIS
                ? JmsDialect.ARTEMIS : JmsDialect.ACTIVEMQ_CLASSIC;
        ConnectionFactory factory = provider.brokerUrl() != null
                ? dialect.connectionFactory(provider.brokerUrl())
                : beans.getBeanProvider(ConnectionFactory.class).getIfAvailable();
        if (factory == null) {
            throw new IllegalStateException("messaging.providers." + providerName
                    + ": defina broker-url ou declare um bean jakarta.jms.ConnectionFactory");
        }
        try {
            connection = provider.user() != null
                    ? factory.createConnection(provider.user(), provider.password())
                    : factory.createConnection();
        } catch (JMSException e) {
            throw new IllegalStateException("messaging.providers." + providerName + ": não conectou ao broker: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        long max = destination.maxMessageBytes() != null
                ? destination.maxMessageBytes() : JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
        if (destination.topic() != null) {
            return JmsMessageSender.forTopic(connection, dialect, destination.topic(), max);
        }
        return destination.queue() == null ? null
                : JmsMessageSender.forQueue(connection, dialect, destination.queue(), max);
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        String queue = destination.subscription() != null
                ? dialect.subscriptionQueue(destination.topic(), destination.subscription())
                : destination.queue();
        if (queue == null) {
            return null;
        }
        JmsMessageReceiver.Redelivery redelivery = destination.redelivery() == MessagingProperties.Redelivery.RESCHEDULE
                ? JmsMessageReceiver.Redelivery.SCHEDULED : JmsMessageReceiver.Redelivery.IMMEDIATE;
        return new JmsMessageReceiver(connection, dialect, queue,
                destination.lease() != null ? destination.lease() : JmsMessageReceiver.DEFAULT_LEASE,
                JmsMessageReceiver.DEFAULT_MAX_IN_FLIGHT, redelivery, deadLetterSender);
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (JMSException ignored) {
            // a conexão pode já ter caído
        }
    }
}
