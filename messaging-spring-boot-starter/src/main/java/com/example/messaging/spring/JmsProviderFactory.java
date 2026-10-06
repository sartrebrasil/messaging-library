package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.jms.JmsMessageReceiver;
import com.example.messaging.jms.JmsMessageSender;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.ListableBeanFactory;

/**
 * ActiveMQ Classic (ADR-0009). Uma {@link Connection} por provider, compartilhada pelos senders e
 * receivers, criada com {@code broker-url} ou com o bean {@link ConnectionFactory} da aplicação.
 * A conexão é aberta no startup e fechada no {@link #close()}.
 *
 * <p>O {@code checkAccess} precisa de uma {@code ActiveMQConnection}: um bean com pool por cima
 * ({@code pooled-jms}, {@code CachingConnectionFactory}) derruba o health check.</p>
 */
final class ActiveMqProviderFactory implements ProviderFactory {

    private final Connection connection;

    ActiveMqProviderFactory(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans) {
        ConnectionFactory factory = provider.brokerUrl() != null
                ? new ActiveMQConnectionFactory(provider.brokerUrl())
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
            return JmsMessageSender.forTopic(connection, destination.topic(), max);
        }
        return destination.queue() == null ? null : JmsMessageSender.forQueue(connection, destination.queue(), max);
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        String queue = destination.subscription() != null
                ? "Consumer." + destination.subscription() + "." + destination.topic()
                : destination.queue();
        if (queue == null) {
            return null;
        }
        JmsMessageReceiver.Redelivery redelivery = destination.redelivery() == MessagingProperties.Redelivery.RESCHEDULE
                ? JmsMessageReceiver.Redelivery.SCHEDULED : JmsMessageReceiver.Redelivery.IMMEDIATE;
        return new JmsMessageReceiver(connection, queue,
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
