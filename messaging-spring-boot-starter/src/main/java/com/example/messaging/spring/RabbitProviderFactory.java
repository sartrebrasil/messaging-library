package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.rabbitmq.RabbitMessageReceiver;
import com.example.messaging.rabbitmq.RabbitMessageSender;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ (ADR-0009). Uma {@link Connection} por provider, compartilhada pelos senders e
 * receivers, criada com {@code broker-url} ({@code amqp://host:5672/vhost}) ou com o bean
 * {@link ConnectionFactory} da aplicação. A conexão é aberta no startup e fechada no
 * {@link #close()}.
 */
final class RabbitProviderFactory implements ProviderFactory {

    private final Connection connection;

    RabbitProviderFactory(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans) {
        ConnectionFactory factory = provider.brokerUrl() != null
                ? new ConnectionFactory() : beans.getBeanProvider(ConnectionFactory.class).getIfAvailable();
        if (factory == null) {
            throw new IllegalStateException("messaging.providers." + providerName
                    + ": defina broker-url ou declare um bean com.rabbitmq.client.ConnectionFactory");
        }
        try {
            if (provider.brokerUrl() != null) {
                factory.setUri(provider.brokerUrl());
            }
            if (provider.user() != null) {
                factory.setUsername(provider.user());
                factory.setPassword(provider.password());
            }
            connection = factory.newConnection("messaging-" + providerName);
        } catch (URISyntaxException | GeneralSecurityException e) {
            throw new IllegalStateException("messaging.providers." + providerName + ".broker-url inválida: "
                    + e.getMessage(), e);
        } catch (IOException | TimeoutException e) {
            throw new IllegalStateException("messaging.providers." + providerName + ": não conectou ao broker: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        long max = destination.maxMessageBytes() != null
                ? destination.maxMessageBytes() : RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
        if (destination.topic() != null) {
            return RabbitMessageSender.forExchange(connection, destination.topic(),
                    destination.routingKey() != null ? destination.routingKey() : "", max);
        }
        return destination.queue() == null ? null : RabbitMessageSender.forQueue(connection, destination.queue(), max);
    }

    /** A subscription de um exchange é a fila ligada a ele: o receiver lê dela. */
    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        String queue = destination.subscription() != null ? destination.subscription() : destination.queue();
        if (queue == null) {
            return null;
        }
        return new RabbitMessageReceiver(connection, queue,
                destination.lease() != null ? destination.lease() : RabbitMessageReceiver.DEFAULT_LEASE,
                deadLetterSender);
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (IOException ignored) {
            // a conexão pode já ter caído
        }
    }
}
