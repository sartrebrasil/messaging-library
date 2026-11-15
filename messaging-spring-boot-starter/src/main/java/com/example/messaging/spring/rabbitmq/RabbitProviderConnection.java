package com.example.messaging.spring.rabbitmq;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.rabbitmq.RabbitMessageReceiver;
import com.example.messaging.rabbitmq.RabbitMessageSender;
import com.example.messaging.spring.ClientBeans;
import com.example.messaging.spring.MessagingProperties;
import com.example.messaging.spring.ProviderConnection;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ (ADR-0009). Uma {@link Connection} por provider, compartilhada pelos senders e receivers, criada pelo
 * bean {@link ConnectionFactory} da aplicação (ADR-0011): URI, vhost e credenciais são da factory. A conexão é
 * aberta no startup e fechada no {@link #close()}; a factory é da aplicação.
 */
public final class RabbitProviderConnection implements ProviderConnection {

    private final Connection connection;

    public RabbitProviderConnection(String providerName, ListableBeanFactory beans) {
        ConnectionFactory factory = ClientBeans.require(beans, ConnectionFactory.class, providerName);
        try {
            connection = factory.newConnection("messaging-" + providerName);
        } catch (IOException | TimeoutException e) {
            throw new IllegalStateException("messaging.providers." + providerName + ": não conectou ao broker: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        long max = destination.maxMessageBytes() != null
                ? destination.maxMessageBytes() : RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
        RabbitMessageSender sender;
        if (destination.topic() != null && destination.routingByOrderingKey()) {
            sender = RabbitMessageSender.forExchangeByOrderingKey(connection, destination.topic(), max);
        } else if (destination.topic() != null) {
            sender = RabbitMessageSender.forExchange(connection, destination.topic(),
                    destination.routingKey() != null ? destination.routingKey() : "", max);
        } else {
            sender = RabbitMessageSender.forQueue(connection, destination.queue(), max);
        }
        return destination.confirmTimeout() != null ? sender.withConfirmTimeout(destination.confirmTimeout()) : sender;
    }

    /** A subscription de um exchange é a fila ligada a ele: o receiver lê dela. */
    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        String queue = destination.subscription() != null ? destination.subscription() : destination.queue();
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
