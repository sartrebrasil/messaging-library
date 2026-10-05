package com.example.messaging.spring;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.azure.ServiceBusMessageReceiver;
import com.example.messaging.azure.ServiceBusMessageSender;
import com.example.messaging.azure.ServiceBusSessionMessageReceiver;
import org.springframework.beans.factory.ListableBeanFactory;

/**
 * Service Bus. Com {@code connection-string}, um {@link ServiceBusClientBuilder} por provider
 * (a conexão AMQP é compartilhada pelos clientes do builder); sem ela, o bean
 * {@code ServiceBusClientBuilder} da aplicação. Os clientes são fechados pelos próprios
 * senders e receivers ({@code for*}).
 */
final class AzureProviderFactory implements ProviderFactory {

    private final ServiceBusClientBuilder builder;

    AzureProviderFactory(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans) {
        if (provider.connectionString() != null) {
            builder = new ServiceBusClientBuilder().connectionString(provider.connectionString());
        } else {
            builder = beans.getBeanProvider(ServiceBusClientBuilder.class).getIfAvailable();
            if (builder == null) {
                throw new IllegalStateException("messaging.providers." + providerName
                        + ": defina connection-string ou declare um bean ServiceBusClientBuilder");
            }
        }
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        long max = maxMessageBytes(destination);
        if (destination.topic() != null) {
            return ServiceBusMessageSender.forTopic(builder, destination.topic(), destination.sessions(), max);
        }
        return destination.queue() == null ? null
                : ServiceBusMessageSender.forQueue(builder, destination.queue(), destination.sessions(), max);
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        long max = maxMessageBytes(destination);
        if (destination.subscription() != null) {
            return destination.sessions()
                    ? ServiceBusSessionMessageReceiver.forSubscription(builder, destination.topic(),
                    destination.subscription(), max)
                    : ServiceBusMessageReceiver.forSubscription(builder, destination.topic(), destination.subscription(), max);
        }
        if (destination.queue() == null) {
            return null;
        }
        if (destination.sessions()) {
            return ServiceBusSessionMessageReceiver.forQueue(builder, destination.queue(), max);
        }
        ServiceBusMessageReceiver.Redelivery redelivery =
                ServiceBusMessageReceiver.Redelivery.valueOf(destination.redelivery().name());
        return ServiceBusMessageReceiver.forQueue(builder, destination.queue(), redelivery, max);
    }

    private static long maxMessageBytes(MessagingProperties.Destination destination) {
        return destination.maxMessageBytes() != null
                ? destination.maxMessageBytes() : ServiceBusMessageSender.STANDARD_MAX_MESSAGE_BYTES;
    }

    @Override
    public void close() {
        // cada sender/receiver fecha o cliente que criou; o builder não tem recurso próprio
    }
}
