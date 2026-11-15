package com.example.messaging.spring.azure;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.azure.ServiceBusMessageReceiver;
import com.example.messaging.azure.ServiceBusMessageSender;
import com.example.messaging.azure.ServiceBusSessionMessageReceiver;
import com.example.messaging.spring.ClientBeans;
import com.example.messaging.spring.MessagingProperties;
import com.example.messaging.spring.ProviderConnection;
import org.springframework.beans.factory.ListableBeanFactory;

/**
 * Service Bus sobre o bean {@link ServiceBusClientBuilder} da aplicação (ADR-0011): a conexão AMQP é compartilhada
 * pelos clientes do builder. Os clientes de cada fila ou subscription são fechados pelos próprios senders e
 * receivers ({@code for*}); o builder é da aplicação.
 */
public final class AzureProviderConnection implements ProviderConnection {

    private final ServiceBusClientBuilder builder;

    public AzureProviderConnection(String providerName, ListableBeanFactory beans) {
        this.builder = ClientBeans.require(beans, ServiceBusClientBuilder.class, providerName);
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        long max = maxMessageBytes(destination);
        if (destination.topic() != null) {
            return ServiceBusMessageSender.forTopic(builder, destination.topic(), destination.sessions(), max);
        }
        return ServiceBusMessageSender.forQueue(builder, destination.queue(), destination.sessions(), max);
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        long max = maxMessageBytes(destination);
        // a DLQ não tem sessions, mesmo quando a entidade tem
        if (destination.deadLetterQueue()) {
            return destination.subscription() != null
                    ? ServiceBusMessageReceiver.forSubscriptionDeadLetterQueue(builder, destination.topic(),
                    destination.subscription(), max)
                    : ServiceBusMessageReceiver.forDeadLetterQueue(builder, destination.queue(), max);
        }
        if (destination.subscription() != null) {
            return destination.sessions()
                    ? ServiceBusSessionMessageReceiver.forSubscription(builder, destination.topic(),
                    destination.subscription(), max)
                    : ServiceBusMessageReceiver.forSubscription(builder, destination.topic(), destination.subscription(), max);
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
