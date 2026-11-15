package com.example.messaging.spring.gcp;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.gcp.PubSubMessageReceiver;
import com.example.messaging.gcp.PubSubMessageSender;
import com.example.messaging.spring.ClientBeans;
import com.example.messaging.spring.MessagingProperties;
import com.example.messaging.spring.ProviderConnection;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.TopicName;
import org.springframework.beans.factory.ListableBeanFactory;

import java.io.IOException;

/**
 * Pub/Sub. O {@link TransportChannelProvider} e o {@link CredentialsProvider} da aplicação, quando declarados (ADR-0011),
 * valem para o admin, os publishers e os subscribers; sem eles, o padrão do SDK (Application Default Credentials). O
 * canal é da aplicação e nunca é fechado aqui. Nomes curtos de tópico e subscription são completados com
 * {@code project}.
 */
public final class GcpProviderConnection implements ProviderConnection {

    private final MessagingProperties.GcpProvider provider;
    private final TransportChannelProvider channel;
    private final CredentialsProvider credentials;
    private final TopicAdminClient admin;

    public GcpProviderConnection(String providerName, MessagingProperties.GcpProvider provider,
                                 ListableBeanFactory beans) {
        this.provider = provider;
        this.channel = ClientBeans.optional(beans, TransportChannelProvider.class, providerName);
        this.credentials = ClientBeans.optional(beans, CredentialsProvider.class, providerName);
        try {
            TopicAdminSettings.Builder settings = TopicAdminSettings.newBuilder();
            if (channel != null) {
                settings.setTransportChannelProvider(channel);
            }
            if (credentials != null) {
                settings.setCredentialsProvider(credentials);
            }
            admin = TopicAdminClient.create(settings.build());
        } catch (IOException e) {
            throw new MessagingException("gcp", "messaging.providers." + providerName + ": " + e.getMessage(), e, false);
        }
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        Publisher.Builder builder = Publisher.newBuilder(topicName(destination.topic()));
        if (channel != null) {
            builder.setChannelProvider(channel);
        }
        if (credentials != null) {
            builder.setCredentialsProvider(credentials);
        }
        return PubSubMessageSender.create(builder, admin);
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        PubSubMessageReceiver.Options options = PubSubMessageReceiver.Options.ackDeadline(destination.ackDeadline())
                .withOrderedDelivery(destination.ordered())
                .withExactlyOnce(destination.exactlyOnce())
                .withDeadLetterSender(deadLetterSender);
        try {
            SubscriberStubSettings.Builder settings = SubscriberStubSettings.newBuilder();
            if (channel != null) {
                settings.setTransportChannelProvider(channel);
            }
            if (credentials != null) {
                settings.setCredentialsProvider(credentials);
            }
            return PubSubMessageReceiver.create(settings.build(), subscriptionName(destination.subscription()), options);
        } catch (IOException e) {
            throw new MessagingException("gcp", "SubscriberStub de " + name + ": " + e.getMessage(), e, false);
        }
    }

    private String topicName(String topic) {
        return topic.startsWith("projects/") ? topic : TopicName.format(provider.project(), topic);
    }

    private String subscriptionName(String subscription) {
        return subscription.startsWith("projects/") ? subscription
                : ProjectSubscriptionName.format(provider.project(), subscription);
    }

    @Override
    public void close() {
        admin.close();
    }
}
