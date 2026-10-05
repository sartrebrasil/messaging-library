package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.gcp.PubSubMessageReceiver;
import com.example.messaging.gcp.PubSubMessageSender;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;

/**
 * Pub/Sub. Credenciais e canal padrão do SDK (Application Default Credentials); com
 * {@code emulator-host}, canal sem TLS e sem credenciais. Nomes curtos de tópico e subscription
 * são completados com {@code project}.
 */
final class GcpProviderFactory implements ProviderFactory {

    private final MessagingProperties.Provider provider;
    private final ManagedChannel emulatorChannel;
    private final TransportChannelProvider channel;
    private final TopicAdminClient admin;

    GcpProviderFactory(String providerName, MessagingProperties.Provider provider) {
        this.provider = provider;
        if (provider.emulatorHost() != null) {
            emulatorChannel = ManagedChannelBuilder.forTarget(provider.emulatorHost()).usePlaintext().build();
            channel = FixedTransportChannelProvider.create(GrpcTransportChannel.create(emulatorChannel));
        } else {
            emulatorChannel = null;
            channel = null;
        }
        try {
            TopicAdminSettings.Builder settings = TopicAdminSettings.newBuilder();
            if (channel != null) {
                settings.setTransportChannelProvider(channel).setCredentialsProvider(NoCredentialsProvider.create());
            }
            admin = TopicAdminClient.create(settings.build());
        } catch (IOException e) {
            throw new MessagingException("gcp", "messaging.providers." + providerName + ": " + e.getMessage(), e, false);
        }
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        if (destination.topic() == null) {
            return null;
        }
        Publisher.Builder builder = Publisher.newBuilder(topicName(destination.topic()));
        if (channel != null) {
            builder.setChannelProvider(channel).setCredentialsProvider(NoCredentialsProvider.create());
        }
        return PubSubMessageSender.create(builder, admin);
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        if (destination.subscription() == null) {
            return null;
        }
        PubSubMessageReceiver.Options options = PubSubMessageReceiver.Options.ackDeadline(destination.ackDeadline())
                .withOrderedDelivery(destination.ordered())
                .withExactlyOnce(destination.exactlyOnce())
                .withDeadLetterSender(deadLetterSender);
        try {
            SubscriberStubSettings.Builder settings = SubscriberStubSettings.newBuilder();
            if (channel != null) {
                settings.setTransportChannelProvider(channel).setCredentialsProvider(NoCredentialsProvider.create());
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
        if (emulatorChannel != null) {
            emulatorChannel.shutdown();
        }
    }
}
