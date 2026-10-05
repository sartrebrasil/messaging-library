package com.example.messaging.gcp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.testcontainers.gcloud.PubSubEmulatorContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

/** Um Pub/Sub emulator por JVM, compartilhado pelos contratos. Imagem com versão fixada. */
final class PubSubEmulator {

    static final String PROJECT = "contract";
    /** Menor ack deadline que o Pub/Sub aceita. */
    static final Duration LEASE = Duration.ofSeconds(10);

    private static TransportChannelProvider channel;
    private static final CredentialsProvider CREDENTIALS = NoCredentialsProvider.create();
    private static TopicAdminClient topics;
    private static SubscriptionAdminClient subscriptions;
    private static RuntimeException startFailure;

    private PubSubEmulator() {
    }

    static synchronized void start() {
        // falha no startup vale para a JVM toda
        if (startFailure != null) {
            throw startFailure;
        }
        if (channel != null) {
            return;
        }
        try {
            PubSubEmulatorContainer emulator =
                    new PubSubEmulatorContainer("gcr.io/google.com/cloudsdktool/google-cloud-cli:587.0.0-emulators");
            emulator.start();
            ManagedChannel grpc = ManagedChannelBuilder.forTarget(emulator.getEmulatorEndpoint()).usePlaintext().build();
            channel = FixedTransportChannelProvider.create(GrpcTransportChannel.create(grpc));
            topics = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                    .setTransportChannelProvider(channel).setCredentialsProvider(CREDENTIALS).build());
            subscriptions = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
                    .setTransportChannelProvider(channel).setCredentialsProvider(CREDENTIALS).build());
        } catch (IOException e) {
            startFailure = new UncheckedIOException(e);
            throw startFailure;
        } catch (RuntimeException e) {
            startFailure = e;
            throw e;
        }
    }

    /** Cria o tópico e uma subscription; idempotente entre classes de teste. */
    static String topicWithSubscription(String topic, String subscription, boolean ordered) {
        TopicName topicName = TopicName.of(PROJECT, topic);
        String subscriptionName = ProjectSubscriptionName.format(PROJECT, subscription);
        try {
            topics.createTopic(topicName);
        } catch (com.google.api.gax.rpc.AlreadyExistsException ignored) {
            // outra classe já criou
        }
        try {
            subscriptions.createSubscription(Subscription.newBuilder()
                    .setName(subscriptionName)
                    .setTopic(topicName.toString())
                    .setAckDeadlineSeconds((int) LEASE.toSeconds())
                    .setEnableMessageOrdering(ordered)
                    .build());
        } catch (com.google.api.gax.rpc.AlreadyExistsException ignored) {
            // outra classe já criou
        }
        return subscriptionName;
    }

    static PubSubMessageSender sender(String topic) {
        return PubSubMessageSender.create(Publisher.newBuilder(TopicName.of(PROJECT, topic))
                .setChannelProvider(channel).setCredentialsProvider(CREDENTIALS), topics);
    }

    static PubSubMessageReceiver receiver(String subscription, PubSubMessageReceiver.Options options) {
        try {
            return PubSubMessageReceiver.create(SubscriberStubSettings.newBuilder()
                    .setTransportChannelProvider(channel).setCredentialsProvider(CREDENTIALS).build(),
                    subscription, options);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
