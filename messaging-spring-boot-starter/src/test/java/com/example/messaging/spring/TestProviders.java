package com.example.messaging.spring;

import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;

/** Um {@link Provider} de cada tipo; {@code project} {@code "p"} no GCP. */
public final class TestProviders {

    private TestProviders() {
    }

    public static Provider of(Type type) {
        return switch (type) {
            case AWS -> new MessagingProperties.AwsProvider(type);
            case AZURE -> new MessagingProperties.AzureProvider(type);
            case GCP -> new MessagingProperties.GcpProvider(type, "p");
            case ACTIVEMQ_CLASSIC, ARTEMIS -> new MessagingProperties.JmsProvider(type);
            case RABBITMQ -> new MessagingProperties.RabbitProvider(type);
        };
    }
}
