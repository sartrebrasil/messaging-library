package com.example.messaging.spring;

import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;

/** Um {@link Provider} sem campos de conexão, de cada tipo; {@code project} {@code "p"} no GCP. */
public final class TestProviders {

    private TestProviders() {
    }

    public static Provider of(Type type) {
        return switch (type) {
            case AWS -> new MessagingProperties.AwsProvider(type, null, null, null, null);
            case AZURE -> new MessagingProperties.AzureProvider(type, null);
            case GCP -> new MessagingProperties.GcpProvider(type, "p", null);
            case ACTIVEMQ_CLASSIC, ARTEMIS -> new MessagingProperties.JmsProvider(type, null, null, null);
            case RABBITMQ -> new MessagingProperties.RabbitProvider(type, null, null, null);
        };
    }
}
