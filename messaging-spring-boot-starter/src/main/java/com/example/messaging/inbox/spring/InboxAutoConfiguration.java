package com.example.messaging.inbox.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.PayloadDeserializer;
import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.List;
import java.util.function.Function;

/**
 * Wires {@code messaging.receivers} (ADR-0008): addresses through the messaging-lib starter, payloads through
 * the application's Jackson mapper, metrics and tracing when Micrometer is there, idempotency when an
 * {@code IdempotencyStore} bean is there. {@code messaging.receivers-enabled: false} turns all of it off (API-only
 * instances and tests): no receiver is bound, validated or started.
 */
@AutoConfiguration(after = MessagingAutoConfiguration.class, afterName = {
        "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration"})
@ConditionalOnClass(name = {"com.example.messaging.inbox.InboxConsumer", "tools.jackson.databind.ObjectMapper"})
@ConditionalOnProperty(prefix = "messaging", name = "receivers-enabled", matchIfMissing = true)
public class InboxAutoConfiguration {

    private static final boolean MICROMETER = ClassUtils.isPresent("io.micrometer.core.instrument.MeterRegistry",
            InboxAutoConfiguration.class.getClassLoader())
            && ClassUtils.isPresent("io.micrometer.observation.ObservationRegistry",
            InboxAutoConfiguration.class.getClassLoader());

    @Bean
    InboxProperties inboxProperties(Environment environment) {
        return InboxProperties.bind(Binder.get(environment));
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(MessagingDestinations.class)
    InboxDestinations inboxDestinations(MessagingDestinations destinations) {
        return new InboxDestinations() {
            @Override
            public MessageReceiver receiver(String name, String provider, Endpoint endpoint, MessageSender deadLetter) {
                return destinations.receiver(name, provider, endpoint, deadLetter);
            }

            @Override
            public MessageSender sender(String name, String provider, Endpoint endpoint) {
                return destinations.sender(name, provider, endpoint);
            }

            @Override
            public List<String> validate(String key, String provider, Endpoint endpoint, boolean receive) {
                return destinations.validate(key, provider, endpoint, receive);
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean
    PayloadDeserializer inboxPayloadDeserializer(ObjectProvider<ObjectMapper> mappers) {
        return new JacksonPayloadDeserializer(mappers.getIfAvailable(() -> JsonMapper.builder().build()));
    }

    @Bean
    InboxLifecycle inboxLifecycle(InboxProperties properties, ConfigurableListableBeanFactory beans,
                                  ObjectProvider<InboxDestinations> destinations, PayloadDeserializer deserializer) {
        Clock clock = Clock.systemUTC();
        InboxLifecycle[] lifecycle = new InboxLifecycle[1];
        Function<InboxConfig.Consumer, ConsumerObserver> observers = MICROMETER
                ? c -> MicrometerSupport.observer(c, beans, clock, () -> lifecycle[0].lastReceive(c.name()))
                : c -> ConsumerObserver.NONE;
        ConsumerBuilder builder = new ConsumerBuilder(lazy(destinations), deserializer, beans, observers, clock);
        lifecycle[0] = new InboxLifecycle(properties, beans, builder);
        return lifecycle[0];
    }

    /** Destinations are looked up on first use, so a consumer-less application needs none. */
    private static InboxDestinations lazy(ObjectProvider<InboxDestinations> provider) {
        return new InboxDestinations() {
            @Override
            public MessageReceiver receiver(String name, String provider, Endpoint endpoint, MessageSender deadLetter) {
                return required().receiver(name, provider, endpoint, deadLetter);
            }

            @Override
            public MessageSender sender(String name, String provider, Endpoint endpoint) {
                return required().sender(name, provider, endpoint);
            }

            @Override
            public List<String> validate(String key, String provider, Endpoint endpoint, boolean receive) {
                return required().validate(key, provider, endpoint, receive);
            }

            private InboxDestinations required() {
                InboxDestinations d = provider.getIfAvailable();
                if (d == null) {
                    throw new IllegalStateException("messaging.receivers need the messaging-lib starter (messaging.providers) "
                            + "starter) or an InboxDestinations bean");
                }
                return d;
            }
        };
    }
}
