package com.example.messaging.inbox.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.PayloadDeserializer;
import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import com.example.messaging.spring.MessagingProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.ClassUtils;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.function.Function;

/**
 * Wires {@code inbox.*} consumers (ADR-0008): destinations from {@code messaging.destinations}, payloads through
 * the application's Jackson mapper, metrics and tracing when Micrometer is there, idempotency when an
 * {@code IdempotencyStore} bean is there.
 */
@AutoConfiguration(after = MessagingAutoConfiguration.class, afterName = {
        "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration"})
@ConditionalOnClass(name = {"com.example.messaging.inbox.InboxConsumer", "tools.jackson.databind.ObjectMapper"})
@ConditionalOnProperty(prefix = "inbox", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(InboxProperties.class)
public class InboxAutoConfiguration {

    private static final boolean MICROMETER = ClassUtils.isPresent("io.micrometer.core.instrument.MeterRegistry",
            InboxAutoConfiguration.class.getClassLoader())
            && ClassUtils.isPresent("io.micrometer.observation.ObservationRegistry",
            InboxAutoConfiguration.class.getClassLoader());

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(MessagingDestinations.class)
    InboxDestinations inboxDestinations(MessagingDestinations destinations) {
        return new InboxDestinations() {
            @Override
            public MessageReceiver receiver(String name) {
                return destinations.receiver(name, null, new MessagingProperties.Endpoint(name, null, null, null), null);
            }

            @Override
            public MessageSender sender(String name) {
                return destinations.sender(name, null, new MessagingProperties.Endpoint(name, null, null, null));
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
            public MessageReceiver receiver(String name) {
                return required().receiver(name);
            }

            @Override
            public MessageSender sender(String name) {
                return required().sender(name);
            }

            private InboxDestinations required() {
                InboxDestinations d = provider.getIfAvailable();
                if (d == null) {
                    throw new IllegalStateException("inbox consumers need messaging.destinations (the messaging-lib "
                            + "starter) or an InboxDestinations bean");
                }
                return d;
            }
        };
    }
}
