package com.example.messaging.spring;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Cria {@link MessagingDestinations} a partir de {@code messaging.*}. Métricas quando há um
 * {@link MeterRegistry}; {@code traceparent} quando há um {@link ObservationRegistry} com tracing.
 * Um {@code MessagingDestinations} próprio desliga esta auto-configuração.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration"})
@EnableConfigurationProperties(MessagingProperties.class)
public class MessagingAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    MessagingDestinations messagingDestinations(MessagingProperties properties, ListableBeanFactory beans,
                                                ObjectProvider<MeterRegistry> meters,
                                                ObjectProvider<ObservationRegistry> observations) {
        return new MessagingDestinations(properties, beans, meters.getIfAvailable(), observations.getIfAvailable());
    }
}
