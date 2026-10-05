package com.example.messaging.spring;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Health {@code messaging}: {@code checkAccess()} de cada destino, sem consumir mensagens
 * (ADR-0008). Opt-in com {@code management.health.messaging.enabled=true}, porque as checagens
 * exigem permissões extras em alguns provedores ({@code sns:GetTopicAttributes}, por exemplo).
 */
@AutoConfiguration(after = MessagingAutoConfiguration.class)
@ConditionalOnClass(HealthIndicator.class)
@ConditionalOnBean(MessagingDestinations.class)
@ConditionalOnProperty(name = "management.health.messaging.enabled", havingValue = "true")
public class MessagingHealthAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "messagingHealthIndicator")
    HealthIndicator messagingHealthIndicator(MessagingDestinations destinations) {
        return new MessagingHealthIndicator(destinations);
    }

    static final class MessagingHealthIndicator extends AbstractHealthIndicator {

        private final MessagingDestinations destinations;

        MessagingHealthIndicator(MessagingDestinations destinations) {
            super("Falha no health check de messaging");
            this.destinations = destinations;
        }

        @Override
        protected void doHealthCheck(Health.Builder builder) {
            Map<String, String> details = new LinkedHashMap<>();
            boolean up = true;
            for (String name : destinations.names()) {
                try {
                    destinations.checkAccess(name);
                    details.put(name, "UP");
                } catch (RuntimeException e) {
                    up = false;
                    details.put(name, e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
            (up ? builder.up() : builder.down()).withDetails(details);
        }
    }
}
