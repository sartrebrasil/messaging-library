package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.redrive.InboxRedrive;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.Map;

/** The {@code inboxredrive} endpoint, only with the Actuator and only when access and exposure turn it on. */
@AutoConfiguration(after = InboxAutoConfiguration.class)
@ConditionalOnClass(name = {"org.springframework.boot.actuate.endpoint.annotation.Endpoint",
        "org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint",
        "com.example.messaging.inbox.InboxConsumer"})
@ConditionalOnBean(InboxRedrive.class)
public class InboxRedriveEndpointAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnAvailableEndpoint
    InboxRedriveEndpoint inboxRedriveEndpoint(InboxRedrive redrive, ObjectProvider<InboxLifecycle> lifecycle) {
        return new InboxRedriveEndpoint(redrive, () -> {
            InboxLifecycle l = lifecycle.getIfAvailable();
            return l == null ? Map.of() : l.redrive().routes();
        });
    }
}
