package com.example.messaging.rabbitmq.spring;

import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;
import com.example.messaging.spring.ProviderConnection;
import com.example.messaging.spring.ProviderFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.Set;

/** RabbitMQ no messaging-spring-boot-starter; carregada por ServiceLoader. */
public final class RabbitProviderFactory implements ProviderFactory {

    @Override
    public Set<Type> types() {
        return Set.of(Type.RABBITMQ);
    }

    @Override
    public ProviderConnection connect(String providerName, Provider provider, ListableBeanFactory beans) {
        return new RabbitProviderConnection(providerName, provider, beans);
    }
}
