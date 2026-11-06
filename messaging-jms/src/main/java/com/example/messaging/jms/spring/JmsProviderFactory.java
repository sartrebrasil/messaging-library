package com.example.messaging.jms.spring;

import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;
import com.example.messaging.spring.ProviderConnection;
import com.example.messaging.spring.ProviderFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.Set;

/** ActiveMQ Classic e Artemis no messaging-spring-boot-starter; carregada por ServiceLoader. */
public final class JmsProviderFactory implements ProviderFactory {

    @Override
    public Set<Type> types() {
        return Set.of(Type.ACTIVEMQ_CLASSIC, Type.ARTEMIS);
    }

    @Override
    public ProviderConnection connect(String providerName, Provider provider, ListableBeanFactory beans) {
        return new JmsProviderConnection(providerName, provider, beans);
    }
}
