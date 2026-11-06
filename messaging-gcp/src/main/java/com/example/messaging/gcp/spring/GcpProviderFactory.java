package com.example.messaging.gcp.spring;

import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;
import com.example.messaging.spring.ProviderConnection;
import com.example.messaging.spring.ProviderFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.Set;

/** Google Pub/Sub no messaging-spring-boot-starter; carregada por ServiceLoader. */
public final class GcpProviderFactory implements ProviderFactory {

    @Override
    public Set<Type> types() {
        return Set.of(Type.GCP);
    }

    @Override
    public ProviderConnection connect(String providerName, Provider provider, ListableBeanFactory beans) {
        return new GcpProviderConnection(providerName, provider);
    }
}
