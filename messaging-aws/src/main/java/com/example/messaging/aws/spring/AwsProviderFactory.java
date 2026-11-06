package com.example.messaging.aws.spring;

import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;
import com.example.messaging.spring.ProviderConnection;
import com.example.messaging.spring.ProviderFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.Set;

/** AWS (SQS e SNS) no messaging-spring-boot-starter; carregada por ServiceLoader. */
public final class AwsProviderFactory implements ProviderFactory {

    @Override
    public Set<Type> types() {
        return Set.of(Type.AWS);
    }

    @Override
    public ProviderConnection connect(String providerName, Provider provider, ListableBeanFactory beans) {
        return new AwsProviderConnection(provider, beans);
    }
}
