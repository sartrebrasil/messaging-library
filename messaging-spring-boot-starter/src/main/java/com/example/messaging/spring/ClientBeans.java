package com.example.messaging.spring;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;

/**
 * Onde a conexão de um provider procura o objeto raiz que a aplicação declara (ADR-0011): o bean do tipo, ou o
 * {@code @Primary} quando há mais de um. O starter nunca cria nem fecha esses beans.
 */
public final class ClientBeans {

    private ClientBeans() {
    }

    /** @throws IllegalStateException sem bean, ou com vários e nenhum {@code @Primary} */
    public static <T> T require(ListableBeanFactory beans, Class<T> type, String providerName) {
        T bean = optional(beans, type, providerName);
        if (bean == null) {
            throw new IllegalStateException("messaging.providers." + providerName + ": declare um bean "
                    + type.getName() + " (ADR-0011)");
        }
        return bean;
    }

    /** @return o bean, ou {@code null} sem bean; vários sem {@code @Primary} lançam {@link IllegalStateException} */
    public static <T> T optional(ListableBeanFactory beans, Class<T> type, String providerName) {
        try {
            return beans.getBeanProvider(type).getIfAvailable();
        } catch (NoUniqueBeanDefinitionException e) {
            throw new IllegalStateException("messaging.providers." + providerName + ": há mais de um bean "
                    + type.getName() + "; marque um com @Primary (ADR-0011)", e);
        }
    }
}
