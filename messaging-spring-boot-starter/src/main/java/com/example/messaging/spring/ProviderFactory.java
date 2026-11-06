package com.example.messaging.spring;

import org.springframework.beans.factory.ListableBeanFactory;

import java.util.Set;

/**
 * Provedor de {@code messaging.providers.<nome>.type}, descoberto por {@link java.util.ServiceLoader}
 * ({@code META-INF/services/com.example.messaging.spring.ProviderFactory}) no jar de cada adapter.
 *
 * <p>Toda fábrica do classpath é instanciada, mesmo sem provider configurado: a implementação só
 * referencia tipos do core e do starter, e as classes do SDK ficam na {@link ProviderConnection}.
 */
public interface ProviderFactory {

    /** Tipos atendidos; dois adapters não podem atender o mesmo tipo. */
    Set<MessagingProperties.Type> types();

    /**
     * Cria os clientes do provider (pode conectar). Chamado só depois da validação de
     * {@code messaging.*}.
     *
     * @param beans beans da aplicação, para clientes nativos declarados por ela
     */
    ProviderConnection connect(String providerName, MessagingProperties.Provider provider, ListableBeanFactory beans);
}
