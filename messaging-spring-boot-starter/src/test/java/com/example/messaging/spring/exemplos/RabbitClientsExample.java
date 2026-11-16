package com.example.messaging.spring.exemplos;

import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URISyntaxException;
import java.security.GeneralSecurityException;

/**
 * Como a aplicação declara o {@code com.rabbitmq.client.ConnectionFactory} que o starter usa com {@code type: rabbitmq}
 * (ADR-0011). É a factory do cliente nativo, não a do Spring AMQP: o adapter usa a {@code Connection} nativa. URI, vhost,
 * credenciais, TLS e recuperação de conexão são da factory; o starter só chama {@code newConnection(nome)}.
 */
public final class RabbitClientsExample {

    private RabbitClientsExample() {
    }

    /**
     * @param uri          {@code amqp://host:5672/vhost} (ou {@code amqps://})
     * @param userOrNull   sem valor, o usuário da URI, ou {@code guest}
     */
    public static ConnectionFactory connectionFactory(String uri, String userOrNull, String passwordOrNull) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
        } catch (URISyntaxException | GeneralSecurityException e) {
            throw new IllegalStateException("URI do RabbitMQ inválida: " + e.getMessage(), e);
        }
        if (userOrNull != null) {
            factory.setUsername(userOrNull);
            factory.setPassword(passwordOrNull);
        }
        return factory;
    }

    /** Produção: a URI e as credenciais vêm da configuração da aplicação. */
    @Configuration(proxyBeanMethods = false)
    public static class Producao {

        @Bean
        ConnectionFactory rabbitConnectionFactory(@Value("${rabbit.uri}") String uri,
                                                  @Value("${rabbit.user:}") String user,
                                                  @Value("${rabbit.password:}") String password) {
            return RabbitClientsExample.connectionFactory(uri, user.isEmpty() ? null : user, password);
        }
    }
}
