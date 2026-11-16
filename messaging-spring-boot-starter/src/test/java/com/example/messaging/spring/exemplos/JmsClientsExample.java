package com.example.messaging.spring.exemplos;

import com.example.messaging.jms.JmsDialect;
import jakarta.jms.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Como a aplicação declara o {@code jakarta.jms.ConnectionFactory} que o starter usa com {@code type: activemq-classic}
 * ou {@code type: artemis} (ADR-0011). O starter chama {@code createConnection()} sem usuário nem senha: as credenciais
 * ficam na factory.
 *
 * <p>{@code JmsDialect.<X>.connectionFactory(url)} devolve a factory do cliente do broker com a reconexão que a lib exige:
 * no Classic, embrulha a URL em {@code failover:(...)}, salvo {@code failover:} e {@code vm:}; no Artemis, acrescenta
 * {@code reconnectAttempts=-1} e {@code consumerWindowSize=0} (o receiver confere a janela).</p>
 *
 * <p><b>Sem pool nem cache por cima.</b> O {@code checkAccess} precisa da conexão do cliente do broker: com um
 * {@code CachingConnectionFactory} ou {@code JmsPoolConnectionFactory}, o health check falha. Declarar esta factory
 * desliga a que o Spring Boot criaria (cache por padrão); ela vira o {@code ConnectionFactory} da aplicação inteira.</p>
 */
public final class JmsClientsExample {

    private JmsClientsExample() {
    }

    /** @param userOrNull sem valor, sem autenticação */
    public static ConnectionFactory activeMqClassic(String brokerUrl, String userOrNull, String passwordOrNull) {
        org.apache.activemq.ActiveMQConnectionFactory factory = (org.apache.activemq.ActiveMQConnectionFactory)
                JmsDialect.ACTIVEMQ_CLASSIC.connectionFactory(brokerUrl);
        if (userOrNull != null) {
            factory.setUserName(userOrNull);
            factory.setPassword(passwordOrNull);
        }
        return factory;
    }

    /** @param userOrNull sem valor, sem autenticação */
    public static ConnectionFactory artemis(String brokerUrl, String userOrNull, String passwordOrNull) {
        org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory factory =
                (org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory)
                        JmsDialect.ARTEMIS.connectionFactory(brokerUrl);
        if (userOrNull != null) {
            factory.setUser(userOrNull);
            factory.setPassword(passwordOrNull);
        }
        return factory;
    }

    /** ActiveMQ Classic, com a URL e as credenciais vindas da configuração da aplicação. */
    @Configuration(proxyBeanMethods = false)
    public static class Classic {

        @Bean
        ConnectionFactory jmsConnectionFactory(@Value("${jms.broker-url}") String brokerUrl,
                                               @Value("${jms.user:}") String user,
                                               @Value("${jms.password:}") String password) {
            return JmsClientsExample.activeMqClassic(brokerUrl, user.isEmpty() ? null : user, password);
        }
    }

    /** Artemis, com a URL e as credenciais vindas da configuração da aplicação. */
    @Configuration(proxyBeanMethods = false)
    public static class Artemis {

        @Bean
        ConnectionFactory jmsConnectionFactory(@Value("${jms.broker-url}") String brokerUrl,
                                               @Value("${jms.user:}") String user,
                                               @Value("${jms.password:}") String password) {
            return JmsClientsExample.artemis(brokerUrl, user.isEmpty() ? null : user, password);
        }
    }
}
