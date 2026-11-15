package com.example.messaging.spring;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A regra do bean de cliente da aplicação (ADR-0011). Sem rede: nenhum cliente é chamado de verdade. */
class ClientBeansTest {

    private static final String URL = "http://localhost:4566/0/q";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class));

    private static Endpoint named(String queue) {
        return new Endpoint(queue, null, null, null);
    }

    /** Um cliente que só anota os métodos chamados; {@code getQueueUrl} devolve {@link #URL}. */
    private static <T> T counting(Class<T> type, List<String> calls) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            calls.add(method.getName());
            return method.getName().equals("getQueueUrl") ? GetQueueUrlResponse.builder().queueUrl(URL).build() : null;
        }));
    }

    @Test
    void awsBeanIsUsed() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean(SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> {
                    context.getBean(MessagingDestinations.class).sender("s", null, named("nome-da-fila"));
                    assertThat(calls).contains("getQueueUrl");
                });
    }

    @Test
    void starterNeverClosesTheApplicationsClient() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean(SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    destinations.sender("s", null, named("nome-da-fila"));
                    destinations.close();
                    assertThat(calls).doesNotContain("close");
                });
    }

    private static final String AZURE = "Endpoint=sb://localhost;SharedAccessKeyName=k;SharedAccessKey=v;"
            + "UseDevelopmentEmulator=true;";

    @Test
    void azureConnectionStringWinsOverTheBean() {
        runner.withPropertyValues("messaging.providers.a.type=azure", "messaging.providers.a.connection-string=" + AZURE)
                .withBean(ServiceBusClientBuilder.class, ServiceBusClientBuilder::new)
                .run(context -> assertThat(context).hasNotFailed());
    }

    /** Uma fábrica JMS que falha com {@code failure} ({@code BEAN-USED} na mensagem) ou, sem ela, conecta num stub. */
    private static ConnectionFactory jms(String failure) {
        Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> null);
        return (ConnectionFactory) Proxy.newProxyInstance(ConnectionFactory.class.getClassLoader(),
                new Class<?>[]{ConnectionFactory.class}, (proxy, method, args) -> {
                    if (failure != null) {
                        throw new JMSException(failure);
                    }
                    return connection;
                });
    }

    @Test
    void jmsBrokerUrlWinsOverAnUnnamedBean() {
        runner.withPropertyValues("messaging.providers.j.type=activemq-classic",
                        "messaging.providers.j.broker-url=tcp://localhost:1?connectionTimeout=500")
                .withBean(ConnectionFactory.class, () -> jms("BEAN-USED"))
                .run(context -> assertThat(context).getFailure()
                        .hasStackTraceContaining("não conectou ao broker")
                        .satisfies(e -> assertThat(e.toString()).doesNotContain("BEAN-USED")));
    }

    /** Uma fábrica RabbitMQ que falha com {@code failure} ({@code BEAN-USED} na mensagem) ou, sem ela, conecta num stub. */
    private static com.rabbitmq.client.ConnectionFactory rabbit(String failure) {
        com.rabbitmq.client.Connection connection = (com.rabbitmq.client.Connection) Proxy.newProxyInstance(
                com.rabbitmq.client.Connection.class.getClassLoader(),
                new Class<?>[]{com.rabbitmq.client.Connection.class}, (proxy, method, args) -> null);
        return new com.rabbitmq.client.ConnectionFactory() {
            @Override
            public com.rabbitmq.client.Connection newConnection(String connectionName) throws IOException {
                if (failure != null) {
                    throw new IOException(failure);
                }
                return connection;
            }
        };
    }

    @Test
    void rabbitBrokerUrlWinsOverAnUnnamedBean() {
        runner.withPropertyValues("messaging.providers.r.type=rabbitmq",
                        "messaging.providers.r.broker-url=amqp://localhost:1")
                .withBean(com.rabbitmq.client.ConnectionFactory.class, () -> rabbit("BEAN-USED"))
                .run(context -> assertThat(context).getFailure()
                        .hasStackTraceContaining("não conectou ao broker")
                        .satisfies(e -> assertThat(e.toString()).doesNotContain("BEAN-USED"))
                        .rootCause().hasMessageNotContaining("BEAN-USED"));
    }


    private static <T> void register(DefaultListableBeanFactory factory, String name, Class<T> type, T bean,
                                     boolean primary) {
        RootBeanDefinition definition = new RootBeanDefinition(type, () -> bean);
        definition.setPrimary(primary);
        factory.registerBeanDefinition(name, definition);
    }

    @Test
    void missingBeanFailsWithTheProviderKey() {
        assertThatThrownBy(() -> ClientBeans.require(new DefaultListableBeanFactory(), SqsClient.class, "aws"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("messaging.providers.aws: declare um bean software.amazon.awssdk.services.sqs.SqsClient"
                        + " (ADR-0011)");
    }

    @Test
    void twoBeansWithoutPrimaryFail() {
        DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
        register(factory, "one", SqsClient.class, counting(SqsClient.class, new ArrayList<>()), false);
        register(factory, "two", SqsClient.class, counting(SqsClient.class, new ArrayList<>()), false);

        assertThatThrownBy(() -> ClientBeans.require(factory, SqsClient.class, "aws"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("messaging.providers.aws")
                .hasMessageContaining("marque um com @Primary");
    }

    @Test
    void primaryBeanWins() {
        DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
        SqsClient other = counting(SqsClient.class, new ArrayList<>());
        SqsClient primary = counting(SqsClient.class, new ArrayList<>());
        register(factory, "one", SqsClient.class, other, false);
        register(factory, "two", SqsClient.class, primary, true);

        assertThat(ClientBeans.require(factory, SqsClient.class, "aws")).isSameAs(primary);
    }

    @Test
    void optionalReturnsNullWithoutBean() {
        assertThat(ClientBeans.optional(new DefaultListableBeanFactory(), SqsClient.class, "aws")).isNull();
    }

    @Test
    void secondProviderOfTheSameGroupFails() {
        runner.withPropertyValues("messaging.providers.a.type=aws", "messaging.providers.b.type=aws")
                .run(context -> assertThat(context).getFailure().rootCause().hasMessageContaining(
                        "messaging.providers: a e b usam o mesmo cliente nativo; um provider por tipo (ADR-0011)"));
    }

    @Test
    void jmsTypesShareOneGroup() {
        runner.withPropertyValues("messaging.providers.j1.type=artemis",
                        "messaging.providers.j2.type=activemq-classic")
                .run(context -> assertThat(context).getFailure().rootCause().hasMessageContaining(
                        "messaging.providers: j1 e j2 usam o mesmo cliente nativo; um provider por tipo (ADR-0011)"));
    }

    @Test
    void providersOfDifferentGroupsCoexist() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws", "messaging.providers.azure.type=azure")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean(SnsClient.class, () -> counting(SnsClient.class, calls))
                .withBean(ServiceBusClientBuilder.class, ServiceBusClientBuilder::new)
                .run(context -> assertThat(context).hasNotFailed());
    }
}
