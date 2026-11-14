package com.example.messaging.spring;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import org.junit.jupiter.api.Test;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A regra do bean de cliente da aplicação (ADR-0010). Sem rede: nenhum cliente é chamado de verdade. */
class ClientBeansTest {

    private static final String URL = "http://localhost:4566/0/q";
    private static final String AWS_FIELDS_A = "messaging.providers.a.";
    private static final String AWS_FIELDS_B = "messaging.providers.b.";

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
    void awsBeanIsUsedWhenTheProviderHasNoConnectionFields() {
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
    void awsBeanWithConnectionFieldsFailsTheStartup() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws", "messaging.providers.aws.region=us-east-1")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean(SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.providers.aws")
                        .hasMessageContaining("SqsClient e SnsClient"));
    }

    @Test
    void awsBeanForOneClientLeavesThePropertiesToTheOther() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws", "messaging.providers.aws.region=us-east-1")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    context.getBean(MessagingDestinations.class).sender("s", null, named("nome-da-fila"));
                    assertThat(calls).contains("getQueueUrl");
                });
    }

    @Test
    void awsUnnamedBeanIsIgnoredWithTwoProviders() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues(
                        AWS_FIELDS_A + "type=aws", AWS_FIELDS_A + "region=us-east-1",
                        AWS_FIELDS_A + "endpoint=http://localhost:1", AWS_FIELDS_A + "access-key=test",
                        AWS_FIELDS_A + "secret-key=test",
                        AWS_FIELDS_B + "type=aws", AWS_FIELDS_B + "region=us-east-1",
                        AWS_FIELDS_B + "endpoint=http://localhost:1", AWS_FIELDS_B + "access-key=test",
                        AWS_FIELDS_B + "secret-key=test")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // o cliente do starter não alcança o endpoint; o bean sem nome responderia
                    assertThatThrownBy(() -> context.getBean(MessagingDestinations.class)
                            .sender("s", "a", named("nome-da-fila")));
                    assertThat(calls).isEmpty();
                });
    }

    @Test
    void awsBeanNamedAfterTheProviderIsUsedWithTwoProviders() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues(
                        AWS_FIELDS_A + "type=aws",
                        AWS_FIELDS_B + "type=aws", AWS_FIELDS_B + "region=us-east-1",
                        AWS_FIELDS_B + "endpoint=http://localhost:1", AWS_FIELDS_B + "access-key=test",
                        AWS_FIELDS_B + "secret-key=test")
                .withBean("a", SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean("aSnsClient", SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    context.getBean(MessagingDestinations.class).sender("s", "a", named("nome-da-fila"));
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
    void azureUnnamedBeanIsIgnoredWithTwoProviders() {
        runner.withPropertyValues("messaging.providers.a1.type=azure", "messaging.providers.a2.type=azure")
                .withBean(ServiceBusClientBuilder.class, ServiceBusClientBuilder::new)
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("defina connection-string ou declare um bean")
                        .hasMessageMatching("(?s).*a[12]ServiceBusClientBuilder.*"));
    }

    @Test
    void azureBeanNamedAfterTheProviderIsUsedWithTwoProviders() {
        runner.withPropertyValues("messaging.providers.a1.type=azure", "messaging.providers.a2.type=azure",
                        "messaging.providers.a2.connection-string=" + AZURE)
                .withBean("a1", ServiceBusClientBuilder.class, ServiceBusClientBuilder::new)
                .run(context -> assertThat(context).hasNotFailed());
    }

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
    void jmsNamedBeanIsUsedWithTwoProviders() {
        runner.withPropertyValues("messaging.providers.j1.type=artemis", "messaging.providers.j2.type=activemq-classic")
                .withBean("j1", ConnectionFactory.class, () -> jms("BEAN-USED"))
                .withBean("j2", ConnectionFactory.class, () -> jms(null))
                .run(context -> assertThat(context).getFailure().rootCause().isNotNull()
                        .hasMessageContaining("BEAN-USED"));
    }

    @Test
    void jmsUnnamedBeanIsIgnoredWithTwoProviders() {
        runner.withPropertyValues("messaging.providers.j1.type=artemis", "messaging.providers.j2.type=activemq-classic")
                .withBean(ConnectionFactory.class, () -> jms("BEAN-USED"))
                .withBean("j2", ConnectionFactory.class, () -> jms(null))
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.providers.j1: defina broker-url ou declare um bean")
                        .hasMessageContaining("j1ConnectionFactory"));
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
    void rabbitNamedBeanIsUsedWithTwoProviders() {
        runner.withPropertyValues("messaging.providers.r1.type=rabbitmq", "messaging.providers.r2.type=rabbitmq")
                .withBean("r1", com.rabbitmq.client.ConnectionFactory.class, () -> rabbit("BEAN-USED"))
                .withBean("r2", com.rabbitmq.client.ConnectionFactory.class, () -> rabbit(null))
                .run(context -> assertThat(context).getFailure().hasStackTraceContaining(
                        "messaging.providers.r1: não conectou ao broker: BEAN-USED"));
    }

    @Test
    void rabbitUnnamedBeanIsIgnoredWithTwoProviders() {
        runner.withPropertyValues("messaging.providers.r1.type=rabbitmq", "messaging.providers.r2.type=rabbitmq")
                .withBean(com.rabbitmq.client.ConnectionFactory.class, () -> rabbit("BEAN-USED"))
                .withBean("r2", com.rabbitmq.client.ConnectionFactory.class, () -> rabbit(null))
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.providers.r1: defina broker-url ou declare um bean")
                        .hasMessageContaining("r1ConnectionFactory"));
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

    /** Com dois providers e um bean sem nome, o provider sem campos de conexão cria o cliente e avisa que ignorou o bean. */
    @Test
    void awsIgnoredBeanIsWarnedAbout() {
        java.util.List<String> logged = new CopyOnWriteArrayList<>();
        java.util.logging.Logger jul = java.util.logging.Logger.getLogger(
                "com.example.messaging.spring.aws.AwsProviderConnection");
        java.util.logging.Handler handler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord r) {
                logged.add(r.getLevel() + " " + new java.util.logging.SimpleFormatter().formatMessage(r));
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        jul.addHandler(handler);
        try {
            runner.withPropertyValues(AWS_FIELDS_A + "type=aws", AWS_FIELDS_A + "region=us-east-1",
                            AWS_FIELDS_B + "type=aws", AWS_FIELDS_B + "region=us-east-1")
                    .withBean(SqsClient.class, () -> counting(SqsClient.class, new CopyOnWriteArrayList<>()))
                    .run(context -> assertThat(context).hasNotFailed());
        } finally {
            jul.removeHandler(handler);
        }
        assertThat(logged).anyMatch(m -> m.startsWith("WARNING") && m.contains("SqsClient") && m.contains("'a' ou 'aSqsClient'"));
    }
}
