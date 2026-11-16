package com.example.messaging.spring.exemplos;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * As classes de exemplo de cada provedor (ADR-0011) montam o que dizem montar. Sem rede: os clientes só são construídos.
 * Os exemplos de JMS e RabbitMQ também rodam nos testes de auto-configuração de cada broker.
 */
class ExemplosTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner();

    @BeforeEach
    void awsRegion() {
        System.setProperty("aws.region", "us-east-1");
    }

    @AfterEach
    void clear() {
        System.clearProperty("aws.region");
    }

    @Test
    void awsProducaoDeclaraSqsESnsComOMesmoProviderDeCredenciais() {
        runner.withUserConfiguration(AwsClientsExample.Producao.class).run(context -> {
            assertThat(context).hasSingleBean(SqsClient.class).hasSingleBean(SnsClient.class);
            AwsCredentialsProvider credentials = context.getBean(AwsCredentialsProvider.class);
            assertThat(context.getBean(SqsClient.class).serviceClientConfiguration().credentialsProvider())
                    .isSameAs(credentials);
            assertThat(context.getBean(SnsClient.class).serviceClientConfiguration().credentialsProvider())
                    .isSameAs(credentials);
        });
    }

    @Test
    void awsEmuladorApontaParaOLocalStack() {
        runner.withUserConfiguration(AwsClientsExample.Emulador.class).run(context ->
                assertThat(context.getBean(SqsClient.class).serviceClientConfiguration().endpointOverride())
                        .hasValueSatisfying(endpoint -> assertThat(endpoint.toString()).isEqualTo("http://localhost:4566")));
    }

    @Test
    void azureProducaoUsaOEntraId() {
        runner.withUserConfiguration(AzureClientsExample.Producao.class)
                .withPropertyValues("azure.servicebus.namespace=meu.servicebus.windows.net")
                .run(context -> assertThat(context).hasSingleBean(ServiceBusClientBuilder.class));
    }

    @Test
    void azureEmuladorUsaAConnectionStringDoEmulator() {
        runner.withUserConfiguration(AzureClientsExample.Emulador.class)
                .run(context -> assertThat(context).hasSingleBean(ServiceBusClientBuilder.class));
    }

    @Test
    void gcpProducaoNaoDeclaraNada() {
        runner.withUserConfiguration(GcpClientsExample.Producao.class).run(context ->
                assertThat(context).doesNotHaveBean(TransportChannelProvider.class)
                        .doesNotHaveBean(CredentialsProvider.class));
    }

    @Test
    void gcpEmuladorDeclaraCanalECredencialNulaEFechaOCanalComOContexto() {
        AtomicReference<ManagedChannel> channel = new AtomicReference<>();
        runner.withUserConfiguration(GcpClientsExample.Emulador.class).run(context -> {
            assertThat(context).hasSingleBean(TransportChannelProvider.class).hasSingleBean(CredentialsProvider.class);
            channel.set(context.getBean(ManagedChannel.class));
            assertThat(channel.get().isShutdown()).isFalse();
        });
        assertThat(channel.get().isShutdown()).isTrue();
    }

    @Test
    void jmsAplicaAsCredenciaisNaFactoryDoCliente() {
        org.apache.activemq.ActiveMQConnectionFactory classic = (org.apache.activemq.ActiveMQConnectionFactory)
                JmsClientsExample.activeMqClassic("tcp://amq:61616", "u", "p");
        assertThat(classic.getUserName()).isEqualTo("u");
        assertThat(classic.getPassword()).isEqualTo("p");
        // o dialeto embrulha a URL em failover:(...) para a conexão sobreviver a um restart do broker
        assertThat(classic.getBrokerURL()).startsWith("failover:");

        org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory artemis =
                (org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory)
                        JmsClientsExample.artemis("tcp://artemis:61616", "u", "p");
        assertThat(artemis.getUser()).isEqualTo("u");
        assertThat(artemis.getPassword()).isEqualTo("p");
        assertThat(artemis.getServerLocator().getConsumerWindowSize()).isZero();
    }

    @Test
    void rabbitAplicaUriUsuarioESenha() {
        com.rabbitmq.client.ConnectionFactory factory =
                RabbitClientsExample.connectionFactory("amqp://rabbit:5672/vhost", "u", "p");
        assertThat(factory.getHost()).isEqualTo("rabbit");
        assertThat(factory.getPort()).isEqualTo(5672);
        assertThat(factory.getVirtualHost()).isEqualTo("vhost");
        assertThat(factory.getUsername()).isEqualTo("u");
        assertThat(factory.getPassword()).isEqualTo("p");
    }
}
