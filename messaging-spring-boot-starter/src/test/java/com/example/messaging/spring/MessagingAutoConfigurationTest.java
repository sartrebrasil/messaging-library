package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regras de {@code messaging.*} e ligação por nome, sobre o {@link InMemoryProviderFactory}; o
 * comportamento de cada SDK é testado no adapter ({@code *AutoConfigurationTest}).
 */
class MessagingAutoConfigurationTest {

    private static final String AWS = "messaging.providers.aws.";
    private static final String QUEUE = "http://localhost:4566/000000000000/";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class,
                    MessagingHealthAutoConfiguration.class))
            .withPropertyValues(AWS + "type=aws", AWS + "region=us-east-1", AWS + "endpoint=http://localhost:4566",
                    AWS + "access-key=test", AWS + "secret-key=test");

    @Test
    void createsSendersAndReceiversByName() {
        runner.withPropertyValues(
                        "messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos",
                        "messaging.destinations.pedidos.dead-letter=pedidos-dlq",
                        "messaging.destinations.pedidos-dlq.provider=aws",
                        "messaging.destinations.pedidos-dlq.queue-url=" + QUEUE + "pedidos-dlq",
                        "messaging.destinations.eventos.provider=aws",
                        "messaging.destinations.eventos.topic-arn=arn:aws:sns:us-east-1:000000000000:eventos",
                        "messaging.destinations.eventos.max-message-bytes=1048576")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    assertThat(destinations.names()).containsExactly("eventos", "pedidos", "pedidos-dlq");

                    MessageSender sender = destinations.sender("pedidos");
                    assertThat(destinations.sender("pedidos")).isSameAs(sender);

                    MessageReceiver first = destinations.receiver("pedidos");
                    MessageReceiver second = destinations.receiver("pedidos");
                    assertThat(first).isNotSameAs(second);
                });
    }

    @Test
    void topicOnlyDestinationDoesNotReceive() {
        runner.withPropertyValues("messaging.destinations.eventos.provider=aws",
                        "messaging.destinations.eventos.topic-arn=arn:aws:sns:us-east-1:000000000000:eventos")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    assertThat(destinations.sender("eventos")).isNotNull();
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> destinations.receiver("eventos"))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("não recebe");
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> destinations.sender("nao-existe"))
                            .isInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void invalidConfigurationFailsStartupListingEveryProblem() {
        runner.withPropertyValues(
                        "messaging.destinations.sem-provider.provider=nao-existe",
                        "messaging.destinations.sem-endereco.provider=aws",
                        "messaging.destinations.sessions.provider=aws",
                        "messaging.destinations.sessions.queue-url=" + QUEUE + "x",
                        "messaging.destinations.sessions.sessions=true",
                        "messaging.destinations.dlq-inexistente.provider=aws",
                        "messaging.destinations.dlq-inexistente.queue-url=" + QUEUE + "y",
                        "messaging.destinations.dlq-inexistente.dead-letter=nada",
                        "messaging.destinations.campo-errado.provider=aws",
                        "messaging.destinations.campo-errado.queue-url=" + QUEUE + "z",
                        "messaging.destinations.campo-errado.subscription=s")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("sem-provider.provider 'nao-existe' não existe")
                        .hasMessageContaining("sem-endereco: defina queue-url ou topic-arn")
                        .hasMessageContaining("sessions.sessions só vale para o Service Bus")
                        .hasMessageContaining("dlq-inexistente.dead-letter 'nada' não existe")
                        .hasMessageContaining("campo-errado.subscription não se aplica"));
    }

    @Test
    void azureAndGcpRulesAreValidated() {
        runner.withPropertyValues(
                        "messaging.providers.azure.type=azure",
                        "messaging.providers.azure.connection-string=Endpoint=sb://localhost;SharedAccessKeyName=k;"
                                + "SharedAccessKey=v;UseDevelopmentEmulator=true;",
                        "messaging.providers.gcp.type=gcp",
                        "messaging.providers.gcp.emulator-host=localhost:8085",
                        "messaging.destinations.sb.provider=azure",
                        "messaging.destinations.sb.queue=pedidos",
                        "messaging.destinations.sb.sessions=true",
                        "messaging.destinations.sb.redelivery=reschedule",
                        "messaging.destinations.sb.dead-letter=outro",
                        "messaging.destinations.ps.provider=gcp",
                        "messaging.destinations.ps.subscription=pedidos-sub")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("sb.dead-letter: o Service Bus tem DLQ nativa; leia-a com dead-letter-queue")
                        .hasMessageContaining("sb.redelivery=RESCHEDULE só vale para fila sem sessions")
                        .hasMessageContaining("ps.ack-deadline é obrigatório")
                        .hasMessageContaining("ps: nome curto exige messaging.providers.gcp.project"));
    }

    @Test
    void azureDeadLetterQueueRulesAreValidated() {
        ApplicationContextRunner azure = runner.withPropertyValues(
                "messaging.providers.azure.type=azure",
                "messaging.providers.azure.connection-string=Endpoint=sb://localhost;SharedAccessKeyName=k;"
                        + "SharedAccessKey=v;UseDevelopmentEmulator=true;");

        azure.withPropertyValues(
                        "messaging.destinations.sem-sub.provider=azure",
                        "messaging.destinations.sem-sub.topic=eventos",
                        "messaging.destinations.sem-sub.dead-letter-queue=true",
                        "messaging.destinations.sqs.provider=aws",
                        "messaging.destinations.sqs.queue-url=" + QUEUE + "x",
                        "messaging.destinations.sqs.dead-letter-queue=true")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("sem-sub.dead-letter-queue em tópico exige subscription")
                        .hasMessageContaining("sqs.dead-letter-queue só vale para o Service Bus"));
    }

    @Test
    void healthIndicatorIsOptIn() {
        runner.withPropertyValues("messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos")
                .run(context -> assertThat(context).doesNotHaveBean(HealthIndicator.class));

        runner.withPropertyValues("messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos",
                        "management.health.messaging.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(HealthIndicator.class));
    }

    @Test
    void typeWithoutAdapterAsksForTheDependency() {
        runner.withPropertyValues("messaging.providers.rmq.type=rabbitmq")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.providers.rmq: adicione a dependência messaging-rabbitmq"));
    }

    @Test
    void twoAdaptersForOneTypeAreReported() {
        List<String> problems = new ArrayList<>();
        ProviderFactory other = new ProviderFactory() {
            @Override
            public Set<MessagingProperties.Type> types() {
                return Set.of(MessagingProperties.Type.AWS);
            }

            @Override
            public ProviderConnection connect(String name, MessagingProperties.Provider provider,
                                              org.springframework.beans.factory.ListableBeanFactory beans) {
                throw new AssertionError("não conecta");
            }
        };

        assertThat(MessagingDestinations.byType(List.of(new InMemoryProviderFactory(), other), problems))
                .containsOnlyKeys(MessagingProperties.Type.AWS, MessagingProperties.Type.AZURE,
                        MessagingProperties.Type.GCP);
        assertThat(problems).singleElement().asString().contains("dois adapters atendem o tipo AWS");
    }
}
