package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.aws.SnsMessageSender;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** Sem rede: os clientes do SDK só conectam no primeiro uso. */
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
                    assertThat(destinations.sender("eventos").capabilities().maxMessageBytes()).isEqualTo(1048576);

                    MessageReceiver first = destinations.receiver("pedidos");
                    MessageReceiver second = destinations.receiver("pedidos");
                    assertThat(first).isNotSameAs(second);
                    // DLQ por cópia configurada a partir de dead-letter
                    assertThat(first.capabilities().nativeDeadLetter()).isFalse();
                });
    }

    @Test
    void topicOnlyDestinationDoesNotReceive() {
        runner.withPropertyValues("messaging.destinations.eventos.provider=aws",
                        "messaging.destinations.eventos.topic-arn=arn:aws:sns:us-east-1:000000000000:eventos")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    assertThat(destinations.sender("eventos").capabilities().maxMessageBytes())
                            .isEqualTo(SnsMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
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
    void requiredCapabilitiesAreCheckedAtStartup() {
        runner.withPropertyValues("messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos",
                        "messaging.destinations.pedidos.require=ordered-delivery,lease-expired-on-ack")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("pedidos.require ORDERED_DELIVERY: o sender não suporta")
                        .hasMessageContaining("pedidos.require LEASE_EXPIRED_ON_ACK: o receiver não suporta"));

        runner.withPropertyValues("messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos.fifo",
                        "messaging.destinations.pedidos.require=ordered-delivery,delayed-redelivery")
                .run(context -> assertThat(context).hasNotFailed());
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
                        .hasMessageContaining("sb.dead-letter: o Service Bus tem DLQ nativa")
                        .hasMessageContaining("sb.redelivery=RESCHEDULE só vale para fila sem sessions")
                        .hasMessageContaining("ps.ack-deadline é obrigatório")
                        .hasMessageContaining("ps: nome curto exige messaging.providers.gcp.project"));
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
}
