package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ActiveMQ Classic com broker embutido ({@code vm://}, sem persistência): sem Docker. */
class ActiveMqAutoConfigurationTest {

    private static final String AMQ = "messaging.providers.amq.";
    private static final String PEDIDOS = "messaging.destinations.pedidos.";
    private static final String EVENTOS = "messaging.destinations.eventos.";

    // um broker por teste: o vm:// para quando a última conexão fecha
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class,
                    MessagingHealthAutoConfiguration.class))
            .withPropertyValues(AMQ + "type=activemq-classic",
                    AMQ + "broker-url=vm://" + UUID.randomUUID() + "?broker.persistent=false&broker.useJmx=false");

    @Test
    void queueAndVirtualTopicSubscriptionRoundTrip() {
        runner.withPropertyValues(
                        PEDIDOS + "provider=amq", PEDIDOS + "queue=pedidos", PEDIDOS + "dead-letter=pedidos-dlq",
                        PEDIDOS + "redelivery=reschedule", PEDIDOS + "lease=5s",
                        "messaging.destinations.pedidos-dlq.provider=amq",
                        "messaging.destinations.pedidos-dlq.queue=pedidos-dlq",
                        EVENTOS + "provider=amq", EVENTOS + "topic=VirtualTopic.eventos",
                        EVENTOS + "subscription=faturamento")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);

                    destinations.sender("pedidos").send(OutgoingMessage.ofText("pedido"));
                    try (MessageReceiver receiver = destinations.receiver("pedidos")) {
                        assertThat(receiver.capabilities().delayedRedelivery()).isTrue();
                        ReceivedMessage message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("pedido");
                        assertThat(message.leaseDuration()).isEqualTo(Duration.ofSeconds(5));
                        receiver.deadLetter(message, "teste");
                    }
                    try (MessageReceiver dlq = destinations.receiver("pedidos-dlq")) {
                        ReceivedMessage dead = dlq.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(dead.deadLetter().reason()).isEqualTo("teste");
                        dlq.ack(dead);
                    }

                    // o Virtual Topic só copia para filas Consumer.* que já existem
                    try (MessageReceiver subscription = destinations.receiver("eventos")) {
                        assertThat(subscription.receive(1, Duration.ZERO)).isEmpty();
                        destinations.sender("eventos").send(OutgoingMessage.ofText("evento"));
                        List<ReceivedMessage> received = subscription.receive(1, Duration.ofSeconds(5));
                        assertThat(received.getFirst().bodyAsString()).isEqualTo("evento");
                        subscription.ack(received.getFirst());
                    }
                });
    }

    @Test
    void healthChecksExistingAndMissingQueues() {
        runner.withPropertyValues(PEDIDOS + "provider=amq", PEDIDOS + "queue=pedidos",
                        "messaging.destinations.nada.provider=amq", "messaging.destinations.nada.queue=nao-existe",
                        "management.health.messaging.enabled=true")
                .run(context -> {
                    context.getBean(MessagingDestinations.class).sender("pedidos").send(OutgoingMessage.ofText("x"));

                    var health = context.getBean(HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails()).containsEntry("pedidos", "UP");
                    assertThat(health.getDetails().get("nada").toString()).startsWith("DestinationNotFoundException");
                });
    }

    @Test
    void activeMqRulesAreValidated() {
        runner.withPropertyValues(
                        "messaging.destinations.sem-endereco.provider=amq",
                        "messaging.destinations.topico-comum.provider=amq",
                        "messaging.destinations.topico-comum.topic=eventos",
                        "messaging.destinations.sem-topico.provider=amq",
                        "messaging.destinations.sem-topico.queue=x",
                        "messaging.destinations.sem-topico.subscription=s",
                        "messaging.destinations.lease-zero.provider=amq",
                        "messaging.destinations.lease-zero.queue=y",
                        "messaging.destinations.lease-zero.lease=0s",
                        "messaging.destinations.campo-errado.provider=amq",
                        "messaging.destinations.campo-errado.queue-url=http://x")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("sem-endereco: defina queue ou topic")
                        .hasMessageContaining("topico-comum.topic precisa ser um Virtual Topic")
                        .hasMessageContaining("sem-topico: subscription exige topic")
                        .hasMessageContaining("lease-zero.lease precisa ser positivo")
                        .hasMessageContaining("campo-errado.queue-url não se aplica"));
    }

    @Test
    void unreachableBrokerFailsStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
                .withPropertyValues(AMQ + "type=activemq-classic",
                        AMQ + "broker-url=tcp://localhost:1?connectionTimeout=500",
                        PEDIDOS + "provider=amq", PEDIDOS + "queue=pedidos")
                .run(context -> assertThat(context).getFailure().hasStackTraceContaining("não conectou ao broker"));
    }
}
