package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tipo {@code rabbitmq} contra um broker real. Pulado sem Docker. */
@Testcontainers(disabledWithoutDocker = true)
class RabbitAutoConfigurationTest {

    @Container
    private static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:4.1-management-alpine")
            .withExposedPorts(5672)
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1));

    private static final String RMQ = "messaging.providers.rmq.";

    private static String url() {
        return "amqp://" + RABBIT.getHost() + ":" + RABBIT.getMappedPort(5672);
    }

    /** Topologia que o Terraform deixaria: filas quorum e um topic exchange com a fila ligada. */
    @BeforeAll
    static void createTopology() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(url());
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            Map<String, Object> quorum = Map.of("x-queue-type", "quorum");
            channel.queueDeclare("pedidos", true, false, false, quorum);
            channel.queueDeclare("faturamento", true, false, false, quorum);
            channel.exchangeDeclare("eventos", "topic", true);
            channel.queueBind("faturamento", "eventos", "pedido.*");
        }
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class,
                        MessagingHealthAutoConfiguration.class))
                .withPropertyValues(RMQ + "type=rabbitmq", RMQ + "broker-url=" + url(), RMQ + "user=guest",
                        RMQ + "password=guest");
    }

    @Test
    void queueAndExchangeSubscriptionRoundTrip() {
        runner().withPropertyValues(
                        "messaging.destinations.pedidos.provider=rmq", "messaging.destinations.pedidos.queue=pedidos",
                        "messaging.destinations.pedidos.lease=5s",
                        "messaging.destinations.pedidos.require=lease-expired-on-ack",
                        "messaging.destinations.eventos.provider=rmq", "messaging.destinations.eventos.topic=eventos",
                        "messaging.destinations.eventos.routing-key=pedido.criado",
                        "messaging.destinations.eventos.subscription=faturamento")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);

                    destinations.sender("pedidos").send(OutgoingMessage.ofText("pedido"));
                    try (MessageReceiver receiver = destinations.receiver("pedidos")) {
                        ReceivedMessage message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("pedido");
                        assertThat(message.leaseDuration()).isEqualTo(Duration.ofSeconds(5));
                        receiver.ack(message);
                    }

                    // routing key pedido.criado casa com o binding pedido.*
                    destinations.sender("eventos").send(OutgoingMessage.ofText("evento"));
                    try (MessageReceiver subscription = destinations.receiver("eventos")) {
                        ReceivedMessage message = subscription.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("evento");
                        subscription.ack(message);
                    }
                });
    }

    @Test
    void healthChecksExistingAndMissingQueues() {
        runner().withPropertyValues(
                        "messaging.destinations.pedidos.provider=rmq", "messaging.destinations.pedidos.queue=pedidos",
                        "messaging.destinations.nada.provider=rmq", "messaging.destinations.nada.queue=nao-existe",
                        "management.health.messaging.enabled=true")
                .run(context -> {
                    var health = context.getBean(HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails()).containsEntry("pedidos", "UP");
                    assertThat(health.getDetails().get("nada").toString()).startsWith("DestinationNotFoundException");
                });
    }

    @Test
    void rabbitRulesAreValidated() {
        runner().withPropertyValues(
                        "messaging.destinations.atraso.provider=rmq", "messaging.destinations.atraso.queue=pedidos",
                        "messaging.destinations.atraso.redelivery=reschedule",
                        "messaging.destinations.chave-em-fila.provider=rmq",
                        "messaging.destinations.chave-em-fila.queue=pedidos",
                        "messaging.destinations.chave-em-fila.routing-key=x")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("atraso.redelivery=RESCHEDULE: o RabbitMQ não tem atraso")
                        .hasMessageContaining("chave-em-fila.routing-key só vale para o RabbitMQ com topic"));
    }
}
