package com.example.messaging.spring.rabbitmq;

import com.example.messaging.MessageReceiver;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.spring.MessagingProperties.Options;
import com.example.messaging.spring.MessagingProperties.Requirement;
import java.util.Set;
import com.example.messaging.spring.MessagingHealthAutoConfiguration;
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
                        "messaging.senders.pedidos.destination=pedidos", "messaging.senders.pedidos.options.confirm-timeout=10s",
                        "messaging.senders.eventos.destination=eventos", "messaging.senders.eventos.kind=topic",
                        "messaging.senders.eventos.options.routing-key=pedido.criado")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    Endpoint pedidos = new Endpoint("pedidos", null, null, new Options(
                            Set.of(Requirement.LEASE_EXPIRED_ON_ACK), null, null, null, null, null, null, null,
                            Duration.ofSeconds(5), null, null, null));

                    destinations.sender("pedidos").send(OutgoingMessage.ofText("pedido"));
                    try (MessageReceiver receiver = destinations.receiver("pedidos", null, pedidos, null)) {
                        ReceivedMessage message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("pedido");
                        assertThat(message.leaseDuration()).isEqualTo(Duration.ofSeconds(5));
                        receiver.ack(message);
                    }

                    // routing key pedido.criado casa com o binding pedido.*
                    destinations.sender("eventos").send(OutgoingMessage.ofText("evento"));
                    try (MessageReceiver subscription = destinations.receiver("eventos", null,
                            new Endpoint("eventos", "faturamento", null, null), null)) {
                        ReceivedMessage message = subscription.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("evento");
                        subscription.ack(message);
                    }
                });
    }

    @Test
    void healthChecksExistingAndMissingQueues() {
        runner().withPropertyValues("messaging.senders.pedidos.destination=pedidos", "management.health.messaging.enabled=true")
                .run(context -> {
                    context.getBean(MessagingDestinations.class)
                            .receiver("nada", null, new Endpoint("nao-existe", null, null, null), null).close();
                    var health = context.getBean(HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails()).containsEntry("pedidos", "UP");
                    assertThat(health.getDetails().get("nada").toString()).startsWith("DestinationNotFoundException");
                });
    }

    @Test
    void rabbitRulesAreValidated() {
        runner().withPropertyValues(
                        "messaging.senders.atraso.destination=pedidos", "messaging.senders.atraso.options.redelivery=reschedule",
                        "messaging.senders.chave-em-fila.destination=pedidos", "messaging.senders.chave-em-fila.options.routing-key=x",
                        "messaging.senders.duas-chaves.destination=eventos", "messaging.senders.duas-chaves.kind=topic",
                        "messaging.senders.duas-chaves.options.routing-key=x", "messaging.senders.duas-chaves.options.routing-by-ordering-key=true",
                        "messaging.senders.por-chave-em-fila.destination=pedidos",
                        "messaging.senders.por-chave-em-fila.options.routing-by-ordering-key=true",
                        "messaging.senders.confirm-zero.destination=pedidos", "messaging.senders.confirm-zero.options.confirm-timeout=0s")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("atraso.options.redelivery=RESCHEDULE: o RabbitMQ não tem atraso")
                        .hasMessageContaining("chave-em-fila.options.routing-key só vale para o RabbitMQ com kind: topic")
                        .hasMessageContaining("duas-chaves.options.routing-by-ordering-key não combina com routing-key")
                        .hasMessageContaining("por-chave-em-fila.options.routing-by-ordering-key só vale para o RabbitMQ com kind: topic")
                        .hasMessageContaining("confirm-zero.options.confirm-timeout precisa ser positivo"));
    }
}
