package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import jakarta.jms.Connection;
import jakarta.jms.Session;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
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
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tipo {@code artemis} contra um broker real. Pulado sem Docker. */
@Testcontainers(disabledWithoutDocker = true)
class ArtemisAutoConfigurationTest {

    @Container
    private static final GenericContainer<?> ARTEMIS = new GenericContainer<>("apache/activemq-artemis:2.40.0")
            .withExposedPorts(61616)
            .waitingFor(Wait.forLogMessage(".*AMQ221007.*", 1));

    private static final String ART = "messaging.providers.art.";

    private static String url() {
        return "tcp://" + ARTEMIS.getHost() + ":" + ARTEMIS.getMappedPort(61616);
    }

    /** A fila multicast da subscription existe antes do envio, como o Terraform deixaria. */
    @BeforeAll
    static void createSubscription() throws Exception {
        try (Connection connection = new ActiveMQConnectionFactory(url()).createConnection("artemis", "artemis");
             Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            ClientSession core = ((ActiveMQSession) session).getCoreSession();
            core.createAddress(SimpleString.of("eventos"), EnumSet.of(RoutingType.MULTICAST), false);
            core.createQueue(QueueConfiguration.of("faturamento").setAddress("eventos")
                    .setRoutingType(RoutingType.MULTICAST).setDurable(true));
        }
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class,
                        MessagingHealthAutoConfiguration.class))
                .withPropertyValues(ART + "type=artemis", ART + "broker-url=" + url(), ART + "user=artemis",
                        ART + "password=artemis");
    }

    @Test
    void queueAndMulticastSubscriptionRoundTrip() {
        runner().withPropertyValues(
                        "messaging.destinations.pedidos.provider=art", "messaging.destinations.pedidos.queue=pedidos",
                        "messaging.destinations.pedidos.require=publisher-deduplication,delayed-redelivery",
                        "messaging.destinations.pedidos.redelivery=reschedule",
                        "messaging.destinations.eventos.provider=art", "messaging.destinations.eventos.topic=eventos",
                        "messaging.destinations.eventos.subscription=faturamento")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);

                    destinations.sender("pedidos").send(OutgoingMessage.ofText("pedido"));
                    try (MessageReceiver receiver = destinations.receiver("pedidos")) {
                        ReceivedMessage message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("pedido");
                        receiver.ack(message);
                    }

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
                        "messaging.destinations.eventos.provider=art", "messaging.destinations.eventos.topic=eventos",
                        "messaging.destinations.eventos.subscription=faturamento",
                        "messaging.destinations.nada.provider=art", "messaging.destinations.nada.queue=nao-existe",
                        "management.health.messaging.enabled=true")
                .run(context -> {
                    var health = context.getBean(HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails()).containsEntry("eventos", "UP");
                    assertThat(health.getDetails().get("nada").toString()).startsWith("DestinationNotFoundException");
                });
    }
}
