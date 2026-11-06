package com.example.messaging.spring.jms;

import com.example.messaging.MessageReceiver;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import com.example.messaging.spring.MessagingHealthAutoConfiguration;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.spring.MessagingProperties.Options;
import com.example.messaging.spring.MessagingProperties.Redelivery;
import com.example.messaging.spring.MessagingProperties.Requirement;
import java.util.Set;
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
                        "messaging.senders.pedidos.destination=pedidos",
                        "messaging.senders.pedidos.options.require=publisher-deduplication",
                        "messaging.senders.eventos.destination=eventos", "messaging.senders.eventos.kind=topic")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    Endpoint pedidos = new Endpoint("pedidos", null, null, new Options(
                            Set.of(Requirement.DELAYED_REDELIVERY), null, Redelivery.RESCHEDULE, null, null, null,
                            null, null, null, null, null, null));

                    destinations.sender("pedidos").send(OutgoingMessage.ofText("pedido"));
                    try (MessageReceiver receiver = destinations.receiver("pedidos", null, pedidos, null)) {
                        ReceivedMessage message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("pedido");
                        receiver.ack(message);
                    }

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
        runner().withPropertyValues("management.health.messaging.enabled=true")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    destinations.receiver("eventos", null, new Endpoint("eventos", "faturamento", null, null), null)
                            .close();
                    destinations.receiver("nada", null, new Endpoint("nao-existe", null, null, null), null).close();

                    var health = context.getBean(HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails()).containsEntry("eventos", "UP");
                    assertThat(health.getDetails().get("nada").toString()).startsWith("DestinationNotFoundException");
                });
    }
}
