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
import com.example.messaging.MessageSender;
import com.example.messaging.jms.JmsDialect;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ActiveMQ Classic com broker embutido ({@code vm://}, sem persistência): sem Docker. */
class ActiveMqAutoConfigurationTest {

    private static final String AMQ = "messaging.providers.amq.";
    private static final String PEDIDOS = "messaging.senders.pedidos.";

    // um broker por teste: o vm:// para quando a última conexão fecha
    private final String vm = "vm://" + UUID.randomUUID() + "?broker.persistent=false&broker.useJmx=false";

    private final ApplicationContextRunner base = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class,
                    MessagingHealthAutoConfiguration.class))
            .withPropertyValues(AMQ + "type=activemq-classic");

    /** O {@code ConnectionFactory} que a aplicação declararia (ADR-0011). */
    private final ApplicationContextRunner runner = base
            .withBean(ConnectionFactory.class, () -> JmsDialect.ACTIVEMQ_CLASSIC.connectionFactory(vm));

    private static Endpoint queue(String name) {
        return new Endpoint(name, null, null, null);
    }

    @Test
    void queueAndVirtualTopicSubscriptionRoundTrip() {
        runner.withPropertyValues(PEDIDOS + "destination=pedidos",
                        "messaging.senders.eventos.destination=VirtualTopic.eventos",
                        "messaging.senders.eventos.kind=topic")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    MessageSender dlqSender = destinations.sender("pedidos.dead-letter", null, queue("pedidos-dlq"));
                    Endpoint pedidos = new Endpoint("pedidos", null, null, new Options(null, null,
                            Redelivery.RESCHEDULE, null, null, null, null, null, Duration.ofSeconds(5), null, null, null));

                    destinations.sender("pedidos").send(OutgoingMessage.ofText("pedido"));
                    try (MessageReceiver receiver = destinations.receiver("pedidos", null, pedidos, dlqSender)) {
                        assertThat(receiver.capabilities().delayedRedelivery()).isTrue();
                        ReceivedMessage message = receiver.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(message.bodyAsString()).isEqualTo("pedido");
                        assertThat(message.leaseDuration()).isEqualTo(Duration.ofSeconds(5));
                        receiver.deadLetter(message, "teste");
                    }
                    try (MessageReceiver dlq = destinations.receiver("pedidos-dlq", null, queue("pedidos-dlq"), null)) {
                        ReceivedMessage dead = dlq.receive(1, Duration.ofSeconds(5)).getFirst();
                        assertThat(dead.deadLetter().reason()).isEqualTo("teste");
                        dlq.ack(dead);
                    }

                    // o Virtual Topic só copia para filas Consumer.* que já existem
                    try (MessageReceiver subscription = destinations.receiver("eventos", null,
                            new Endpoint("VirtualTopic.eventos", "faturamento", null, null), null)) {
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
        runner.withPropertyValues(PEDIDOS + "destination=pedidos", "management.health.messaging.enabled=true")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    destinations.sender("pedidos").send(OutgoingMessage.ofText("x"));
                    destinations.receiver("nada", null, queue("nao-existe"), null).close();

                    var health = context.getBean(HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails()).containsEntry("pedidos", "UP");
                    assertThat(health.getDetails().get("nada").toString()).startsWith("DestinationNotFoundException");
                });
    }

    @Test
    void activeMqRulesAreValidated() {
        runner.withPropertyValues(
                        "messaging.senders.sem-endereco.kind=queue",
                        "messaging.senders.topico-comum.destination=eventos",
                        "messaging.senders.topico-comum.kind=topic",
                        "messaging.senders.lease-zero.destination=y",
                        "messaging.senders.lease-zero.options.lease=0s")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("sem-endereco.destination é obrigatório")
                        .hasMessageContaining("topico-comum.destination precisa ser um Virtual Topic")
                        .hasMessageContaining("lease-zero.options.lease precisa ser positivo"));
    }

    @Test
    void unreachableBrokerFailsStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
                .withBean(ConnectionFactory.class, () -> JmsDialect.ACTIVEMQ_CLASSIC
                        .connectionFactory("tcp://localhost:1?connectionTimeout=500"))
                .withPropertyValues(AMQ + "type=activemq-classic", PEDIDOS + "destination=pedidos")
                .run(context -> assertThat(context).getFailure().hasStackTraceContaining("não conectou ao broker"));
    }

    /** Um pool entrega conexões que não são a do cliente do broker: aqui, proxies sobre a conexão de verdade. */
    private static ConnectionFactory pooled(ConnectionFactory real) {
        return (ConnectionFactory) Proxy.newProxyInstance(ConnectionFactory.class.getClassLoader(),
                new Class<?>[]{ConnectionFactory.class}, (proxy, method, args) -> {
                    Object result = invoke(real, method, args);
                    return method.getName().equals("createConnection")
                            ? Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (p, m, a) -> invoke(result, m, a))
                            : result;
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    void connectionFactoryWithAPoolFailsOnlyTheHealthCheck() {
        base.withBean(ConnectionFactory.class, () -> pooled(JmsDialect.ACTIVEMQ_CLASSIC.connectionFactory(vm)))
                .withPropertyValues(PEDIDOS + "destination=pedidos", "management.health.messaging.enabled=true")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    destinations.sender("pedidos").send(OutgoingMessage.ofText("pedido"));
                    try (MessageReceiver receiver = destinations.receiver("pedidos", null, queue("pedidos"), null)) {
                        assertThat(receiver.receive(1, Duration.ofSeconds(5))).hasSize(1);
                    }
                    var health = context.getBean(HealthIndicator.class).health();
                    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
                    assertThat(health.getDetails().get("pedidos").toString())
                            .startsWith("UnsupportedOperationException");
                });
    }
}
