package com.example.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.EnumSet;

/**
 * Um ActiveMQ Artemis por JVM, compartilhado pelos contratos, criado com {@code --no-autocreate}:
 * filas e endereços são criados pelo teste, como o Terraform faria, e destino inexistente falha.
 */
final class ActiveMqArtemis {

    private static GenericContainer<?> container;
    private static Connection connection;

    private ActiveMqArtemis() {
    }

    static synchronized void start() throws JMSException {
        if (container != null) {
            return;
        }
        container = new GenericContainer<>("apache/activemq-artemis:2.40.0")
                .withEnv("EXTRA_ARGS", "--http-host 0.0.0.0 --relax-jolokia --no-autocreate")
                .withExposedPorts(61616)
                .waitingFor(Wait.forLogMessage(".*AMQ221007.*", 1));
        container.start();
        String url = "tcp://" + container.getHost() + ":" + container.getMappedPort(61616);
        connection = JmsDialect.ARTEMIS.connectionFactory(url).createConnection("artemis", "artemis");
        connection.start();
    }

    static Connection connection() {
        return connection;
    }

    /** Conexão sem {@code consumerWindowSize=0}, que o receiver recusa. */
    static Connection connectionWithWindow() throws JMSException {
        String url = "tcp://" + container.getHost() + ":" + container.getMappedPort(61616);
        return new ActiveMQConnectionFactory(url).createConnection("artemis", "artemis");
    }

    /** Fila anycast no endereço de mesmo nome. */
    static String queue(String name) {
        create(QueueConfiguration.of(name).setAddress(name).setRoutingType(RoutingType.ANYCAST));
        return name;
    }

    /** Fila multicast {@code subscription} no endereço {@code topic}; devolve o FQQN. */
    static String subscription(String topic, String subscription) {
        create(QueueConfiguration.of(subscription).setAddress(topic).setRoutingType(RoutingType.MULTICAST));
        return JmsDialect.ARTEMIS.subscriptionQueue(topic, subscription);
    }

    /** Sem auto-create no broker, o endereço precisa existir antes da fila. */
    private static void create(QueueConfiguration queue) {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            ClientSession core = ((ActiveMQSession) session).getCoreSession();
            if (!core.addressQuery(queue.getAddress()).isExists()) {
                core.createAddress(queue.getAddress(), EnumSet.of(queue.getRoutingType()), false);
            }
            core.createQueue(queue.setDurable(true));
        } catch (JMSException | ActiveMQException e) {
            throw new IllegalStateException(e);
        }
    }
}
