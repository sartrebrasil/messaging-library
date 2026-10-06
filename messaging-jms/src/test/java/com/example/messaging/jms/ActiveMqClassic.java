package com.example.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Um ActiveMQ Classic por JVM, compartilhado pelos contratos, com {@code schedulerSupport}
 * ligado (necessário para {@code AMQ_SCHEDULED_DELAY}, F7).
 */
final class ActiveMqClassic {

    private static GenericContainer<?> container;
    private static Connection connection;

    private ActiveMqClassic() {
    }

    static synchronized void start() throws JMSException {
        if (container != null) {
            return;
        }
        container = new GenericContainer<>("apache/activemq-classic:6.2.0")
                .withExposedPorts(61616)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sh"))
                .withCommand("-c", "sed -i 's/brokerName=\"localhost\"/brokerName=\"localhost\" schedulerSupport=\"true\"/'"
                        + " /opt/apache-activemq/conf/activemq.xml && exec /opt/apache-activemq/bin/activemq console")
                .waitingFor(Wait.forLogMessage(".*Apache ActiveMQ .* started.*", 1));
        container.start();
        String url = "tcp://" + container.getHost() + ":" + container.getMappedPort(61616);
        connection = new ActiveMQConnectionFactory(url).createConnection("admin", "admin");
        connection.start();
    }

    static Connection connection() {
        return connection;
    }

    /** Cria a fila (auto-create do Classic) abrindo e fechando um consumer. */
    static String queue(String name) {
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(name)).close();
            session.close();
            return name;
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }
}
