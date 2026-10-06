package com.example.messaging.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Map;

/** Um RabbitMQ 4.1 por JVM, compartilhado pelos contratos. Filas e exchanges criados pelo teste. */
final class RabbitMq {

    private static GenericContainer<?> container;
    private static Connection connection;

    private RabbitMq() {
    }

    static synchronized void start() throws Exception {
        if (container != null) {
            return;
        }
        container = new GenericContainer<>("rabbitmq:4.1-management-alpine")
                .withExposedPorts(5672)
                .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1));
        container.start();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(container.getHost());
        factory.setPort(container.getMappedPort(5672));
        connection = factory.newConnection();
    }

    static Connection connection() {
        return connection;
    }

    /** Declara (ou reaproveita) uma fila durável do tipo dado ({@code quorum} ou {@code classic}). */
    static String queue(String name, String type) {
        try (Channel channel = connection.createChannel()) {
            channel.queueDeclare(name, true, false, false, Map.of("x-queue-type", type));
            return name;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Exchange fanout com a fila {@code queue} ligada a ele. */
    static void fanout(String exchange, String queue) {
        try (Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(exchange, "fanout", true);
            channel.queueBind(queue, exchange, "");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
