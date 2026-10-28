package com.example.messaging.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
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

    /** Liga ({@code true}) ou desliga o alarme de memória do broker, que trava os publishers. */
    static void memoryAlarm(boolean on) {
        try {
            var result = container.execInContainer("rabbitmqctl", "set_vm_memory_high_watermark", on ? "0" : "0.4");
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(result.getStderr());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Exchange direct com a fila {@code queue} ligada pela routing key {@code key}. */
    static void direct(String exchange, String queue, String key) {
        try (Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(exchange, "direct", true);
            channel.queueBind(queue, exchange, key);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
