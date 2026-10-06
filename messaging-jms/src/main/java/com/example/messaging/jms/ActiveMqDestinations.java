package com.example.messaging.jms;

import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessagingException;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.command.ActiveMQDestination;

import java.time.Duration;
import java.util.Set;

/**
 * {@code checkAccess} do ActiveMQ Classic. Abrir producer, consumer ou browser cria o destino
 * (auto-create, F7), então a existência é lida das advisories, pelo {@link DestinationSource} da
 * conexão. As advisories chegam de forma assíncrona: um destino inexistente custa
 * {@link #WAIT} de espera.
 */
final class ActiveMqDestinations {

    static final Duration WAIT = Duration.ofSeconds(2);

    private ActiveMqDestinations() {
    }

    static void check(Connection connection, String name, boolean topic) {
        if (!(connection instanceof ActiveMQConnection activeMq)) {
            throw new UnsupportedOperationException("checkAccess só com ActiveMQ Classic: " + connection.getClass());
        }
        try {
            // sem start, as advisories não chegam ao DestinationSource
            activeMq.start();
            DestinationSource source = activeMq.getDestinationSource();
            // Opções de consumer (?consumer.prefetchSize=0) não fazem parte do nome físico
            String physical = name.contains("?") ? name.substring(0, name.indexOf('?')) : name;
            long deadline = System.nanoTime() + WAIT.toNanos();
            do {
                Set<? extends ActiveMQDestination> known = topic ? source.getTopics() : source.getQueues();
                if (known.stream().anyMatch(destination -> physical.equals(destination.getPhysicalName()))) {
                    return;
                }
                Thread.sleep(50);
            } while (System.nanoTime() < deadline);
        } catch (JMSException e) {
            throw JmsErrors.map("checkAccess " + name, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingException(JmsErrors.PROVIDER, "checkAccess " + name + " interrompido", e, true);
        }
        throw new DestinationNotFoundException(JmsErrors.PROVIDER, (topic ? "Tópico" : "Fila") + " " + name
                + " não existe no broker", null);
    }
}
