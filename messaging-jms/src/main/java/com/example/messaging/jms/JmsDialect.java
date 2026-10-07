package com.example.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;

import java.time.Duration;

/**
 * O que não é padrão JMS em cada broker (ADR-0009). As classes de cada cliente só são carregadas
 * pelo valor que as usa: a aplicação traz {@code activemq-client} ou {@code artemis-jakarta-client}.
 *
 * <table>
 *   <tr><th></th><th>Classic</th><th>Artemis</th></tr>
 *   <tr><td>Prefetch 0</td><td>{@code ?consumer.prefetchSize=0} no nome da fila</td>
 *       <td>{@code consumerWindowSize=0} na connection factory, conferido no receiver</td></tr>
 *   <tr><td>Atraso</td><td>{@code AMQ_SCHEDULED_DELAY} (exige {@code schedulerSupport})</td>
 *       <td>{@code deliveryDelay} do JMS 2.0</td></tr>
 *   <tr><td>Deduplicação</td><td>Não tem</td><td>{@code _AMQ_DUPL_ID}</td></tr>
 *   <tr><td>Subscription de tópico</td><td>Fila {@code Consumer.<s>.<VirtualTopic.t>}</td>
 *       <td>FQQN {@code <endereço>::<fila>}</td></tr>
 *   <tr><td>{@code checkAccess}</td><td>{@code DestinationSource} (advisories)</td>
 *       <td>{@code queueQuery}/{@code addressQuery} da session Core</td></tr>
 * </table>
 */
public enum JmsDialect {

    ACTIVEMQ_CLASSIC {
        @Override
        public ConnectionFactory connectionFactory(String brokerUrl) {
            return ClassicSupport.connectionFactory(brokerUrl);
        }

        @Override
        public String subscriptionQueue(String topic, String subscription) {
            return "Consumer." + subscription + "." + topic;
        }

        @Override
        String consumerQueue(String queue) {
            return queue + (queue.contains("?") ? "&" : "?") + "consumer.prefetchSize=0";
        }

        @Override
        void requireNoPrefetch(Connection connection) {
            // o prefetch vai no nome da fila (consumerQueue)
        }

        @Override
        void delay(MessageProducer producer, Message message, Duration delay) throws JMSException {
            message.setLongProperty("AMQ_SCHEDULED_DELAY", delay.toMillis());
        }

        @Override
        boolean deduplicates() {
            return false;
        }

        @Override
        void deduplicate(Message message, String deduplicationId) {
            // o Classic não deduplica
        }

        @Override
        void checkAccess(Connection connection, String name, boolean topic) {
            ClassicSupport.check(connection, name, topic);
        }
    },

    ARTEMIS {
        @Override
        public ConnectionFactory connectionFactory(String brokerUrl) {
            return ArtemisSupport.connectionFactory(brokerUrl);
        }

        @Override
        public String subscriptionQueue(String topic, String subscription) {
            return topic + "::" + subscription;
        }

        @Override
        String consumerQueue(String queue) {
            return queue;
        }

        @Override
        void requireNoPrefetch(Connection connection) {
            ArtemisSupport.requireNoWindow(connection);
        }

        @Override
        void delay(MessageProducer producer, Message message, Duration delay) throws JMSException {
            producer.setDeliveryDelay(delay.toMillis());
        }

        @Override
        boolean deduplicates() {
            return true;
        }

        @Override
        void deduplicate(Message message, String deduplicationId) throws JMSException {
            message.setStringProperty("_AMQ_DUPL_ID", deduplicationId);
        }

        @Override
        void checkAccess(Connection connection, String name, boolean topic) {
            ArtemisSupport.check(connection, name, topic);
        }
    };

    /**
     * Connection factory do cliente do broker, com reconexão: a lib não recria a conexão, então sem
     * isso ela morreria de vez quando o broker reiniciasse. Classic: embrulha a URL em
     * {@code failover:(...)}, salvo {@code failover:} e {@code vm:}. Artemis: acrescenta
     * {@code reconnectAttempts=-1} e {@code consumerWindowSize=0} quando a URL não os define.
     */
    public abstract ConnectionFactory connectionFactory(String brokerUrl);

    /** Fila de onde a subscription {@code subscription} do tópico {@code topic} é lida. */
    public abstract String subscriptionQueue(String topic, String subscription);

    /** Nome da fila para o consumer do receiver, com prefetch 0 quando é por nome. */
    abstract String consumerQueue(String queue);

    /** @throws IllegalArgumentException a conexão entrega com prefetch */
    abstract void requireNoPrefetch(Connection connection);

    /** Agenda a entrega de {@code message}, enviada a seguir por {@code producer}. */
    abstract void delay(MessageProducer producer, Message message, Duration delay) throws JMSException;

    abstract boolean deduplicates();

    abstract void deduplicate(Message message, String deduplicationId) throws JMSException;

    abstract void checkAccess(Connection connection, String name, boolean topic);
}
