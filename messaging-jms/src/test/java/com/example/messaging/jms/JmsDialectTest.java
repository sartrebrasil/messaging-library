package com.example.messaging.jms;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** URLs com reconexão: sem ela, a conexão compartilhada morreria de vez num restart do broker. */
class JmsDialectTest {

    @Test
    void classicWrapsPlainUrlsInFailover() {
        assertEquals("failover:(tcp://amq:61616)?startupMaxReconnectAttempts=0&timeout=30000",
                ClassicSupport.withFailover("tcp://amq:61616"));
        assertEquals("failover:(tcp://a:61616,tcp://b:61616)", ClassicSupport.withFailover("failover:(tcp://a:61616,tcp://b:61616)"));
        assertEquals("vm://local?broker.persistent=false", ClassicSupport.withFailover("vm://local?broker.persistent=false"));
    }

    @Test
    void artemisAddsReconnectAndNoWindowUnlessSet() {
        var defaults = (ActiveMQConnectionFactory) JmsDialect.ARTEMIS.connectionFactory("tcp://art:61616");
        assertEquals(-1, defaults.getServerLocator().getReconnectAttempts());
        assertEquals(0, defaults.getServerLocator().getConsumerWindowSize());

        var explicit = (ActiveMQConnectionFactory) JmsDialect.ARTEMIS.connectionFactory(
                "tcp://art:61616?reconnectAttempts=5&consumerWindowSize=0");
        assertEquals(5, explicit.getServerLocator().getReconnectAttempts());
    }
}
