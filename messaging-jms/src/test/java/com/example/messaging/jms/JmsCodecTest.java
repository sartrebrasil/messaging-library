package com.example.messaging.jms;

import com.example.messaging.ReceivedMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JmsCodecTest {

    /** A cópia que volta pelo agendador do Classic traz propriedades dele; não são atributos do usuário. */
    @Test
    void schedulerPropertiesOfTheClassicAreNotAttributes() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setJMSMessageID("ID:test-1:1:1:1:1");
        message.setText("x");
        message.setStringProperty("scheduledJobId", "ID:job-1");
        message.setLongProperty("AMQ_SCHEDULED_DELAY", 1000);
        message.setStringProperty("order_id", "o-1");

        ReceivedMessage received = JmsCodec.decode(message, Instant.now(), Instant.now(), null);

        assertEquals(java.util.Map.of("order_id", "o-1"), received.attributes());
    }

    /** O nack com atraso copia a mensagem: atributo de fora da lib que a cópia não aceita fica de fora. */
    @Test
    void delayedCopyLeavesOutAttributesACopyCannotCarry() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setJMSMessageID("ID:test-1:1:1:1:1");
        message.setText("x");
        message.setStringProperty("orderId", "o-1");
        message.setStringProperty("order_id", "o-1");
        ReceivedMessage received = JmsCodec.decode(message, Instant.now(), Instant.now(), null);
        assertEquals(java.util.Map.of("orderId", "o-1", "order_id", "o-1"), received.attributes(),
                "a mensagem recebida continua com tudo");

        jakarta.jms.Session session = (jakarta.jms.Session) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{jakarta.jms.Session.class},
                (p, m, a) -> m.getName().equals("createBytesMessage")
                        ? new org.apache.activemq.command.ActiveMQBytesMessage() : null);
        jakarta.jms.BytesMessage copy = JmsCodec.copy(session, JmsDialect.ACTIVEMQ_CLASSIC, received);

        assertEquals("o-1", copy.getStringProperty("order_id"));
        assertEquals(null, copy.getStringProperty("orderId"));
    }
}
