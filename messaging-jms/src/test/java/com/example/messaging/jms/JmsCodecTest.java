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
}
