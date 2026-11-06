package com.example.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

/** Contrato da inbox (F3) no Artemis, sem auto-create. Pulado sem Docker. */
@Testcontainers(disabledWithoutDocker = true)
class ActiveMqArtemisInboxContractTest extends JmsInboxContract {

    @BeforeAll
    static void queues() throws JMSException {
        ActiveMqArtemis.start();
        List.of("inbox-main", "inbox-failed", "inbox-dlq", "inbox-retry").forEach(ActiveMqArtemis::queue);
    }

    @Override
    protected JmsDialect dialect() {
        return JmsDialect.ARTEMIS;
    }

    @Override
    protected Connection connection() {
        return ActiveMqArtemis.connection();
    }

    /** {@code max-delivery-attempts} 10 do {@code broker.xml} padrão. */
    @Override
    protected Integer nativeDeliveryLimit() {
        return 10;
    }
}
