package com.example.messaging.jms;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

/** Contrato da inbox (F3) no ActiveMQ Classic, com {@code schedulerSupport}. Pulado sem Docker. */
@Testcontainers(disabledWithoutDocker = true)
class ActiveMqClassicInboxContractTest extends JmsInboxContract {

    @BeforeAll
    static void queues() throws JMSException {
        ActiveMqClassic.start();
        List.of("inbox-main", "inbox-failed", "inbox-dlq", "inbox-retry").forEach(ActiveMqClassic::queue);
    }

    @Override
    protected JmsDialect dialect() {
        return JmsDialect.ACTIVEMQ_CLASSIC;
    }

    @Override
    protected Connection connection() {
        return ActiveMqClassic.connection();
    }

    /**
     * Sem limite: medido pelo contrato (30 {@code nack(0)} sem a mensagem sair). O {@code nack} do adapter fecha a
     * session, e a {@code RedeliveryPolicy} do cliente ({@code maximumRedeliveries} 6) só age no rollback/recover da
     * mesma session; sem o redelivery plugin no broker, nada tira a mensagem.
     */
    @Override
    protected Integer nativeDeliveryLimit() {
        return null;
    }
}
