package com.example.messaging.inbox.spring;

import com.example.messaging.spring.MessagingAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** O caminho da inbox até a validação de endereço do starter (sem rede: os receivers ficam desligados). */
class InboxProviderOptionsTest {

    private static final String Q = "http://localhost:1/0/";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class, MessagingAutoConfiguration.class,
                    InboxAutoConfiguration.class))
            .withPropertyValues("messaging.providers.aws.type=aws", "messaging.providers.aws.region=us-east-1",
                    "messaging.providers.aws.endpoint=http://localhost:1", "messaging.providers.aws.access-key=test",
                    "messaging.providers.aws.secret-key=test",
                    "messaging.receivers.orders.enabled=false", "messaging.receivers.orders.destination=" + Q + "orders",
                    "messaging.receivers.orders.failed=" + Q + "orders-failed",
                    "messaging.receivers.payments.enabled=false", "messaging.receivers.payments.destination=" + Q + "payments",
                    "messaging.receivers.payments.failed=" + Q + "payments-failed");

    @Test
    void receiverOptionNotAcceptedByItsProviderFailsTheStartup() {
        runner.withPropertyValues("messaging.receivers.orders.options.sessions=true",
                        "messaging.receivers.payments.options.lease=5s")
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .hasStackTraceContaining("Invalid messaging.receivers configuration")
                        .hasStackTraceContaining("messaging.receivers.orders.options.sessions não se aplica ao provedor aws")
                        .hasStackTraceContaining("messaging.receivers.payments.options.lease não se aplica ao provedor aws"));
    }

    @Test
    void deadLetterAndFailedCopiesCarryNoOptions() {
        runner.withPropertyValues("messaging.receivers.orders.options.max-message-bytes=1024",
                        "messaging.receivers.orders.dead-letter=" + Q + "orders-dlq")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
