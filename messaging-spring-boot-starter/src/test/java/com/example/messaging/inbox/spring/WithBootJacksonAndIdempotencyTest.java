package com.example.messaging.inbox.spring;

import com.example.idempotency.spring.IdempotencyAutoConfiguration;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.inbox.PayloadDeserializer;
import com.example.messaging.memory.InMemoryMessaging;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.json.JsonMapper;

import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.C;
import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.HANDLED;
import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.ORDER;
import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.await;
import static org.assertj.core.api.Assertions.assertThat;

/** The engine-tax setup: Boot's Jackson, the idempotency starter and the inbox starter in one context. */
class WithBootJacksonAndIdempotencyTest {

    @Test
    void payloadsUseTheApplicationMapperAndDuplicatesAreAcked() {
        HANDLED.clear();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class,
                        IdempotencyAutoConfiguration.class, InboxAutoConfiguration.class))
                .withUserConfiguration(InboxAutoConfigurationTest.Broker.class, InboxAutoConfigurationTest.Functional.class)
                .withPropertyValues("idempotency.store=memory",
                        C + "destination=orders", C + "failed=orders-failed", C + "dead-letter=orders-dlq",
                        C + "idempotency.enabled=true",
                        C + "configuration.poll-wait=100ms", C + "configuration.shutdown-timeout=2s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Object mapper = ReflectionTestUtils.getField(context.getBean(PayloadDeserializer.class), "mapper");
                    assertThat(mapper).isSameAs(context.getBean("jacksonJsonMapper", JsonMapper.class));

                    InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
                    messaging.queueSender("orders").send(OutgoingMessage.ofText(ORDER));
                    messaging.queueSender("orders").send(OutgoingMessage.ofText(ORDER));
                    await(() -> messaging.size("orders") == 0);
                    assertThat(HANDLED).hasSize(1);
                });
    }
}
