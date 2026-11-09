package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.spring.MessagingProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileSystemResource;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps messaging-inbox/docs/exemplos/application-completo.yaml in step with the properties the starter accepts. */
class DocumentedConfigTest {

    @Test
    void documentedFullExampleBindsAndResolves() throws Exception {
        StandardEnvironment env = new StandardEnvironment();
        new YamlPropertySourceLoader()
                .load("doc", new FileSystemResource("../messaging-inbox/docs/exemplos/application-completo.yaml"))
                .forEach(env.getPropertySources()::addFirst);
        Binder binder = new Binder(ConfigurationPropertySources.get(env), new PropertySourcesPlaceholdersResolver(env));

        MessagingProperties.bind(binder);
        List<InboxConfig.Consumer> consumers = InboxConfig.resolve(InboxProperties.bind(binder),
                getClass().getClassLoader());

        assertThat(consumers).extracting(InboxConfig.Consumer::name)
                .containsExactly("billing-orders", "invoices", "payments");
        InboxConfig.Consumer orders = consumers.getFirst();
        assertThat(orders.deadLetter()).isEqualTo("orders-dlq");
        assertThat(orders.idempotency()).isTrue();
        assertThat(orders.keyLanes()).isEqualTo(10);
        assertThat(orders.nonRetryable()).contains(UnsupportedOperationException.class);
        assertThat(consumers.get(1).strategy()).isEqualTo(DelayedStrategy.RETRY_QUEUES);
        assertThat(consumers.get(1).queues()).hasSize(3);
        assertThat(consumers.get(2).ordering()).isEqualTo(Ordering.STRICT);
        assertThat(consumers.get(2).delayed().max()).isEqualTo(Duration.ofMinutes(2));
    }
}
