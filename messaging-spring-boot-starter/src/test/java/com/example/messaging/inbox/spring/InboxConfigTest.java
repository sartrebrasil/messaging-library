package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.inbox.retry.BackoffSpec;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InboxConfigTest {

    private static final String C = "inbox.consumers.billing-orders.";

    @Test
    void consumerOverDefaultsOverLibraryDefaultsPropertyByProperty() {
        var consumer = single(Map.of(
                "inbox.defaults.retry.immediate.initial", "200ms",
                "inbox.defaults.retry.non-retryable[0]", "java.lang.IllegalStateException",
                "inbox.defaults.ordering", "strict",
                C + "destination", "orders",
                C + "failed", "orders-failed",
                C + "retry.immediate.max-attempts", "5",
                C + "retry.delayed.strategy", "hold"));

        assertThat(consumer.immediate()).isEqualTo(new BackoffSpec(5, Duration.ofMillis(200), 2.0, Duration.ofSeconds(2), 0.2));
        assertThat(consumer.delayed()).isEqualTo(new BackoffSpec(5, Duration.ofSeconds(30), 2.0, Duration.ofMinutes(15), 0.2));
        assertThat(consumer.delayedMaxSet()).isFalse();
        assertThat(consumer.strategy()).isEqualTo(DelayedStrategy.HOLD);
        assertThat(consumer.ordering()).isEqualTo(Ordering.STRICT);
        assertThat(consumer.nonRetryable()).containsExactly(IllegalStateException.class);
        assertThat(consumer.deadLetter()).isNull();
        assertThat(consumer.maxMessagesPerPoll()).isEqualTo(1);
        assertThat(consumer.pollWait()).isEqualTo(Duration.ofSeconds(20));
        assertThat(consumer.idempotency()).isFalse();
        assertThat(consumer.completedTtl()).isEqualTo(Duration.ofDays(7));
        assertThat(consumer.keyLanes()).isEqualTo(1);
    }

    @Test
    void listsAreReplacedNotMerged() {
        var consumer = single(Map.of(
                "inbox.defaults.retry.non-retryable[0]", "java.lang.IllegalStateException",
                C + "destination", "orders", C + "failed", "orders-failed",
                C + "retry.non-retryable", ""));

        assertThat(consumer.nonRetryable()).isEmpty();
    }

    @Test
    void configurationOfTheConsumerWinsOverTheBroker() {
        var consumer = single(Map.of(
                "inbox.brokers.queues.type", "messaging",
                "inbox.brokers.queues.configuration.poll-wait", "5s",
                "inbox.brokers.queues.configuration.max-messages-per-poll", "4",
                C + "destination", "orders", C + "failed", "orders-failed",
                C + "configuration.poll-wait", "10s",
                "inbox.defaults.key-lanes", "8",
                C + "retry.delayed.max", "5m"));

        assertThat(consumer.pollWait()).isEqualTo(Duration.ofSeconds(10));
        assertThat(consumer.maxMessagesPerPoll()).isEqualTo(4);
        assertThat(consumer.delayedMaxSet()).isTrue();
        assertThat(consumer.keyLanes()).isEqualTo(8);
    }

    @Test
    void everyProblemIsReportedAtOnce() {
        Map<String, String> values = new HashMap<>();
        values.put("inbox.defaults.destination", "x");
        values.put("inbox.defaults.circuit-breaker.enabled", "true");
        values.put(C + "retry.dead-letter.max-attempts", "3");
        values.put(C + "configuration.visibility-timeout", "1m");
        values.put(C + "retry.non-retryable[0]", "com.acme.Typo");
        values.put(C + "retry.immediate.jitter", "2");
        values.put(C + "configuration.poll-wait", "soon");

        assertThatThrownBy(() -> resolve(values))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inbox.defaults.destination belongs to one consumer")
                .hasMessageContaining("inbox.defaults.circuit-breaker is not supported yet (F6)")
                .hasMessageContaining(C + "retry.dead-letter (level 3) is not supported yet (F5)")
                .hasMessageContaining(C + "destination is required")
                .hasMessageContaining(C + "failed is required")
                .hasMessageContaining(C + "configuration.visibility-timeout is unknown")
                .hasMessageContaining("com.acme.Typo")
                .hasMessageContaining(C + "retry.immediate: jitter")
                .hasMessageContaining(C + "configuration.poll-wait: invalid value 'soon'");
    }

    @Test
    void brokersAreValidated() {
        assertThatThrownBy(() -> resolve(Map.of(
                "inbox.brokers.a.type", "messaging", "inbox.brokers.b.type", "kafka",
                C + "destination", "orders", C + "failed", "orders-failed")))
                .hasMessageContaining("inbox.brokers.b.type must be 'messaging'")
                .hasMessageContaining(C + "broker is required with more than one broker");
        assertThatThrownBy(() -> resolve(Map.of(C + "broker", "other", C + "destination", "orders",
                C + "failed", "orders-failed")))
                .hasMessageContaining("broker 'other' does not exist");
    }

    private static InboxConfig.Consumer single(Map<String, String> values) {
        List<InboxConfig.Consumer> consumers = resolve(values);
        assertThat(consumers).hasSize(1);
        return consumers.getFirst();
    }

    private static List<InboxConfig.Consumer> resolve(Map<String, String> values) {
        var binder = new Binder(new MapConfigurationPropertySource(values));
        InboxProperties properties = binder.bind("inbox", Bindable.of(InboxProperties.class)).orElseGet(InboxProperties::new);
        return InboxConfig.resolve(properties, InboxConfigTest.class.getClassLoader());
    }
}
