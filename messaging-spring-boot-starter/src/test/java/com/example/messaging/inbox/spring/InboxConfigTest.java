package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.spring.MessagingProperties.Redelivery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InboxConfigTest {

    private static final String C = "messaging.receivers.billing-orders.";
    private static final String D = "messaging.receiver-defaults.";

    @Test
    void receiverOverDefaultsOverLibraryDefaultsPropertyByProperty() {
        var consumer = single(Map.of(
                D + "retry.immediate.initial", "200ms",
                D + "retry.non-retryable[0]", "java.lang.IllegalStateException",
                D + "ordering", "strict",
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
        assertThat(consumer.idempotency()).isFalse();
        assertThat(consumer.completedTtl()).isEqualTo(Duration.ofDays(7));
        assertThat(consumer.keyLanes()).isEqualTo(1);
    }

    @Test
    void libraryDefaults() {
        var consumer = single(Map.of(C + "destination", "orders", C + "failed", "orders-failed"));

        assertThat(consumer.enabled()).isTrue();
        assertThat(consumer.provider()).isNull();
        assertThat(consumer.maxMessagesPerPoll()).isEqualTo(1);
        assertThat(consumer.pollWait()).isEqualTo(Duration.ofSeconds(20));
        assertThat(consumer.maxProcessing()).isEqualTo(Duration.ofMinutes(15));
        assertThat(consumer.shutdownTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(consumer.endpoint().destination()).isEqualTo("orders");
    }

    @Test
    void receiverWinsOverDefaults() {
        var consumer = single(Map.of(
                D + "poll-wait", "5s",
                D + "max-messages-per-poll", "4",
                D + "key-lanes", "8",
                C + "destination", "orders", C + "failed", "orders-failed",
                C + "poll-wait", "10s",
                C + "retry.delayed.max", "5m"));

        assertThat(consumer.pollWait()).isEqualTo(Duration.ofSeconds(10));
        assertThat(consumer.maxMessagesPerPoll()).isEqualTo(4);
        assertThat(consumer.delayedMaxSet()).isTrue();
        assertThat(consumer.keyLanes()).isEqualTo(8);
    }

    @Test
    void addressFieldsGoToTheEndpoint() {
        var consumer = single(Map.of(
                C + "destination", "orders", C + "subscription", "billing", C + "failed", "orders-failed",
                C + "dead-letter", "orders-dlq", C + "enabled", "false",
                C + "options.sessions", "true", C + "options.redelivery", "reschedule"));

        assertThat(consumer.enabled()).isFalse();
        assertThat(consumer.endpoint().subscription()).isEqualTo("billing");
        assertThat(consumer.endpoint().options().sessions()).isTrue();
        assertThat(consumer.endpoint().options().redelivery()).isEqualTo(Redelivery.RESCHEDULE);
        assertThat(consumer.deadLetter()).isEqualTo("orders-dlq");
        assertThat(consumer.failed()).isEqualTo("orders-failed");
    }

    @Test
    void providerFromDefaults() {
        assertThat(single(Map.of(D + "provider", "b", C + "destination", "orders", C + "failed", "f")).provider())
                .isEqualTo("b");
        assertThat(single(Map.of(D + "provider", "b", C + "provider", "a", C + "destination", "orders",
                C + "failed", "f")).provider()).isEqualTo("a");
    }

    @Test
    void everyProblemIsReportedAtOnce() {
        Map<String, String> values = new HashMap<>();
        values.put(D + "circuit-breaker.enabled", "true");
        values.put(C + "retry.dead-letter.max-attempts", "3");
        values.put(C + "retry.non-retryable[0]", "com.acme.Typo");
        values.put(C + "retry.immediate.jitter", "2");

        assertThatThrownBy(() -> resolve(values))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(D + "circuit-breaker is not supported yet (F6)")
                .hasMessageContaining(C + "retry.dead-letter (level 3) is not supported yet (F5)")
                .hasMessageContaining(C + "destination is required")
                .hasMessageContaining(C + "failed is required")
                .hasMessageContaining("com.acme.Typo")
                .hasMessageContaining(C + "retry.immediate: jitter");
    }

    @ParameterizedTest
    @ValueSource(strings = {"destination", "subscription", "dead-letter", "failed", "enabled", "options.sessions",
            "retry.delayed.queues[0].destination"})
    void forbiddenInDefaults(String key) {
        Map<String, String> values = new HashMap<>(Map.of(D + key, "true", C + "destination", "orders",
                C + "failed", "f"));
        String root = key.contains(".") ? key.substring(0, key.indexOf('.')) : key;
        String shown = key.startsWith("retry") ? "retry.delayed.queues" : root;
        if (key.startsWith("retry")) {
            values.put(D + "retry.delayed.queues[0].delay", "1m");
        }
        assertThatThrownBy(() -> resolve(values))
                .hasMessageContaining(D + shown + " belongs to one receiver");
    }

    @Test
    void invalidDurationFailsTheBinding() {
        assertThatThrownBy(() -> bind(Map.of(C + "destination", "orders", C + "failed", "f", C + "poll-wait", "soon")))
                .isInstanceOf(BindException.class)
                .hasMessageContaining(C + "poll-wait");
    }

    @Test
    void unknownKeyFails() {
        assertThatThrownBy(() -> bind(Map.of(C + "destination", "orders", C + "failed", "f",
                C + "visibility-timeout", "1m")))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining(C + "visibility-timeout");
        assertThatThrownBy(() -> bind(Map.of(D + "configuration.poll-wait", "1s")))
                .isInstanceOf(BindException.class);
    }

    @Test
    void providersAndSendersAreNotThisBinding() {
        assertThat(bind(Map.of("messaging.providers.aws.type", "aws", "messaging.senders.s.destination", "q"))
                .getConsumers()).isEmpty();
    }

    private static InboxConfig.Consumer single(Map<String, String> values) {
        List<InboxConfig.Consumer> consumers = resolve(values);
        assertThat(consumers).hasSize(1);
        return consumers.getFirst();
    }

    private static InboxProperties bind(Map<String, String> values) {
        return InboxProperties.bind(new Binder(new MapConfigurationPropertySource(values)));
    }

    private static List<InboxConfig.Consumer> resolve(Map<String, String> values) {
        return InboxConfig.resolve(bind(values), InboxConfigTest.class.getClassLoader());
    }
}
