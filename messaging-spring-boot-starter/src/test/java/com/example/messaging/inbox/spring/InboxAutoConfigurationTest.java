package com.example.messaging.inbox.spring;

import com.example.idempotency.spi.IdempotencyStore;
import com.example.idempotency.store.InMemoryIdempotencyStore;
import com.example.messaging.Capabilities;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.InboxHandler;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.TypedMessage;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import com.example.messaging.inbox.redrive.Filter;
import com.example.messaging.inbox.redrive.InboxRedrive;
import com.example.messaging.inbox.redrive.RedriveReport;
import com.example.messaging.inbox.redrive.RedriveRequest;
import com.example.messaging.inbox.redrive.Source;
import com.example.messaging.inbox.redrive.Target;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class InboxAutoConfigurationTest {

    record Order(String id, int total) {
    }

    static final String ORDER = "{\"messageId\":\"m-1\",\"eventType\":\"OrderCreated\",\"aggregateId\":\"o-1\","
            + "\"payload\":{\"id\":\"o-1\",\"total\":10}}";
    static final String C = "messaging.receivers.billing-orders.";

    static final List<Object> HANDLED = new CopyOnWriteArrayList<>();

    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(InboxAutoConfiguration.class))
            .withUserConfiguration(Broker.class)
            .withPropertyValues(C + "destination=orders", C + "failed=orders-failed", C + "dead-letter=orders-dlq",
                    C + "poll-wait=100ms", C + "shutdown-timeout=2s");

    InboxAutoConfigurationTest() {
        HANDLED.clear();
        Broker.REQUESTED.clear();
    }

    @Test
    void functionalHandlerBoundByCamelCaseName() {
        runner.withUserConfiguration(Functional.class).run(context -> {
            assertThat(context).hasNotFailed();
            InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
            messaging.queueSender("orders").send(OutgoingMessage.ofText(ORDER));

            await(() -> HANDLED.size() == 1);
            assertThat(HANDLED.getFirst()).isEqualTo(new Order("o-1", 10));
        });
    }

    @Test
    void listenerWithPayloadAndMessageAndAnExplicitResult() {
        runner.withUserConfiguration(Listener.class).run(context -> {
            assertThat(context).hasNotFailed();
            InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
            messaging.queueSender("orders").send(OutgoingMessage.ofText(ORDER));

            await(() -> messaging.size("orders-failed") == 1);
            assertThat(HANDLED).containsExactly("o-1/OrderCreated");
        });
    }

    @Test
    void typedMessageListener() {
        runner.withUserConfiguration(TypedListener.class).run(context -> {
            context.getBean(InMemoryMessaging.class).queueSender("orders").send(OutgoingMessage.ofText(ORDER));

            await(() -> HANDLED.size() == 1);
            assertThat(HANDLED.getFirst()).isEqualTo("o-1");
        });
    }

    @Test
    void startupFailsListingEveryHandlerProblem() {
        runner.withUserConfiguration(Functional.class, Listener.class, Stray.class)
                .withPropertyValues("messaging.receivers.lonely.destination=orders", "messaging.receivers.lonely.failed=orders-failed")
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .hasMessageContaining("billing-orders has more than one handler")
                        .hasMessageContaining("lonely has no handler")
                        .hasMessageContaining("receiver 'nobody' does not exist in messaging.receivers"));
    }

    @Test
    void globalSwitchOffStartsNothing() {
        runner.withPropertyValues("messaging.receivers-enabled=false", C + "enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(InboxLifecycle.class);
                    assertThat(Broker.REQUESTED).isEmpty();
                });
    }

    @Test
    void disabledReceiverIsValidatedButNotStarted() {
        runner.withPropertyValues(C + "enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Broker.REQUESTED).isEmpty();
                });
        runner.withPropertyValues(C + "enabled=false", C + "destination=bad")
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .hasStackTraceContaining(C + "destination: bad address"));
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(InboxAutoConfiguration.class))
                .withUserConfiguration(Broker.class)
                .withPropertyValues(C + "enabled=false", C + "destination=orders")
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .hasStackTraceContaining(C + "failed is required"));
    }

    @Test
    void listenerOfDisabledReceiverIsAccepted() {
        runner.withUserConfiguration(Listener.class).withPropertyValues(C + "enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Broker.REQUESTED).isEmpty();
                });
    }

    @Test
    void listenerOfUnknownReceiverFails() {
        runner.withUserConfiguration(Functional.class, Stray.class)
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .hasMessageContaining("@MessageListener")
                        .hasMessageContaining("receiver 'nobody' does not exist in messaging.receivers"));
    }

    @Test
    void retryBandsWithCloseDelaysGetTheirOwnSenders() {
        runner.withUserConfiguration(Functional.class).withPropertyValues(
                        C + "retry.delayed.strategy=retry-queues",
                        C + "retry.delayed.queues[0].delay=500ms", C + "retry.delayed.queues[0].destination=q-fast",
                        C + "retry.delayed.queues[1].delay=900ms", C + "retry.delayed.queues[1].destination=q-slow")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(Broker.REQUESTED).contains("sender billing-orders.retry.0", "sender billing-orders.retry.1");
                });
    }

    @Test
    void deadLetterPassEndToEnd() {
        runner.withUserConfiguration(Functional.class)
                .withPropertyValues(C + "retry.dead-letter.interval=200ms")
                .run(context -> {
                    InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
                    messaging.queueSender("orders-dlq").send(OutgoingMessage.ofText(ORDER)
                            .withAttribute("inbox_attempts", "l2=5;l3=0"));

                    await(() -> HANDLED.size() == 1 && messaging.size("orders-dlq") == 0);
                    assertThat(Broker.REQUESTED).contains("receiver billing-orders.dead-letter");
                });
    }

    @Test
    void redriveEndToEnd() {
        runner.withUserConfiguration(Functional.class).run(context -> {
            InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
            messaging.queueSender("orders-failed").send(OutgoingMessage.ofText(ORDER)
                    .withAttribute("inbox_attempts", "l2=5;l3=3")
                    .withAttribute("inbox_failure", com.example.messaging.inbox.retry.FailureAttribute.next(null,
                            "billing-orders", "orders", new IllegalStateException("down"), java.time.Instant.now())
                            .format()));
            Thread.sleep(5);

            RedriveReport report = context.getBean(InboxRedrive.class).redrive(new RedriveRequest("billing-orders",
                    Source.FAILED, Target.MAIN, 10, Filter.NONE, 100, false));

            assertThat(report.moved()).isEqualTo(1);
            await(() -> HANDLED.size() == 1);
            assertThat(messaging.size("orders-failed")).isZero();
            assertThat(Broker.REQUESTED).contains("receiver billing-orders.failed", "sender billing-orders.redrive");
        });
    }

    /** A redrive to a topic would reach every subscription: refused before reading (review focus 2). */
    @Test
    void redriveMainIsRefusedOnASubscription() {
        runner.withUserConfiguration(Functional.class).withPropertyValues(C + "subscription=billing")
                .run(context -> assertThatThrownBy(() -> context.getBean(InboxRedrive.class).redrive(
                        new RedriveRequest("billing-orders", Source.FAILED, Target.MAIN, 10, Filter.NONE, 100,
                                false)))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("subscription"));
    }

    /** A receiver whose redrive routes cannot be built does not break the others (review I2). */
    @Test
    void aBrokenRouteIsUnavailableNotFatal() {
        String S = "messaging.receivers.shipping.";
        runner.withUserConfiguration(Functional.class)
                .withPropertyValues(S + "enabled=false", S + "destination=orders", S + "failed=missing")
                .run(context -> {
                    InboxRedrive redrive = context.getBean(InboxRedrive.class);
                    assertThat(redrive.redrive(new RedriveRequest("billing-orders", Source.FAILED, Target.MAIN, 10,
                            Filter.NONE, 100, true)).end()).isNotNull();
                    assertThatThrownBy(() -> redrive.redrive(new RedriveRequest("shipping", Source.FAILED,
                            Target.MAIN, 10, Filter.NONE, 100, true)))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("destination not found");
                });
    }

    /** On Pub/Sub the receive destination is the subscription: no sendable main destination (review I3). */
    @Test
    void redriveMainIsRefusedOnPubSub() {
        String S = "messaging.receivers.events.";
        runner.withUserConfiguration(Functional.class)
                .withPropertyValues(S + "enabled=false", S + "destination=pubsub-sub", S + "failed=orders-failed")
                .run(context -> assertThatThrownBy(() -> context.getBean(InboxRedrive.class).redrive(
                        new RedriveRequest("events", Source.FAILED, Target.MAIN, 10, Filter.NONE, 100, false)))
                        .isInstanceOf(IllegalArgumentException.class));
    }

    /** A DLQ that cannot be read (Pub/Sub topic without a subscription) turns level 3 off instead of failing (review I1). */
    @Test
    void unreadableDeadLetterTurnsLevel3Off() {
        runner.withUserConfiguration(Functional.class)
                .withPropertyValues(C + "dead-letter=send-only-dlq", C + "retry.dead-letter.interval=100ms")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Thread.sleep(400);
                    assertThat(Broker.REQUESTED).doesNotContain("receiver billing-orders.dead-letter");
                });
    }

    @Test
    void copiesUseReceiverScopedNames() {
        runner.withUserConfiguration(Listener.class).run(context -> {
            context.getBean(InMemoryMessaging.class).queueSender("orders").send(OutgoingMessage.ofText(ORDER));
            await(() -> context.getBean(InMemoryMessaging.class).size("orders-failed") == 1);
            assertThat(Broker.REQUESTED).contains("receiver billing-orders", "sender billing-orders.failed",
                    "sender billing-orders.dead-letter");
        });
    }

    @Test
    void unknownPropertiesFailTheStartup() {
        runner.withUserConfiguration(Functional.class).withPropertyValues(C + "concurrencyy=2")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void stopThenStartRebuildsTheConsumers() {
        runner.withUserConfiguration(Functional.class).run(context -> {
            InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
            InboxLifecycle lifecycle = context.getBean(InboxLifecycle.class);
            var first = lifecycle.consumer("billing-orders");

            context.stop();
            assertThat(first.running()).isFalse();
            context.start();

            assertThat(lifecycle.consumer("billing-orders")).isNotSameAs(first);
            messaging.queueSender("orders").send(OutgoingMessage.ofText(ORDER));
            await(() -> HANDLED.size() == 1);
        });
    }

    @Test
    void idempotencyUsesTheStoreBean() {
        runner.withUserConfiguration(Functional.class, Store.class)
                .withPropertyValues(C + "idempotency.enabled=true")
                .run(context -> {
                    InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
                    messaging.queueSender("orders").send(OutgoingMessage.ofText(ORDER));
                    messaging.queueSender("orders").send(OutgoingMessage.ofText(ORDER));

                    await(() -> messaging.size("orders") == 0);
                    assertThat(HANDLED).hasSize(1);
                });
    }

    @Test
    void idempotencyWithoutAStoreFailsTheStartup() {
        runner.withUserConfiguration(Functional.class).withPropertyValues(C + "idempotency.enabled=true")
                .run(context -> assertThat(context).hasFailed().getFailure().rootCause()
                        .hasMessageContaining("needs one IdempotencyStore bean"));
    }

    @Test
    void delayedMaxIsCappedToTheReceiverOnlyWhenNotSet() {
        ApplicationContextRunner tenMinutes = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(InboxAutoConfiguration.class))
                .withUserConfiguration(Functional.class)
                .withBean(InboxDestinations.class, () -> limited(Duration.ofMinutes(10)))
                .withPropertyValues(C + "destination=orders", C + "failed=orders-failed", C + "dead-letter=orders-dlq",
                        C + "poll-wait=100ms", C + "shutdown-timeout=2s");

        tenMinutes.run(context -> assertThat(context).hasNotFailed());
        tenMinutes.withPropertyValues(C + "retry.delayed.max=15m")
                .run(context -> assertThat(context).hasFailed().getFailure().rootCause()
                        .hasMessageContaining("above the receiver maxRedeliveryDelay"));
    }

    @Test
    void capabilitiesAreValidatedAtStartup() {
        runner.withUserConfiguration(Functional.class).withPropertyValues(C + "ordering=strict")
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .hasStackTraceContaining("ordering: strict needs a receiver with orderedDelivery"));
        runner.withUserConfiguration(Functional.class).withPropertyValues(C + "retry.delayed.strategy=retry-queues")
                .run(context -> assertThat(context).hasFailed().getFailure()
                        .hasStackTraceContaining("retry-queues without delayed.queues"));
    }

    static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met in 10 s");
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** Destinations over a fresh in-memory broker whose receivers report a {@code maxRedeliveryDelay}. */
    private static InboxDestinations limited(Duration maxRedeliveryDelay) {
        InMemoryMessaging messaging = Broker.messaging();
        return new InboxDestinations() {
            @Override
            public MessageReceiver receiver(String name, String provider, Endpoint endpoint, MessageSender deadLetter) {
                MessageReceiver delegate = messaging.receiver(endpoint.destination());
                return (MessageReceiver) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[] {MessageReceiver.class}, (proxy, method, args) -> {
                            if (method.getName().equals("capabilities")) {
                                Capabilities c = delegate.capabilities();
                                return new Capabilities(c.maxMessageBytes(), c.maxBatchSize(), true, maxRedeliveryDelay,
                                        c.nativeDeadLetter(), c.orderedDelivery(), c.publisherDeduplication(),
                                        c.reportsLeaseExpiredOnAck());
                            }
                            try {
                                return method.invoke(delegate, args);
                            } catch (java.lang.reflect.InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
            }

            @Override
            public MessageSender sender(String name, String provider, Endpoint endpoint) {
                return messaging.queueSender(endpoint.destination());
            }
        };
    }

    @Configuration(proxyBeanMethods = false)
    static class Broker {

        /** What the inbox asked for: {@code receiver <name>} or {@code sender <name>}. */
        static final List<String> REQUESTED = new CopyOnWriteArrayList<>();

        static InMemoryMessaging messaging() {
            InMemoryMessaging messaging = new InMemoryMessaging();
            for (String q : List.of("orders", "orders-failed", "orders-dlq", "q-fast", "q-slow", "send-only-dlq")) {
                messaging.createQueue(q, QueueOptions.defaults());
            }
            return messaging;
        }

        @Bean
        InMemoryMessaging inMemoryMessaging() {
            return messaging();
        }

        @Bean
        InboxDestinations inboxDestinations(InMemoryMessaging messaging) {
            return new InboxDestinations() {
                @Override
                public MessageReceiver receiver(String name, String provider, Endpoint endpoint,
                                                MessageSender deadLetter) {
                    REQUESTED.add("receiver " + name);
                    return messaging.receiver(endpoint.destination());
                }

                @Override
                public MessageSender sender(String name, String provider, Endpoint endpoint) {
                    REQUESTED.add("sender " + name);
                    if ("missing".equals(endpoint.destination())) {
                        // as MessagingDestinations does when the provider does not find the address (SQS by name)
                        throw new IllegalStateException(name + ": destination not found");
                    }
                    return messaging.queueSender(endpoint.destination());
                }

                @Override
                public List<String> validate(String key, String provider, Endpoint endpoint, boolean receive) {
                    if (!receive && endpoint.kind() == com.example.messaging.spring.MessagingProperties.Kind.QUEUE
                            && "pubsub-sub".equals(endpoint.destination())) {
                        // as on GCP, which only sends to topics
                        return List.of(key + ".kind não se aplica a este provedor");
                    }
                    if (receive && "send-only-dlq".equals(endpoint.destination())) {
                        return List.of(key + ".destination: cannot be read");
                    }
                    return "bad".equals(endpoint.destination()) ? List.of(key + ".destination: bad address") : List.of();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Functional {
        @Bean
        InboxHandler<Order> billingOrders() {
            return message -> HANDLED.add(message.payload());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Listener {
        @Bean
        Handlers handlers() {
            return new Handlers();
        }
    }

    static class Handlers {
        @MessageListener(receiver = "billing-orders")
        HandlerResult on(JsonNode order, InboxMessage message) {
            HANDLED.add(order.path("id").asString() + "/" + message.type());
            return HandlerResult.reject("not today");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TypedListener {
        @Bean
        TypedHandlers typedHandlers() {
            return new TypedHandlers();
        }
    }

    static class TypedHandlers {
        @MessageListener(receiver = "billing-orders")
        void on(TypedMessage<Order> message) {
            HANDLED.add(message.payload().id());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Stray {
        @Bean
        StrayHandlers strayHandlers() {
            return new StrayHandlers();
        }
    }

    static class StrayHandlers {
        @MessageListener(receiver = "nobody")
        void on(String payload) {
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Store {
        @Bean
        IdempotencyStore idempotencyStore() {
            return new InMemoryIdempotencyStore();
        }
    }

}
