package com.example.messaging.inbox.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.InboxHandler;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Um consumidor que falha ao iniciar não pode deixar os que já subiram rodando (running ainda é false). */
class InboxLifecycleStartFailureTest {

    static final String A = "messaging.receivers.alpha.";
    static final String B = "messaging.receivers.beta.";

    @Test
    void consumersAlreadyStartedAreStoppedWhenALaterOneFailsToStart() throws InterruptedException {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(InboxAutoConfiguration.class))
                .withUserConfiguration(App.class)
                .withPropertyValues(A + "destination=orders", A + "failed=orders-failed",
                        B + "destination=orders", B + "failed=orders-failed", B + "concurrency=2",
                        A + "poll-wait=100ms", B + "poll-wait=100ms", A + "shutdown-timeout=2s",
                        B + "shutdown-timeout=2s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                    while (workers().anyMatch(Thread::isAlive) && System.nanoTime() < deadline) {
                        Thread.sleep(50);
                    }
                    assertThat(workers().filter(Thread::isAlive).map(Thread::getName)).isEmpty();
                });
    }

    private static java.util.stream.Stream<Thread> workers() {
        return Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith("inbox-alpha-"));
    }

    @Configuration(proxyBeanMethods = false)
    static class App {

        @Bean
        InMemoryMessaging inMemoryMessaging() {
            return InboxAutoConfigurationTest.Broker.messaging();
        }

        @Bean
        InboxDestinations inboxDestinations(InMemoryMessaging messaging) {
            AtomicInteger betaReceivers = new AtomicInteger();
            return new InboxDestinations() {
                @Override
                public MessageReceiver receiver(String name, String provider, Endpoint endpoint,
                                                MessageSender deadLetter) {
                    // a segunda thread do beta é a que falha, depois de o alpha já ter iniciado
                    if (name.equals("beta") && betaReceivers.incrementAndGet() > 1) {
                        throw new IllegalStateException("broker fora do ar");
                    }
                    return messaging.receiver(endpoint.destination());
                }

                @Override
                public MessageSender sender(String name, String provider, Endpoint endpoint) {
                    return messaging.queueSender(endpoint.destination());
                }

                @Override
                public List<String> validate(String key, String provider, Endpoint endpoint, boolean receive) {
                    return List.of();
                }
            };
        }

        @Bean
        InboxHandler<InboxAutoConfigurationTest.Order> alpha() {
            return message -> { };
        }

        @Bean
        InboxHandler<InboxAutoConfigurationTest.Order> beta() {
            return message -> { };
        }
    }
}
