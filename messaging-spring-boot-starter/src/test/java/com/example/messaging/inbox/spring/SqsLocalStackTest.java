package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.InboxHandler;
import com.example.messaging.spring.MessagingAutoConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.await;
import static org.assertj.core.api.Assertions.assertThat;

/** The starter over the real messaging-lib path: MessagingDestinations, the SQS adapter and LocalStack (as engine-tax). */
@Testcontainers(disabledWithoutDocker = true)
class SqsLocalStackTest {

    @Container
    static final GenericContainer<?> LOCALSTACK = new GenericContainer<>("localstack/localstack:4.14.0")
            .withEnv("SERVICES", "sqs")
            .withExposedPorts(4566)
            .waitingFor(Wait.forLogMessage(".*Ready\\.\\n", 1));

    static final List<JsonNode> HANDLED = new CopyOnWriteArrayList<>();
    static SqsClient sqs;
    static String main;
    static String dlq;
    static String failed;

    @BeforeAll
    static void queues() {
        sqs = SqsClient.builder().region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://" + LOCALSTACK.getHost() + ":" + LOCALSTACK.getMappedPort(4566)))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30))).build();
        main = queue("orders");
        dlq = queue("orders-dlq");
        failed = queue("orders-failed");
    }

    @Test
    void consumesFromSqsAndSendsWhatCannotBeReadToFailed() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class, MessagingAutoConfiguration.class,
                        InboxAutoConfiguration.class))
                .withUserConfiguration(Handler.class)
                .withPropertyValues("messaging.providers.aws.type=aws", "messaging.providers.aws.region=us-east-1",
                        "messaging.destinations.orders.provider=aws", "messaging.destinations.orders.queue-url=" + main,
                        "messaging.destinations.orders-dlq.provider=aws", "messaging.destinations.orders-dlq.queue-url=" + dlq,
                        "messaging.destinations.orders-failed.provider=aws",
                        "messaging.destinations.orders-failed.queue-url=" + failed,
                        "inbox.consumers.orders.destination=orders", "inbox.consumers.orders.dead-letter=orders-dlq",
                        "inbox.consumers.orders.failed=orders-failed", "inbox.consumers.orders.key-lanes=4",
                        "inbox.consumers.orders.configuration.max-messages-per-poll=10",
                        "inbox.consumers.orders.configuration.poll-wait=1s",
                        "inbox.consumers.orders.configuration.shutdown-timeout=5s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    String id = sqs.sendMessage(b -> b.queueUrl(main).messageBody("not json")).messageId();
                    sqs.sendMessage(b -> b.queueUrl(main).messageBody(
                            "{\"messageId\":\"m-1\",\"eventType\":\"OrderCreated\",\"payload\":{\"id\":\"o-1\"}}"));

                    await(() -> HANDLED.size() == 1);
                    assertThat(HANDLED.getFirst().path("id").asString()).isEqualTo("o-1");
                    List<Message> copies = new CopyOnWriteArrayList<>();
                    await(() -> {
                        copies.addAll(sqs.receiveMessage(b -> b.queueUrl(failed).messageAttributeNames("All")
                                .messageSystemAttributeNames(MessageSystemAttributeName.ALL).waitTimeSeconds(1)).messages());
                        return !copies.isEmpty();
                    });
                    assertThat(copies.getFirst().body()).isEqualTo("not json");
                    assertThat(copies.getFirst().messageAttributes().toString())
                            .contains("inbox_failure", "exception=tools.jackson.core.exc.StreamReadException", "inbox_attempts", "dead_letter", id);
                });
    }

    private static String queue(String name) {
        return sqs.createQueue(b -> b.queueName(name)
                .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "5"))).queueUrl();
    }

    @Configuration(proxyBeanMethods = false)
    static class Handler {
        @Bean
        SqsClient sqsClient() {
            return sqs;
        }

        @Bean
        InboxHandler<JsonNode> orders() {
            return message -> HANDLED.add(message.payload());
        }
    }
}
