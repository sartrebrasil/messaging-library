package com.example.messaging.spring.aws;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Uma aplicação com dois {@code SqsClient}: o da lib, e outro para o outbox, fora do autowire porque o timeout dele é
 * outro. Os dois são {@code @Lazy}, e o YAML do provider só tem {@code type: aws} (ADR-0011). O starter precisa usar o
 * que é candidato ao autowire e não criar cliente nenhum enquanto não há destino.
 */
class AwsClientsOfTheApplicationTest {

    private static final String QUEUE = "http://localhost:4566/000000000000/pedidos";

    private final List<String> created = new CopyOnWriteArrayList<>();
    private final List<String> calls = new CopyOnWriteArrayList<>();

    /** Um cliente que anota que foi criado e cada método chamado, com o nome do bean. */
    private SqsClient client(String name) {
        created.add(name);
        return (SqsClient) Proxy.newProxyInstance(SqsClient.class.getClassLoader(), new Class<?>[]{SqsClient.class},
                (proxy, method, args) -> {
                    calls.add(name + "." + method.getName());
                    return switch (method.getName()) {
                        case "getQueueUrl" -> GetQueueUrlResponse.builder().queueUrl(QUEUE).build();
                        case "sendMessage" -> SendMessageResponse.builder().messageId("1").build();
                        default -> null;
                    };
                });
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
                .withBean("sqsClient", SqsClient.class, () -> client("sqsClient"), bean -> bean.setLazyInit(true))
                .withBean("outboxSqsClient", SqsClient.class, () -> client("outboxSqsClient"), bean -> {
                    bean.setLazyInit(true);
                    bean.setAutowireCandidate(false);
                })
                .withPropertyValues("messaging.providers.aws.type=aws");
    }

    @Test
    void noDestinationCreatesNoClient() {
        runner().run(context -> assertThat(context).hasNotFailed());

        assertThat(created).isEmpty();
    }

    @Test
    void starterUsesTheAutowireCandidateAndNeverTheOtherClient() {
        runner().run(context -> {
            MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
            MessageSender sender = destinations.sender("pedidos", null, new Endpoint(QUEUE, null, null, null));
            sender.send(OutgoingMessage.ofText("pedido"));
            try (MessageReceiver receiver = destinations.receiver("pedidos", null,
                    new Endpoint("pedidos", null, null, null), null)) {
                assertThat(receiver).isNotNull();
            }
        });

        assertThat(created).containsExactly("sqsClient");
        assertThat(calls).anyMatch(call -> call.startsWith("sqsClient."))
                .noneMatch(call -> call.startsWith("outboxSqsClient."));
    }
}
