package com.example.messaging.aws.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.aws.SnsMessageSender;
import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** SQS e SNS pelo starter. Sem rede: os clientes do SDK só conectam no primeiro uso. */
class AwsAutoConfigurationTest {

    private static final String AWS = "messaging.providers.aws.";
    private static final String QUEUE = "http://localhost:4566/000000000000/";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
            .withPropertyValues(AWS + "type=aws", AWS + "region=us-east-1", AWS + "endpoint=http://localhost:4566",
                    AWS + "access-key=test", AWS + "secret-key=test");

    @Test
    void createsSendersAndReceiversByName() {
        runner.withPropertyValues(
                        "messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos",
                        "messaging.destinations.pedidos.dead-letter=pedidos-dlq",
                        "messaging.destinations.pedidos-dlq.provider=aws",
                        "messaging.destinations.pedidos-dlq.queue-url=" + QUEUE + "pedidos-dlq",
                        "messaging.destinations.eventos.provider=aws",
                        "messaging.destinations.eventos.topic-arn=arn:aws:sns:us-east-1:000000000000:eventos",
                        "messaging.destinations.eventos.max-message-bytes=1048576")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    assertThat(destinations.names()).containsExactly("eventos", "pedidos", "pedidos-dlq");

                    MessageSender sender = destinations.sender("pedidos");
                    assertThat(destinations.sender("pedidos")).isSameAs(sender);
                    assertThat(destinations.sender("eventos").capabilities().maxMessageBytes()).isEqualTo(1048576);

                    MessageReceiver first = destinations.receiver("pedidos");
                    MessageReceiver second = destinations.receiver("pedidos");
                    assertThat(first).isNotSameAs(second);
                    // DLQ por cópia configurada a partir de dead-letter
                    assertThat(first.capabilities().nativeDeadLetter()).isFalse();
                });
    }

    @Test
    void topicOnlyDestinationDoesNotReceive() {
        runner.withPropertyValues("messaging.destinations.eventos.provider=aws",
                        "messaging.destinations.eventos.topic-arn=arn:aws:sns:us-east-1:000000000000:eventos")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    assertThat(destinations.sender("eventos").capabilities().maxMessageBytes())
                            .isEqualTo(SnsMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> destinations.receiver("eventos"))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("não recebe");
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> destinations.sender("nao-existe"))
                            .isInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void requiredCapabilitiesAreCheckedAtStartup() {
        runner.withPropertyValues("messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos",
                        "messaging.destinations.pedidos.require=ordered-delivery,lease-expired-on-ack")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("pedidos.require ORDERED_DELIVERY: o sender não suporta")
                        .hasMessageContaining("pedidos.require LEASE_EXPIRED_ON_ACK: o receiver não suporta"));

        runner.withPropertyValues("messaging.destinations.pedidos.provider=aws",
                        "messaging.destinations.pedidos.queue-url=" + QUEUE + "pedidos.fifo",
                        "messaging.destinations.pedidos.require=ordered-delivery,delayed-redelivery")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
