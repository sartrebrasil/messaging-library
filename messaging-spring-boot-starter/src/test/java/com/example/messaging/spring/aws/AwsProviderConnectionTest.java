package com.example.messaging.spring.aws;

import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.sns.SnsClient;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** O AWS só usa os beans da aplicação (ADR-0011): o starter não monta nenhum cliente. */
class AwsProviderConnectionTest {

    private static final String SNS = "software.amazon.awssdk.services.sns.SnsClient";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
            .withPropertyValues("messaging.providers.aws.type=aws");

    /** Um {@code SnsClient} que responde {@code null} a tudo: o teste não chama a AWS. */
    private static SnsClient sns() {
        return (SnsClient) Proxy.newProxyInstance(SnsClient.class.getClassLoader(), new Class<?>[]{SnsClient.class},
                (proxy, method, args) -> null);
    }

    @Test
    void snsOnlyApplicationNeedsNoSqsClient() {
        runner.withPropertyValues("messaging.senders.eventos.destination=arn:aws:sns:sa-east-1:123456789012:eventos")
                .withBean(SnsClient.class, AwsProviderConnectionTest::sns)
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void sqsReceiverWithoutSqsClientFails() {
        runner.withBean(SnsClient.class, AwsProviderConnectionTest::sns)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThatThrownBy(() -> context.getBean(MessagingDestinations.class).receiver("r", null,
                            new Endpoint("http://localhost:1/0/pedidos", null, null, null), null))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("messaging.providers.aws: declare um bean "
                                    + "software.amazon.awssdk.services.sqs.SqsClient (ADR-0011)");
                });
    }

    @Test
    void snsSenderWithoutSnsClientFails() {
        runner.withPropertyValues("messaging.senders.eventos.destination=arn:aws:sns:sa-east-1:123456789012:eventos")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.providers.aws: declare um bean " + SNS + " (ADR-0011)"));
    }
}
