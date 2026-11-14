package com.example.messaging.spring;

import com.example.messaging.spring.MessagingProperties.Endpoint;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A regra do bean de cliente da aplicação (ADR-0010). Sem rede: nenhum cliente é chamado de verdade. */
class ClientBeansTest {

    private static final String URL = "http://localhost:4566/0/q";
    private static final String AWS_FIELDS_A = "messaging.providers.a.";
    private static final String AWS_FIELDS_B = "messaging.providers.b.";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class));

    private static Endpoint named(String queue) {
        return new Endpoint(queue, null, null, null);
    }

    /** Um cliente que só anota os métodos chamados; {@code getQueueUrl} devolve {@link #URL}. */
    private static <T> T counting(Class<T> type, List<String> calls) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            calls.add(method.getName());
            return method.getName().equals("getQueueUrl") ? GetQueueUrlResponse.builder().queueUrl(URL).build() : null;
        }));
    }

    @Test
    void awsBeanIsUsedWhenTheProviderHasNoConnectionFields() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean(SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> {
                    context.getBean(MessagingDestinations.class).sender("s", null, named("nome-da-fila"));
                    assertThat(calls).contains("getQueueUrl");
                });
    }

    @Test
    void awsBeanWithConnectionFieldsFailsTheStartup() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws", "messaging.providers.aws.region=us-east-1")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean(SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.providers.aws")
                        .hasMessageContaining("SqsClient"));
    }

    @Test
    void awsUnnamedBeanIsIgnoredWithTwoProviders() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues(
                        AWS_FIELDS_A + "type=aws", AWS_FIELDS_A + "region=us-east-1",
                        AWS_FIELDS_A + "endpoint=http://localhost:1", AWS_FIELDS_A + "access-key=test",
                        AWS_FIELDS_A + "secret-key=test",
                        AWS_FIELDS_B + "type=aws", AWS_FIELDS_B + "region=us-east-1",
                        AWS_FIELDS_B + "endpoint=http://localhost:1", AWS_FIELDS_B + "access-key=test",
                        AWS_FIELDS_B + "secret-key=test")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // o cliente do starter não alcança o endpoint; o bean sem nome responderia
                    assertThatThrownBy(() -> context.getBean(MessagingDestinations.class)
                            .sender("s", "a", named("nome-da-fila")));
                    assertThat(calls).isEmpty();
                });
    }

    @Test
    void awsBeanNamedAfterTheProviderIsUsedWithTwoProviders() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues(
                        AWS_FIELDS_A + "type=aws",
                        AWS_FIELDS_B + "type=aws", AWS_FIELDS_B + "region=us-east-1",
                        AWS_FIELDS_B + "endpoint=http://localhost:1", AWS_FIELDS_B + "access-key=test",
                        AWS_FIELDS_B + "secret-key=test")
                .withBean("a", SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean("aSnsClient", SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    context.getBean(MessagingDestinations.class).sender("s", "a", named("nome-da-fila"));
                    assertThat(calls).contains("getQueueUrl");
                });
    }

    @Test
    void starterNeverClosesTheApplicationsClient() {
        List<String> calls = new CopyOnWriteArrayList<>();
        runner.withPropertyValues("messaging.providers.aws.type=aws")
                .withBean(SqsClient.class, () -> counting(SqsClient.class, calls))
                .withBean(SnsClient.class, () -> counting(SnsClient.class, calls))
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    destinations.sender("s", null, named("nome-da-fila"));
                    destinations.close();
                    assertThat(calls).doesNotContain("close");
                });
    }
}
