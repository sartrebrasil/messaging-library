package com.example.messaging.spring;

import com.example.messaging.spring.MessagingProperties.Destination;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.spring.MessagingProperties.Kind;
import com.example.messaging.spring.MessagingProperties.Options;
import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EndpointsTest {

    private static final String KEY = "messaging.receivers.r";
    private final List<String> problems = new ArrayList<>();

    private static Provider provider(Type type) {
        return new Provider(type, null, null, null, null, null, "p", null, null, null, null);
    }

    private Destination receive(Type type, String destination, String subscription) {
        return Endpoints.resolve(KEY, "p", provider(type), new Endpoint(destination, subscription, null, gcpOptions(type)),
                true, problems);
    }

    private Destination send(Type type, String destination, Kind kind) {
        return Endpoints.resolve(KEY, "p", provider(type), new Endpoint(destination, null, kind, null), false, problems);
    }

    private static Options gcpOptions(Type type) {
        return type == Type.GCP ? new Options(null, null, null, null, Duration.ofSeconds(30), null, null, null, null,
                null, null, null) : null;
    }

    @Test
    void awsReceiveUrlIsQueueUrl() {
        Destination d = receive(Type.AWS, "https://sqs.sa-east-1.amazonaws.com/1/q", null);
        assertThat(problems).isEmpty();
        assertThat(d.queueUrl()).isEqualTo("https://sqs.sa-east-1.amazonaws.com/1/q");
        assertThat(d.queue()).isNull();
    }

    @Test
    void awsReceiveNameIsQueue() {
        Destination d = receive(Type.AWS, "q", null);
        assertThat(problems).isEmpty();
        assertThat(d.queue()).isEqualTo("q");
        assertThat(d.queueUrl()).isNull();
    }

    @Test
    void awsSendArnIsTopic() {
        Destination d = send(Type.AWS, "arn:aws:sns:sa-east-1:1:t", null);
        assertThat(problems).isEmpty();
        assertThat(d.topicArn()).isEqualTo("arn:aws:sns:sa-east-1:1:t");
    }

    @Test
    void awsTopicKindWithoutArnFails() {
        send(Type.AWS, "t", Kind.TOPIC);
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".kind: topic exige um ARN do SNS"));
    }

    @Test
    void awsSubscriptionFails() {
        receive(Type.AWS, "q", "s");
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".subscription não se aplica a este provedor"));
    }

    @Test
    void gcpSubscriptionFails() {
        receive(Type.GCP, "sub", "s");
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".subscription não se aplica a este provedor"));
    }

    @Test
    void gcpExplicitQueueKindFails() {
        send(Type.GCP, "t", Kind.QUEUE);
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".kind não se aplica a este provedor"));
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"AZURE", "ARTEMIS", "RABBITMQ"})
    void brokerReceiveWithSubscriptionIsTopic(Type type) {
        Destination d = receive(type, "orders", "billing");
        assertThat(problems).isEmpty();
        assertThat(d.topic()).isEqualTo("orders");
        assertThat(d.subscription()).isEqualTo("billing");
        assertThat(d.queue()).isNull();
    }

    @Test
    void activeMqClassicReceiveWithSubscriptionIsVirtualTopic() {
        Destination d = receive(Type.ACTIVEMQ_CLASSIC, "VirtualTopic.orders", "billing");
        assertThat(problems).isEmpty();
        assertThat(d.topic()).isEqualTo("VirtualTopic.orders");
        assertThat(d.subscription()).isEqualTo("billing");
    }

    @Test
    void brokerSendTopicKind() {
        assertThat(send(Type.RABBITMQ, "orders", Kind.TOPIC).topic()).isEqualTo("orders");
        Destination queue = send(Type.RABBITMQ, "orders", null);
        assertThat(queue.queue()).isEqualTo("orders");
        assertThat(queue.topic()).isNull();
        assertThat(problems).isEmpty();
    }

    @Test
    void gcpReceiveIsSubscription() {
        Destination d = receive(Type.GCP, "orders-sub", null);
        assertThat(problems).isEmpty();
        assertThat(d.subscription()).isEqualTo("orders-sub");
        assertThat(d.topic()).isNull();
    }

    @Test
    void gcpSendIsTopic() {
        Destination d = send(Type.GCP, "orders", null);
        assertThat(problems).isEmpty();
        assertThat(d.topic()).isEqualTo("orders");
    }

    @Test
    void destinationIsRequired() {
        receive(Type.AWS, null, null);
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".destination é obrigatório"));
    }

    @Test
    void optionsAreValidatedByProviderType() {
        Endpoints.resolve(KEY, "p", provider(Type.AWS), new Endpoint("q", null, null,
                new Options(null, true, null, null, null, null, null, null, null, null, null, null)), true, problems);
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".options.sessions só vale para o Service Bus"));
    }

    @Test
    void providerFallsBackToTheOnlyOne() {
        assertThat(Endpoints.provider(KEY, null, null, Map.of("aws", provider(Type.AWS)), problems)).isEqualTo("aws");
        assertThat(problems).isEmpty();
    }

    @Test
    void providerFallsBackToTheDefault() {
        Map<String, Provider> two = Map.of("a", provider(Type.AWS), "b", provider(Type.AWS));
        assertThat(Endpoints.provider(KEY, null, "b", two, problems)).isEqualTo("b");
        assertThat(Endpoints.provider(KEY, "a", "b", two, problems)).isEqualTo("a");
        assertThat(problems).isEmpty();
    }

    @Test
    void providerRequiredWithSeveral() {
        Map<String, Provider> two = Map.of("a", provider(Type.AWS), "b", provider(Type.AWS));
        assertThat(Endpoints.provider("messaging.senders.s", null, null, two, problems)).isNull();
        assertThat(problems).containsExactly("messaging.senders.s.provider é obrigatório com mais de um provider");
    }

    @Test
    void unknownProvider() {
        Endpoints.provider(KEY, "x", null, Map.of("aws", provider(Type.AWS)), problems);
        assertThat(problems).containsExactly(KEY + ".provider 'x' não existe em messaging.providers");
    }
}
