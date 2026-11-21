package com.example.messaging.spring;

import com.example.messaging.spring.MessagingProperties.Destination;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.spring.MessagingProperties.Kind;
import com.example.messaging.spring.MessagingProperties.Options;
import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Redelivery;
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
        return TestProviders.of(type);
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
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".options.sessions não se aplica ao provedor aws"));
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

    private static Options options(Boolean sessions, Redelivery redelivery, Long maxMessageBytes, Duration ackDeadline,
                                   Duration lease, Duration confirmTimeout) {
        return new Options(null, sessions, redelivery, maxMessageBytes, ackDeadline, null, null, null, lease, null,
                null, confirmTimeout);
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> notAccepted() {
        Duration d = Duration.ofSeconds(5);
        return java.util.stream.Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(Type.AWS, "sessions", options(true, null, null, null, null, null)),
                org.junit.jupiter.params.provider.Arguments.of(Type.GCP, "redelivery", options(null, Redelivery.RESCHEDULE, null, d, null, null)),
                org.junit.jupiter.params.provider.Arguments.of(Type.AZURE, "ack-deadline", options(null, null, null, d, null, null)),
                org.junit.jupiter.params.provider.Arguments.of(Type.GCP, "max-message-bytes", options(null, null, 1L, d, null, null)),
                org.junit.jupiter.params.provider.Arguments.of(Type.AZURE, "lease", options(null, null, null, null, d, null)),
                org.junit.jupiter.params.provider.Arguments.of(Type.ARTEMIS, "confirm-timeout", options(null, null, null, null, null, d)));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("notAccepted")
    void optionNotAcceptedByTheProviderFails(Type type, String option, Options options) {
        Endpoints.resolve(KEY, "p", provider(type), new Endpoint("q", null, null, options), true, problems);
        assertThat(problems).anyMatch(p -> p.contains(KEY + ".options." + option + " não se aplica ao provedor "
                + type.tag()));
    }

    /** O provider vem de receiver-defaults: a opção é conferida contra o provider resolvido. */
    @Test
    void optionNotAcceptedByTheResolvedProviderFails() {
        Map<String, Provider> two = Map.of("a", provider(Type.AWS), "b", provider(Type.AWS));
        String resolved = Endpoints.provider(KEY, null, "b", two, problems);
        Endpoints.resolve(KEY, resolved, two.get(resolved), new Endpoint("q", null, null,
                options(true, null, null, null, null, null)), true, problems);
        assertThat(problems).containsExactly(KEY + ".options.sessions não se aplica ao provedor aws; vale para: azure");
    }

    @Test
    void falseAndEmptyOptionsAreNotSet() {
        Endpoints.resolve(KEY, "p", provider(Type.AWS), new Endpoint("q", null, null,
                options(false, Redelivery.ABANDON, null, null, null, null)), true, problems);
        Endpoints.resolve(KEY, "p", provider(Type.GCP), new Endpoint("s", null, null,
                new Options(java.util.Set.of(), null, null, null, Duration.ofSeconds(30), null, null, null, null, null,
                        null, null)), true, problems);
        assertThat(problems).isEmpty();
    }

    @Test
    void everyOptionIsInTheTable() {
        for (var component : Options.class.getRecordComponents()) {
            String kebab = component.getName().replaceAll("([A-Z])", "-$1").toLowerCase();
            assertThat(Endpoints.ACCEPTED).as(kebab).containsKey(kebab);
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"arn:aws-cn:sns:cn-north-1:1:t",
            "arn:aws-us-gov:sns:us-gov-west-1:1:t"})
    void awsTopicArnInAnyPartitionIsATopic(String arn) {
        Destination d = send(Type.AWS, arn, null);
        assertThat(problems).isEmpty();
        assertThat(d.topicArn()).isEqualTo(arn);
        assertThat(d.queue()).isNull();
    }

    @Test
    void gcpAckDeadlineOutsideTheSubscriptionRangeIsRejected() {
        Endpoint tooShort = new Endpoint("s", "s", null, new Options(null, null, null, null, Duration.ofSeconds(5),
                null, null, null, null, null, null, null));
        Endpoints.resolve(KEY, "p", provider(Type.GCP), tooShort, true, problems);

        assertThat(problems).anyMatch(p -> p.contains("ack-deadline precisa estar entre 10s e 600s"));
    }

    @Test
    void awsSnsTopicArnCannotBeReceivedFrom() {
        receive(Type.AWS, "arn:aws-us-gov:sns:us-gov-west-1:1:t", null);

        assertThat(problems).anyMatch(p -> p.contains("só serve para enviar"));
    }

    @Test
    void nonPositiveMaxMessageBytesIsRejected() {
        Endpoint endpoint = new Endpoint("t", null, Kind.TOPIC, new Options(null, null, null, 0L, null, null, null,
                null, null, null, null, null));
        Endpoints.resolve(KEY, "p", provider(Type.AWS), endpoint, false, problems);

        assertThat(problems).anyMatch(p -> p.contains("max-message-bytes precisa ser positivo"));
    }
}
