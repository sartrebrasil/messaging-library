package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.aws.SnsMessageSender;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.spring.MessagingProperties.Options;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Sem rede: os clientes do SDK só conectam no primeiro uso. */
class MessagingAutoConfigurationTest {

    private static final String AWS = "messaging.providers.aws.";
    private static final String QUEUE = "http://localhost:4566/000000000000/";
    private static final String AZURE = "Endpoint=sb://localhost;SharedAccessKeyName=k;SharedAccessKey=v;"
            + "UseDevelopmentEmulator=true;";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class,
                    MessagingHealthAutoConfiguration.class))
            .withPropertyValues(AWS + "type=aws")
            .withBean(SqsClient.class, () -> TestAwsClients.sqs(URI.create("http://localhost:4566")))
            .withBean(SnsClient.class, () -> TestAwsClients.sns(URI.create("http://localhost:4566")));

    /** Sem o provider {@code aws} do {@link #runner}: os testes de nome de provider declaram o único provider AWS. */
    private final ApplicationContextRunner bare = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class,
                    MessagingHealthAutoConfiguration.class));

    private static Endpoint queue(String url) {
        return new Endpoint(url, null, null, null);
    }

    @Test
    void createsSendersByNameAndReceiversByEndpoint() {
        runner.withPropertyValues(
                        "messaging.senders.pedidos.destination=" + QUEUE + "pedidos",
                        "messaging.senders.eventos.destination=arn:aws:sns:us-east-1:000000000000:eventos",
                        "messaging.senders.eventos.options.max-message-bytes=1048576")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    assertThat(destinations.names()).containsExactly("eventos", "pedidos");

                    MessageSender sender = destinations.sender("pedidos");
                    assertThat(destinations.sender("pedidos")).isSameAs(sender);
                    assertThat(destinations.sender("eventos").capabilities().maxMessageBytes()).isEqualTo(1048576);
                    assertThatThrownBy(() -> destinations.sender("nao-existe"))
                            .isInstanceOf(IllegalArgumentException.class);

                    MessageSender dlq = destinations.sender("r.dead-letter", "aws", queue(QUEUE + "pedidos-dlq"));
                    assertThat(destinations.sender("r.dead-letter", "aws", queue(QUEUE + "pedidos-dlq"))).isSameAs(dlq);
                    MessageReceiver first = destinations.receiver("r", null, queue(QUEUE + "pedidos"), dlq);
                    MessageReceiver second = destinations.receiver("r", null, queue(QUEUE + "pedidos"), dlq);
                    assertThat(first).isNotSameAs(second);
                    // DLQ por cópia, pelo sender recebido
                    assertThat(first.capabilities().nativeDeadLetter()).isFalse();
                });
    }

    @Test
    void snsSenderHasTheSnsLimit() {
        runner.withPropertyValues("messaging.senders.eventos.destination=arn:aws:sns:us-east-1:000000000000:eventos")
                .run(context -> assertThat(context.getBean(MessagingDestinations.class).sender("eventos")
                        .capabilities().maxMessageBytes()).isEqualTo(SnsMessageSender.DEFAULT_MAX_MESSAGE_BYTES));
    }

    /** A receiver closed by its user (one per level-3 pass) is not kept until shutdown (review I2). */
    @Test
    void closedReceiversAreForgotten() {
        runner.run(context -> {
            MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
            for (int i = 0; i < 3; i++) {
                destinations.receiver("r.dead-letter", "aws", queue(QUEUE + "dlq"), null).close();
            }
            assertThat(destinations.openReceivers()).isZero();
        });
    }

    @Test
    void receiverByEndpointIsInHealth() {
        runner.run(context -> {
            MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
            destinations.receiver("r", "aws", queue(QUEUE + "pedidos"), null).close();
            assertThat(destinations.names()).containsExactly("r");
        });
    }

    @Test
    void invalidConfigurationFailsStartupListingEveryProblem() {
        runner.withPropertyValues(
                        "messaging.senders.sem-provider.provider=nao-existe",
                        "messaging.senders.sem-provider.destination=x",
                        "messaging.senders.sem-endereco.provider=aws",
                        "messaging.senders.sessions.destination=" + QUEUE + "x",
                        "messaging.senders.sessions.options.sessions=true",
                        "messaging.senders.topico.destination=eventos",
                        "messaging.senders.topico.kind=topic")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.senders.sem-provider.provider 'nao-existe' não existe")
                        .hasMessageContaining("messaging.senders.sem-endereco.destination é obrigatório")
                        .hasMessageContaining("messaging.senders.sessions.options.sessions não se aplica ao provedor aws")
                        .hasMessageContaining("messaging.senders.topico.kind: topic exige um ARN do SNS"));
    }

    @Test
    void unknownSenderKeyFails() {
        runner.withPropertyValues("messaging.senders.s.destination=" + QUEUE + "x",
                        "messaging.senders.s.queue-url=" + QUEUE + "x")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.senders.s.queue-url"));
    }

    @Test
    void legacyDestinationsFail() {
        runner.withPropertyValues("messaging.destinations.x.provider=aws")
                .run(context -> assertThat(context).getFailure().rootCause().hasMessageContaining(
                        "messaging.destinations foi removido; use messaging.senders e messaging.receivers"));
    }

    @Test
    void senderNameReusedWithAnotherAddressFails() {
        runner.run(context -> {
            MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
            destinations.sender("r.retry.0", "aws", queue(QUEUE + "a"));
            assertThat(destinations.sender("r.retry.0", "aws", queue(QUEUE + "a"))).isNotNull();
            assertThatThrownBy(() -> destinations.sender("r.retry.0", "aws", queue(QUEUE + "b")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("r.retry.0");
        });
    }

    @Test
    void unknownTopLevelKeyFails() {
        runner.withPropertyValues("messaging.receivers-enable=false")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.receivers-enable"));
        runner.withPropertyValues("messaging.reciever-defaults.poll-wait=1s")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.reciever-defaults"));
    }

    @Test
    void environmentVariableFormsOfTheRootsAreAccepted() {
        bare.withInitializer(c -> c.getEnvironment().getPropertySources().addFirst(
                        new org.springframework.core.env.SystemEnvironmentPropertySource("env", java.util.Map.of(
                                "MESSAGING_RECEIVERDEFAULTS_POLLWAIT", "1s", "MESSAGING_RECEIVERSENABLED", "true"))))
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void legacyInboxKeysFail() {
        for (String key : java.util.List.of("inbox.consumers.x.destination", "inbox.defaults.key-lanes",
                "inbox.enabled", "inbox.brokers.messaging.type")) {
            runner.withPropertyValues(key + "=1").run(context -> assertThat(context).getFailure().rootCause()
                    .as(key).hasMessageContaining("inbox.* was removed; use messaging.receivers"));
        }
    }

    @Test
    void requiredCapabilitiesAreCheckedAtStartup() {
        runner.withPropertyValues("messaging.senders.pedidos.destination=" + QUEUE + "pedidos",
                        "messaging.senders.pedidos.options.require=ordered-delivery,lease-expired-on-ack")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("pedidos.options.require ORDERED_DELIVERY: o sender não suporta")
                        .hasMessageContaining("pedidos.options.require LEASE_EXPIRED_ON_ACK: exige endereço de recebimento"));

        runner.withPropertyValues("messaging.senders.pedidos.destination=" + QUEUE + "pedidos.fifo",
                        "messaging.senders.pedidos.options.require=ordered-delivery")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void receiverRequirementsAreCheckedOnCreation() {
        runner.run(context -> {
            MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
            Endpoint lease = new Endpoint(QUEUE + "pedidos", null, null, new Options(
                    Set.of(MessagingProperties.Requirement.LEASE_EXPIRED_ON_ACK), null, null, null, null,
                    null, null, null, null, null, null, null));
            assertThatThrownBy(() -> destinations.receiver("r", "aws", lease, null))
                    .hasMessageContaining("r.options.require LEASE_EXPIRED_ON_ACK: o receiver não suporta");
        });
    }

    @Test
    void azureAndGcpRulesAreValidated() {
        runner.withPropertyValues("messaging.providers.azure.type=azure", "messaging.providers.gcp.type=gcp")
                .withBean(ServiceBusClientBuilder.class, () -> new ServiceBusClientBuilder().connectionString(AZURE))
                .withBean(TransportChannelProvider.class, () -> TestGcpClients.provider(TestGcpClients.plaintext("localhost:8085")))
                .withBean(CredentialsProvider.class, NoCredentialsProvider::create)
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    assertThat(destinations.validate("messaging.receivers.sb", "azure", new Endpoint("pedidos", null,
                            null, new Options(null, true, MessagingProperties.Redelivery.RESCHEDULE, null, null, null,
                            null, null, null, null, null, null)), true))
                            .anyMatch(p -> p.contains("sb.options.redelivery=RESCHEDULE só vale para fila sem sessions"));
                    assertThat(destinations.validate("messaging.receivers.ps", "gcp", queue("pedidos-sub"), true))
                            .anyMatch(p -> p.contains("ps.options.ack-deadline é obrigatório"))
                            .anyMatch(p -> p.contains("ps.destination: nome curto exige messaging.providers.gcp.project"));
                    assertThat(destinations.validate("messaging.receivers.x", null, queue("q"), true))
                            .anyMatch(p -> p.contains("x.provider é obrigatório com mais de um provider"));
                });
    }

    @Test
    void azureDeadLetterQueueIsReceiveOnly() {
        ApplicationContextRunner azure = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
                .withBean(ServiceBusClientBuilder.class, () -> new ServiceBusClientBuilder().connectionString(AZURE))
                .withPropertyValues("messaging.providers.azure.type=azure");
        Options dlq = new Options(null, null, null, null, null, null, null, true, null, null, null, null);
        Options sessionsDlq = new Options(null, true, null, null, null, null, null, true, null, null, null, null);

        azure.run(context -> {
            MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
            // a DLQ não tem sessions: receiver comum, mesmo com sessions na entidade
            assertThat(destinations.receiver("pedidos-dlq", null, new Endpoint("pedidos", null, null, sessionsDlq),
                    null).capabilities().orderedDelivery()).isFalse();
            assertThat(destinations.receiver("eventos-dlq", null, new Endpoint("eventos", "faturamento", null, dlq),
                    null).capabilities().nativeDeadLetter()).isTrue();
        });

        azure.withPropertyValues("messaging.senders.s.destination=pedidos",
                        "messaging.senders.s.options.dead-letter-queue=true")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.senders.s.options.dead-letter-queue só vale para receber"));
    }

    @Test
    void healthIndicatorIsOptIn() {
        runner.withPropertyValues("messaging.senders.pedidos.destination=" + QUEUE + "pedidos")
                .run(context -> assertThat(context).doesNotHaveBean(HealthIndicator.class));

        runner.withPropertyValues("messaging.senders.pedidos.destination=" + QUEUE + "pedidos",
                        "management.health.messaging.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(HealthIndicator.class));
    }

    @Test
    void durationOptionsBind() {
        runner.withBean(TransportChannelProvider.class, () -> TestGcpClients.provider(TestGcpClients.plaintext("localhost:8085")))
                .withBean(CredentialsProvider.class, NoCredentialsProvider::create)
                .withPropertyValues("messaging.providers.gcp.type=gcp", "messaging.providers.gcp.project=p",
                        "messaging.senders.pedidos.provider=gcp", "messaging.senders.pedidos.destination=pedidos",
                        "messaging.senders.pedidos.options.ack-deadline=5s")
                .run(context -> assertThat(context.getBean(MessagingProperties.class).senders().get("pedidos")
                        .options().ackDeadline()).isEqualTo(Duration.ofSeconds(5)));
    }

    @Test
    void providerFieldOfAnotherTypeFails() {
        runner.withPropertyValues("messaging.providers.r.type=rabbitmq", "messaging.providers.r.region=us-east-1")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("messaging.providers.r.region"));
    }

    @Test
    void providerWithoutTypeFails() {
        runner.withPropertyValues("messaging.providers.x.region=us-east-1")
                .run(context -> assertThat(context).getFailure().rootCause().hasMessageContaining(
                        "messaging.providers.x.type é obrigatório (aws, azure, gcp, activemq-classic, artemis ou rabbitmq)"));
    }

    @Test
    void providerNamesWithUppercaseAndUnderscoreBind() {
        bare.withPropertyValues("messaging.providers.Aws_Prod.type=aws", "messaging.providers.Aws_Prod.region=sa-east-1")
                .run(context -> assertThat(context.getBean(MessagingProperties.class).providers().values())
                        .filteredOn(p -> p instanceof MessagingProperties.AwsProvider aws
                                && "sa-east-1".equals(aws.region()))
                        .hasSize(1));
    }

    @Test
    void typedProviderBindsFromEnvironmentVariables() {
        bare.withInitializer(c -> c.getEnvironment().getPropertySources().addFirst(
                        new org.springframework.core.env.SystemEnvironmentPropertySource("test-systemEnvironment", java.util.Map.of(
                                "MESSAGING_PROVIDERS_ENVAWS_TYPE", "aws", "MESSAGING_PROVIDERS_ENVAWS_REGION", "us-west-2"))))
                .run(context -> assertThat(context.getBean(MessagingProperties.class).providers().get("envaws"))
                        .isInstanceOfSatisfying(MessagingProperties.AwsProvider.class,
                                aws -> assertThat(aws.region()).isEqualTo("us-west-2")));
    }

    @Test
    void providerNameWithADotBinds() {
        bare.withPropertyValues("messaging.providers[a.b].type=aws", "messaging.providers[a.b].region=sa-east-1")
                .run(context -> assertThat(context.getBean(MessagingProperties.class).providers())
                        .containsKey("a.b"));
    }
}
