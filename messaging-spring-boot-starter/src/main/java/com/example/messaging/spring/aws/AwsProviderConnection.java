package com.example.messaging.spring.aws;

import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.aws.SnsMessageSender;
import com.example.messaging.aws.SqsMessageReceiver;
import com.example.messaging.aws.SqsMessageSender;
import com.example.messaging.spring.ClientBeans;
import com.example.messaging.spring.MessagingProperties;
import com.example.messaging.spring.ProviderConnection;
import org.springframework.beans.factory.ListableBeanFactory;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQS e SNS. Beans {@code SqsClient}/{@code SnsClient} da aplicação valem pela regra de {@link ClientBeans}, e só
 * para um provider sem {@code region}, {@code endpoint} nem credenciais; sem eles, um cliente por provider com {@code apache-client} e read
 * timeout de 30 s (acima dos 20 s do long poll).
 */
public final class AwsProviderConnection implements ProviderConnection {

    private final SqsClient sqs;
    private final SnsClient sns;
    private final boolean ownsSqs;
    private final boolean ownsSns;
    /** URL de cada nome de fila, resolvida uma vez ({@code GetQueueUrl}). */
    private final Map<String, String> queueUrls = new ConcurrentHashMap<>();

    public AwsProviderConnection(String providerName, MessagingProperties.AwsProvider provider, ListableBeanFactory beans) {
        SqsClient userSqs = ClientBeans.optional(beans, SqsClient.class, providerName);
        SnsClient userSns = ClientBeans.optional(beans, SnsClient.class, providerName);
        if (userSqs != null && userSns != null) {
            rejectConnectionFields(providerName, provider);
        }
        this.ownsSqs = userSqs == null;
        this.ownsSns = userSns == null;
        this.sqs = ownsSqs ? configure(SqsClient.builder(), provider).build() : userSqs;
        this.sns = ownsSns ? configure(SnsClient.builder(), provider).build() : userSns;
    }

    private static boolean hasConnectionFields(MessagingProperties.AwsProvider provider) {
        return provider.region() != null || provider.endpoint() != null || provider.accessKey() != null
                || provider.secretKey() != null;
    }

    /**
     * Com os dois beans da aplicação, as properties de conexão não configurariam nenhum cliente e seriam ignoradas
     * em silêncio. Com um só, elas ainda configuram o outro cliente (o {@code SnsClient} precisa de região).
     */
    private static void rejectConnectionFields(String providerName, MessagingProperties.AwsProvider provider) {
        if (hasConnectionFields(provider)) {
            throw new IllegalStateException("messaging.providers." + providerName + ": defina region, endpoint, "
                    + "access-key e secret-key ou os beans SqsClient e SnsClient, não os dois");
        }
    }

    private static <B extends AwsClientBuilder<B, C> & AwsSyncClientBuilder<B, C>, C> B configure(
            B builder, MessagingProperties.AwsProvider provider) {
        builder.credentialsProvider(credentials(provider))
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30)));
        if (provider.region() != null) {
            builder.region(Region.of(provider.region()));
        }
        if (provider.endpoint() != null) {
            builder.endpointOverride(provider.endpoint());
        }
        return builder;
    }

    private static AwsCredentialsProvider credentials(MessagingProperties.AwsProvider provider) {
        return provider.accessKey() != null
                ? StaticCredentialsProvider.create(AwsBasicCredentials.create(provider.accessKey(), provider.secretKey()))
                : DefaultCredentialsProvider.builder().build();
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        if (destination.topicArn() != null) {
            long max = destination.maxMessageBytes() != null
                    ? destination.maxMessageBytes() : SnsMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
            return new SnsMessageSender(sns, destination.topicArn(), max);
        }
        return new SqsMessageSender(sqs, queueUrl(destination));
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        return new SqsMessageReceiver(sqs, queueUrl(destination), deadLetterSender);
    }

    /** {@code queue-url}, ou o nome da fila resolvido no primeiro uso; exige {@code sqs:GetQueueUrl}. */
    private String queueUrl(MessagingProperties.Destination destination) {
        if (destination.queueUrl() != null) {
            return destination.queueUrl();
        }
        return queueUrls.computeIfAbsent(destination.queue(), name -> {
            try {
                return sqs.getQueueUrl(r -> r.queueName(name)).queueUrl();
            } catch (QueueDoesNotExistException e) {
                throw new DestinationNotFoundException("aws", "fila SQS '" + name + "' não existe", e);
            }
        });
    }

    @Override
    public void close() {
        if (ownsSqs) {
            sqs.close();
        }
        if (ownsSns) {
            sns.close();
        }
    }
}
