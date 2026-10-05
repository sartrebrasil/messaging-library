package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.aws.SnsMessageSender;
import com.example.messaging.aws.SqsMessageReceiver;
import com.example.messaging.aws.SqsMessageSender;
import org.springframework.beans.factory.ListableBeanFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import java.time.Duration;

/**
 * SQS e SNS. Beans {@code SqsClient}/{@code SnsClient} da aplicação valem para todos os
 * providers {@code aws}; sem eles, um cliente por provider com {@code apache-client} e read
 * timeout de 30 s (acima dos 20 s do long poll).
 */
final class AwsProviderFactory implements ProviderFactory {

    private final SqsClient sqs;
    private final SnsClient sns;
    private final boolean ownsSqs;
    private final boolean ownsSns;

    AwsProviderFactory(MessagingProperties.Provider provider, ListableBeanFactory beans) {
        SqsClient userSqs = beans.getBeanProvider(SqsClient.class).getIfAvailable();
        SnsClient userSns = beans.getBeanProvider(SnsClient.class).getIfAvailable();
        this.ownsSqs = userSqs == null;
        this.ownsSns = userSns == null;
        this.sqs = ownsSqs ? configure(SqsClient.builder(), provider).build() : userSqs;
        this.sns = ownsSns ? configure(SnsClient.builder(), provider).build() : userSns;
    }

    private static SqsClientBuilder configure(SqsClientBuilder builder, MessagingProperties.Provider provider) {
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

    private static SnsClientBuilder configure(SnsClientBuilder builder, MessagingProperties.Provider provider) {
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

    private static AwsCredentialsProvider credentials(MessagingProperties.Provider provider) {
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
        return destination.queueUrl() == null ? null : new SqsMessageSender(sqs, destination.queueUrl());
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        return destination.queueUrl() == null ? null
                : new SqsMessageReceiver(sqs, destination.queueUrl(), deadLetterSender);
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
