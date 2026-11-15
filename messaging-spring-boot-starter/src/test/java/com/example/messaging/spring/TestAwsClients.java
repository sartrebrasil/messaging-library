package com.example.messaging.spring;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.time.Duration;

/** Clientes AWS que a aplicação declararia, apontados para um endpoint (LocalStack, ou uma porta sem ninguém). */
public final class TestAwsClients {

    /** Nada escuta aqui: serve aos testes que só montam o contexto, sem chamar a AWS. */
    public static final URI UNREACHABLE = URI.create("http://localhost:1");

    private TestAwsClients() {
    }

    public static SqsClient sqs(URI endpoint) {
        return SqsClient.builder().region(Region.US_EAST_1).endpointOverride(endpoint)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30))).build();
    }

    public static SnsClient sns(URI endpoint) {
        return SnsClient.builder().region(Region.US_EAST_1).endpointOverride(endpoint)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }
}
