package com.example.messaging.aws;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/**
 * Um LocalStack por JVM, compartilhado pelos contratos. Fixado em 4.14.0, a última imagem que
 * roda sem {@code LOCALSTACK_AUTH_TOKEN} (plano, seção Testes).
 */
final class LocalStack {

    static final Duration LEASE = Duration.ofSeconds(2);

    private static GenericContainer<?> container;
    private static SqsClient sqs;
    private static SnsClient sns;

    private LocalStack() {
    }

    static synchronized void start() {
        if (container != null) {
            return;
        }
        container = new GenericContainer<>("localstack/localstack:4.14.0")
                .withEnv("SERVICES", "sqs,sns")
                .withExposedPorts(4566)
                .waitingFor(Wait.forLogMessage(".*Ready\\.\\n", 1));
        container.start();
        URI endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(4566));
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
        // read timeout acima dos 20 s do long poll
        var http = ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30));
        sqs = SqsClient.builder().region(Region.US_EAST_1).endpointOverride(endpoint)
                .credentialsProvider(credentials).httpClientBuilder(http).build();
        sns = SnsClient.builder().region(Region.US_EAST_1).endpointOverride(endpoint)
                .credentialsProvider(credentials).httpClientBuilder(http).build();
    }

    static SqsClient sqs() {
        return sqs;
    }

    static SnsClient sns() {
        return sns;
    }

    /** Cria (ou reaproveita) uma fila com o lease do contrato; FIFO quando o nome termina em {@code .fifo}. */
    static String queue(String name) {
        Map<QueueAttributeName, String> attributes = name.endsWith(".fifo")
                ? Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, Long.toString(LEASE.toSeconds()),
                QueueAttributeName.FIFO_QUEUE, "true",
                QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true")
                : Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, Long.toString(LEASE.toSeconds()));
        return sqs.createQueue(b -> b.queueName(name).attributes(attributes)).queueUrl();
    }

    static String queueArn(String queueUrl) {
        return sqs.getQueueAttributes(b -> b.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    /** URL de uma fila que não existe, no formato do LocalStack. */
    static String missingQueueUrl() {
        return queue("referencia").replace("referencia", "nao-existe");
    }
}
