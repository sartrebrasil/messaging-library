package com.example.messaging.spring.exemplos;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

import java.net.URI;
import java.time.Duration;

/**
 * Como a aplicação declara o {@link SqsClient} e o {@link SnsClient} que o starter usa com {@code type: aws} (ADR-0011).
 * O starter não cria cliente nem credencial.
 *
 * <p><b>Read timeout acima do long poll.</b> O receiver SQS espera até 20 s por mensagem. O cliente HTTP daqui usa 30 s
 * (o padrão do AWS SDK 2.55 também é 30 s); abaixo de 20 s o long poll é cortado e o receive falha de forma
 * intermitente. O starter não consegue conferir isso: o SDK não expõe o timeout de um cliente pronto. Se o cliente usado
 * para publicar precisar de um {@code apiCallTimeout} curto (o outbox, por exemplo), declare um segundo
 * {@code SqsClient} com {@code autowireCandidate = false} ou {@code @Primary} no que o starter deve usar.</p>
 */
public final class AwsClientsExample {

    /** Acima dos 20 s do long poll do receiver SQS. */
    private static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(30);

    private AwsClientsExample() {
    }

    /**
     * @param regionOrNull   sem valor, a cadeia padrão do SDK ({@code AWS_REGION})
     * @param endpointOrNull sem valor, a AWS; com valor, LocalStack ou outro compatível
     */
    public static SqsClient sqsClient(AwsCredentialsProvider credentials, Region regionOrNull, URI endpointOrNull) {
        SqsClientBuilder builder = SqsClient.builder().credentialsProvider(credentials)
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(SOCKET_TIMEOUT));
        if (regionOrNull != null) {
            builder.region(regionOrNull);
        }
        if (endpointOrNull != null) {
            builder.endpointOverride(endpointOrNull);
        }
        return builder.build();
    }

    /** Só é necessário com algum sender para um ARN do SNS. */
    public static SnsClient snsClient(AwsCredentialsProvider credentials, Region regionOrNull, URI endpointOrNull) {
        SnsClientBuilder builder = SnsClient.builder().credentialsProvider(credentials)
                .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(SOCKET_TIMEOUT));
        if (regionOrNull != null) {
            builder.region(regionOrNull);
        }
        if (endpointOrNull != null) {
            builder.endpointOverride(endpointOrNull);
        }
        return builder.build();
    }

    /**
     * Produção: credenciais da cadeia padrão da AWS (variáveis, IRSA, perfil, metadata), renovadas em segundo plano; um só
     * provider para os dois clientes, fechado pelo Spring. Região e endpoint, quando necessários, vêm de
     * {@code AWS_REGION} e {@code AWS_ENDPOINT_URL_SQS}/{@code AWS_ENDPOINT_URL_SNS}.
     */
    @Configuration(proxyBeanMethods = false)
    public static class Producao {

        @Bean
        AwsCredentialsProvider awsCredentialsProvider() {
            return DefaultCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
        }

        @Bean
        SqsClient sqsClient(AwsCredentialsProvider credentials) {
            return AwsClientsExample.sqsClient(credentials, null, null);
        }

        @Bean
        SnsClient snsClient(AwsCredentialsProvider credentials) {
            return AwsClientsExample.snsClient(credentials, null, null);
        }
    }

    /** Emulador: LocalStack em {@code http://localhost:4566}, credencial estática e região fixa. */
    @Configuration(proxyBeanMethods = false)
    public static class Emulador {

        private static final AwsCredentialsProvider CREDENTIALS =
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
        private static final URI ENDPOINT = URI.create("http://localhost:4566");

        @Bean
        SqsClient sqsClient() {
            return AwsClientsExample.sqsClient(CREDENTIALS, Region.US_EAST_1, ENDPOINT);
        }

        @Bean
        SnsClient snsClient() {
            return AwsClientsExample.snsClient(CREDENTIALS, Region.US_EAST_1, ENDPOINT);
        }
    }
}
