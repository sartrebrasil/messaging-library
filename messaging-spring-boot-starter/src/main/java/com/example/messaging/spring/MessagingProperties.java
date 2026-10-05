package com.example.messaging.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * {@code messaging.providers.<nome>} e {@code messaging.destinations.<nome>} (ADR-0008).
 *
 * <pre>{@code
 * messaging:
 *   providers:
 *     aws: { type: aws, region: sa-east-1 }
 *   destinations:
 *     pedidos:
 *       provider: aws
 *       queue-url: https://sqs.sa-east-1.amazonaws.com/123/pedidos.fifo
 *       dead-letter: pedidos-dlq
 *       require: [ordered-delivery]
 * }</pre>
 */
@ConfigurationProperties("messaging")
public record MessagingProperties(Map<String, Provider> providers, Map<String, Destination> destinations) {

    public MessagingProperties {
        providers = providers == null ? Map.of() : Map.copyOf(providers);
        destinations = destinations == null ? Map.of() : Map.copyOf(destinations);
    }

    public enum Type { AWS, AZURE, GCP }

    /**
     * Conexão com um provedor. Cada tipo usa só os seus campos:
     *
     * @param region           AWS: região; sem ela, a cadeia padrão do SDK
     * @param endpoint         AWS: endpoint alternativo (LocalStack)
     * @param accessKey        AWS: credencial estática; sem ela, a cadeia padrão do SDK
     * @param secretKey        AWS: par de {@code accessKey}
     * @param connectionString Azure: connection string do namespace; sem ela, um bean {@code ServiceBusClientBuilder}
     * @param project          GCP: projeto, usado para completar nomes curtos de tópico e subscription
     * @param emulatorHost     GCP: {@code host:porta} do emulator (canal sem TLS e sem credenciais)
     */
    public record Provider(Type type, String region, URI endpoint, String accessKey, String secretKey,
                           String connectionString, String project, String emulatorHost) {
    }

    /** Capacidades que um destino pode exigir no startup ({@code require}). */
    public enum Requirement {
        ORDERED_DELIVERY, DELAYED_REDELIVERY, NATIVE_DEAD_LETTER, PUBLISHER_DEDUPLICATION, LEASE_EXPIRED_ON_ACK
    }

    /** {@code nack} no Service Bus (ADR-0005). */
    public enum Redelivery { ABANDON, RESCHEDULE }

    /**
     * Um destino. O endereço de envio e o de recebimento dependem do tipo do provedor:
     *
     * <table>
     *   <tr><th>Tipo</th><th>Envio</th><th>Recebimento</th></tr>
     *   <tr><td>aws</td><td>{@code topic-arn}, senão {@code queue-url}</td><td>{@code queue-url}</td></tr>
     *   <tr><td>azure</td><td>{@code topic}, senão {@code queue}</td><td>{@code topic} + {@code subscription}, senão {@code queue}</td></tr>
     *   <tr><td>gcp</td><td>{@code topic}</td><td>{@code subscription}</td></tr>
     * </table>
     *
     * @param deadLetter      destino que recebe a cópia de {@code deadLetter} (AWS e GCP; o Service Bus é nativo)
     * @param require         capacidades exigidas; o startup falha se o adapter não tiver
     * @param sessions        Azure: entidade com sessions
     * @param redelivery      Azure: {@code nack} por {@code ABANDON} (padrão) ou {@code RESCHEDULE} (só filas)
     * @param maxMessageBytes SNS: {@code MaximumMessageSize} do tópico; Azure: limite da entidade (padrão 256 KB)
     * @param ackDeadline     GCP: ack deadline da subscription (obrigatório para receber)
     * @param ordered         GCP: subscription com {@code enable_message_ordering}
     * @param exactlyOnce     GCP: subscription com exactly-once delivery
     */
    public record Destination(String provider, String queueUrl, String topicArn, String queue, String topic,
                              String subscription, String deadLetter, Set<Requirement> require, boolean sessions,
                              Redelivery redelivery, Long maxMessageBytes, Duration ackDeadline, boolean ordered,
                              boolean exactlyOnce) {

        public Destination {
            require = require == null ? Set.of() : Set.copyOf(require);
            redelivery = redelivery == null ? Redelivery.ABANDON : redelivery;
        }
    }
}
