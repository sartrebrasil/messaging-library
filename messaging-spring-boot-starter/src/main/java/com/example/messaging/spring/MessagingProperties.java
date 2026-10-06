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

    /** {@code activemq-classic} no YAML. */
    public enum Type { AWS, AZURE, GCP, ACTIVEMQ_CLASSIC }

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
     * @param brokerUrl        ActiveMQ: URL do broker ({@code tcp://...}, {@code failover:(...)}); sem ela, o
     *                         bean {@code jakarta.jms.ConnectionFactory} da aplicação, sem pool por cima
     * @param user             ActiveMQ: usuário da conexão
     * @param password         ActiveMQ: senha de {@code user}
     */
    public record Provider(Type type, String region, URI endpoint, String accessKey, String secretKey,
                           String connectionString, String project, String emulatorHost, String brokerUrl,
                           String user, String password) {
    }

    /** Capacidades que um destino pode exigir no startup ({@code require}). */
    public enum Requirement {
        ORDERED_DELIVERY, DELAYED_REDELIVERY, NATIVE_DEAD_LETTER, PUBLISHER_DEDUPLICATION, LEASE_EXPIRED_ON_ACK
    }

    /**
     * {@code nack} no Service Bus (ADR-0005) e no ActiveMQ (ADR-0009): {@code RESCHEDULE} é a cópia
     * agendada ({@code Redelivery.SCHEDULED} no ActiveMQ, que exige {@code schedulerSupport} no broker).
     */
    public enum Redelivery { ABANDON, RESCHEDULE }

    /**
     * Um destino. O endereço de envio e o de recebimento dependem do tipo do provedor:
     *
     * <table>
     *   <tr><th>Tipo</th><th>Envio</th><th>Recebimento</th></tr>
     *   <tr><td>aws</td><td>{@code topic-arn}, senão {@code queue-url}</td><td>{@code queue-url}</td></tr>
     *   <tr><td>azure</td><td>{@code topic}, senão {@code queue}</td><td>{@code topic} + {@code subscription}, senão {@code queue}</td></tr>
     *   <tr><td>gcp</td><td>{@code topic}</td><td>{@code subscription}</td></tr>
     *   <tr><td>activemq-classic</td><td>{@code topic} ({@code VirtualTopic.*}), senão {@code queue}</td>
     *       <td>{@code topic} + {@code subscription} (fila {@code Consumer.<subscription>.<topic>}), senão {@code queue}</td></tr>
     * </table>
     *
     * @param deadLetter      destino que recebe a cópia de {@code deadLetter} (todos menos o Service Bus, que é nativo)
     * @param require         capacidades exigidas; o startup falha se o adapter não tiver
     * @param sessions        Azure: entidade com sessions
     * @param redelivery      Azure e ActiveMQ: {@code nack} imediato ({@code ABANDON}, padrão) ou por cópia
     *                        agendada ({@code RESCHEDULE}; no Azure, só filas)
     * @param maxMessageBytes SNS: {@code MaximumMessageSize} do tópico; Azure: limite da entidade (padrão 256 KB);
     *                        ActiveMQ: limite do sender (padrão perto de 100 MiB, o {@code maxFrameSize} do broker)
     * @param ackDeadline     GCP: ack deadline da subscription (obrigatório para receber)
     * @param ordered         GCP: subscription com {@code enable_message_ordering}
     * @param exactlyOnce     GCP: subscription com exactly-once delivery
     * @param deadLetterQueue Azure: só recebe, da DLQ nativa da fila ou da subscription
     * @param lease           ActiveMQ: prazo controlado pela lib até a mensagem voltar ao broker (padrão 60 s)
     */
    public record Destination(String provider, String queueUrl, String topicArn, String queue, String topic,
                              String subscription, String deadLetter, Set<Requirement> require, boolean sessions,
                              Redelivery redelivery, Long maxMessageBytes, Duration ackDeadline, boolean ordered,
                              boolean exactlyOnce, boolean deadLetterQueue, Duration lease) {

        public Destination {
            require = require == null ? Set.of() : Set.copyOf(require);
            redelivery = redelivery == null ? Redelivery.ABANDON : redelivery;
        }
    }
}
