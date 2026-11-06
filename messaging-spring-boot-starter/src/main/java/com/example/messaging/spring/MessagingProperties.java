package com.example.messaging.spring;

import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * {@code messaging.providers.<nome>} e {@code messaging.senders.<nome>} (ADR-0008). Os receivers
 * ({@code messaging.receivers}) são da inbox; o endereço deles usa {@link Endpoint} e {@link Options} daqui.
 *
 * <pre>{@code
 * messaging:
 *   providers:
 *     aws: { type: aws, region: sa-east-1 }
 *   senders:
 *     pedidos:
 *       provider: aws            # opcional com um provider só
 *       destination: https://sqs.sa-east-1.amazonaws.com/123/pedidos.fifo
 *       options: { require: [ordered-delivery] }
 * }</pre>
 *
 * Chave desconhecida derruba o startup.
 */
public record MessagingProperties(Map<String, Provider> providers, Map<String, Sender> senders) {

    public MessagingProperties {
        providers = providers == null ? Map.of() : Map.copyOf(providers);
        senders = senders == null ? Map.of() : Map.copyOf(senders);
    }

    /**
     * Liga {@code messaging.providers} e {@code messaging.senders}, cada raiz em modo estrito. As outras chaves de
     * {@code messaging.*} (receivers) não são deste binding.
     *
     * @throws IllegalStateException com {@code messaging.destinations}, removido
     */
    public static MessagingProperties bind(Binder binder) {
        if (binder.bind("messaging.destinations", Bindable.mapOf(String.class, Object.class)).isBound()) {
            throw new IllegalStateException("messaging.destinations foi removido; use messaging.senders e "
                    + "messaging.receivers, com o endereço em destination (ADR-0008)");
        }
        BindHandler strict = new NoUnboundElementsBindHandler(BindHandler.DEFAULT);
        return new MessagingProperties(
                binder.bind("messaging.providers", Bindable.mapOf(String.class, Provider.class), strict).orElse(null),
                binder.bind("messaging.senders", Bindable.mapOf(String.class, Sender.class), strict).orElse(null));
    }

    /** {@code activemq-classic}, {@code artemis} e {@code rabbitmq} no YAML. */
    public enum Type { AWS, AZURE, GCP, ACTIVEMQ_CLASSIC, ARTEMIS, RABBITMQ }

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
     * @param brokerUrl        ActiveMQ: URL do broker ({@code tcp://...}, {@code failover:(...)} no Classic); sem
     *                         ela, o bean {@code jakarta.jms.ConnectionFactory} da aplicação, sem pool por cima.
     *                         No Artemis, ganha {@code consumerWindowSize=0} quando não define a janela
     *                         RabbitMQ: URI AMQP ({@code amqp://host:5672/vhost}); sem ela, o bean
     *                         {@code com.rabbitmq.client.ConnectionFactory}
     * @param user             ActiveMQ e RabbitMQ: usuário da conexão
     * @param password         ActiveMQ e RabbitMQ: senha de {@code user}
     */
    public record Provider(Type type, String region, URI endpoint, String accessKey, String secretKey,
                           String connectionString, String project, String emulatorHost, String brokerUrl,
                           String user, String password) {
    }

    /** Capacidades que um destino pode exigir no startup ({@code options.require}). */
    public enum Requirement {
        ORDERED_DELIVERY, DELAYED_REDELIVERY, NATIVE_DEAD_LETTER, PUBLISHER_DEDUPLICATION, LEASE_EXPIRED_ON_ACK
    }

    /**
     * {@code nack} no Service Bus (ADR-0005) e no ActiveMQ (ADR-0009): {@code RESCHEDULE} é a cópia
     * agendada ({@code Redelivery.SCHEDULED} no ActiveMQ, que exige {@code schedulerSupport} no broker).
     */
    public enum Redelivery { ABANDON, RESCHEDULE }

    /** Envio para fila ({@code QUEUE}, padrão) ou tópico. No AWS, o ARN do SNS já diz que é tópico; no GCP, sempre tópico. */
    public enum Kind { QUEUE, TOPIC }

    /**
     * Opções do provedor de um endereço; todas opcionais, e cada uma só vale para alguns tipos.
     *
     * @param require         capacidades exigidas; o startup falha se o adapter não tiver
     * @param sessions        Azure: entidade com sessions
     * @param redelivery      Azure e ActiveMQ: {@code nack} imediato ({@code ABANDON}, padrão) ou por cópia
     *                        agendada ({@code RESCHEDULE}; no Azure, só filas). RabbitMQ: só {@code ABANDON}
     * @param maxMessageBytes SNS: {@code MaximumMessageSize} do tópico; Azure: limite da entidade (padrão 256 KB);
     *                        ActiveMQ: limite do sender (padrão perto de 100 MiB, o {@code maxFrameSize} do broker);
     *                        RabbitMQ: {@code max_message_size} do broker (padrão 16 MiB)
     * @param ackDeadline     GCP: ack deadline da subscription (obrigatório para receber)
     * @param ordered         GCP: subscription com {@code enable_message_ordering}
     * @param exactlyOnce     GCP: subscription com exactly-once delivery
     * @param deadLetterQueue Azure: só recebe, da DLQ nativa da fila ou da subscription
     * @param lease           ActiveMQ e RabbitMQ: prazo controlado pela lib até a mensagem voltar ao broker
     *                        (padrão 60 s)
     * @param routingKey      RabbitMQ: routing key do envio ao exchange ({@code kind: topic}); padrão {@code ""}
     * @param routingByOrderingKey RabbitMQ: routing key de cada envio ao exchange é o {@code orderingKey};
     *                        não combina com {@code routingKey}
     * @param confirmTimeout  RabbitMQ: espera máxima do publisher confirm (padrão 30 s)
     */
    public record Options(Set<Requirement> require, Boolean sessions, Redelivery redelivery, Long maxMessageBytes,
                          Duration ackDeadline, Boolean ordered, Boolean exactlyOnce, Boolean deadLetterQueue,
                          Duration lease, String routingKey, Boolean routingByOrderingKey, Duration confirmTimeout) {

        public static final Options NONE = new Options(null, null, null, null, null, null, null, null, null, null,
                null, null);

        public Options {
            require = require == null ? Set.of() : Set.copyOf(require);
        }
    }

    /**
     * Um endereço no provedor. O que {@code destination} significa depende do tipo (ADR-0008):
     *
     * <table>
     *   <tr><th>Tipo</th><th>Receber</th><th>Enviar</th></tr>
     *   <tr><td>aws</td><td>URL da fila ({@code http(s)://}), senão nome da fila</td>
     *       <td>ARN do SNS, URL da fila, senão nome da fila</td></tr>
     *   <tr><td>azure, activemq-classic, artemis, rabbitmq</td><td>com {@code subscription}, o tópico; senão a fila</td>
     *       <td>com {@code kind: topic}, o tópico; senão a fila</td></tr>
     *   <tr><td>gcp</td><td>a subscription</td><td>o tópico</td></tr>
     * </table>
     *
     * @param subscription só para receber de um tópico
     * @param kind         só para enviar
     */
    public record Endpoint(String destination, String subscription, Kind kind, Options options) {

        public Endpoint {
            options = options == null ? Options.NONE : options;
        }
    }

    /** {@code messaging.senders.<nome>}: {@code provider} é opcional quando existe um provider só. */
    public record Sender(String provider, String destination, Kind kind, Options options) {

        public Sender {
            options = options == null ? Options.NONE : options;
        }

        public Endpoint endpoint() {
            return new Endpoint(destination, null, kind, options);
        }
    }

    /**
     * Um endereço já traduzido para os campos de cada provedor; é o que os {@link ProviderConnection} consomem.
     * Criado por {@link Endpoints#resolve}, nunca ligado a properties.
     *
     * @param queue no AWS, o nome da fila, resolvido para a URL pela conexão
     */
    public record Destination(String queueUrl, String topicArn, String queue, String topic, String subscription,
                              Set<Requirement> require, boolean sessions, Redelivery redelivery, Long maxMessageBytes,
                              Duration ackDeadline, boolean ordered, boolean exactlyOnce, boolean deadLetterQueue,
                              Duration lease, String routingKey, boolean routingByOrderingKey,
                              Duration confirmTimeout) {

        public Destination {
            require = require == null ? Set.of() : Set.copyOf(require);
            redelivery = redelivery == null ? Redelivery.ABANDON : redelivery;
        }
    }
}
