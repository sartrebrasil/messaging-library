package com.example.messaging.spring;

import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
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

    /** Raízes de {@code messaging.*}, sem hífen (a forma de variável de ambiente chega assim). */
    private static final Set<String> ROOTS = Set.of("providers", "senders", "receivers", "receiverdefaults",
            "receiversenabled");

    public MessagingProperties {
        providers = providers == null ? Map.of() : Map.copyOf(providers);
        senders = senders == null ? Map.of() : Map.copyOf(senders);
    }

    /**
     * Liga {@code messaging.providers} e {@code messaging.senders}, cada raiz em modo estrito. As outras chaves de
     * {@code messaging.*} (receivers) não são deste binding.
     *
     * @throws IllegalStateException com {@code messaging.destinations} ou {@code inbox.*}, removidos
     */
    public static MessagingProperties bind(Binder binder) {
        if (binder.bind("messaging.destinations", Bindable.mapOf(String.class, Object.class)).isBound()) {
            throw new IllegalStateException("messaging.destinations foi removido; use messaging.senders e "
                    + "messaging.receivers, com o endereço em destination (ADR-0008)");
        }
        Map<String, Object> inbox = binder.bind("inbox", Bindable.mapOf(String.class, Object.class)).orElse(Map.of());
        if (inbox.keySet().stream().anyMatch(Set.of("enabled", "defaults", "consumers", "brokers")::contains)) {
            throw new IllegalStateException("inbox.* was removed; use messaging.receivers, messaging.receiver-defaults "
                    + "and messaging.receivers-enabled (ADR-0008)");
        }
        // the roots themselves are strict too: a typo like receivers-enable would otherwise fall back to the default
        binder.bind("messaging", Bindable.mapOf(String.class, Object.class)).orElse(Map.of()).keySet().stream()
                .filter(k -> !ROOTS.contains(k.replace("-", "").toLowerCase(java.util.Locale.ROOT)))
                .findFirst().ifPresent(k -> {
                    throw new IllegalStateException("messaging." + k + " é desconhecida; aceitas: messaging.providers, "
                            + "messaging.senders, messaging.receivers, messaging.receiver-defaults e "
                            + "messaging.receivers-enabled");
                });
        BindHandler strict = new NoUnboundElementsBindHandler(BindHandler.DEFAULT);
        return new MessagingProperties(
                bindProviders(binder, strict),
                binder.bind("messaging.senders", Bindable.mapOf(String.class, Sender.class), strict).orElse(null));
    }

    /** Só para listar os providers e ler o {@code type} de cada um; o resto é de {@link #bindProviders}. */
    record TypeOnly(Type type) {
    }

    /** Lê {@code type} de cada provider e liga o resto ao record daquele tipo, em modo estrito. */
    private static Map<String, Provider> bindProviders(Binder binder, BindHandler strict) {
        Map<String, Provider> providers = new LinkedHashMap<>();
        Map<String, TypeOnly> types = binder.bind("messaging.providers", Bindable.mapOf(String.class, TypeOnly.class))
                .orElse(Map.of());
        // os nomes vêm de um mapa sem tipo de valor: um provider sem type não aparece em types
        binder.bind("messaging.providers", Bindable.mapOf(String.class, Object.class)).orElse(Map.of()).keySet()
                .forEach(name -> {
                    TypeOnly typeOnly = types.get(name);
                    if (typeOnly == null || typeOnly.type() == null) {
                        throw new IllegalStateException("messaging.providers." + name + ".type é obrigatório (aws, azure, "
                                + "gcp, activemq-classic, artemis ou rabbitmq)");
                    }
                    // adapt, não of: o nome vem como o usuário o escreveu (Aws_Prod), que of() recusa; com '.', só entre colchetes
                    // ele é um elemento só
                    ConfigurationPropertyName node = name.contains(".")
                            ? ConfigurationPropertyName.of("messaging.providers[" + name + "]")
                            : ConfigurationPropertyName.adapt("messaging.providers." + name, '.');
                    providers.put(name, switch (typeOnly.type()) {
                        case AWS -> binder.bind(node, Bindable.of(AwsProvider.class), strict).get();
                        case AZURE -> binder.bind(node, Bindable.of(AzureProvider.class), strict).get();
                        case GCP -> binder.bind(node, Bindable.of(GcpProvider.class), strict).get();
                        case ACTIVEMQ_CLASSIC, ARTEMIS -> binder.bind(node, Bindable.of(JmsProvider.class), strict).get();
                        case RABBITMQ -> binder.bind(node, Bindable.of(RabbitProvider.class), strict).get();
                    });
                });
        return providers;
    }

    /** {@code activemq-classic}, {@code artemis} e {@code rabbitmq} no YAML. */
    public enum Type {
        AWS, AZURE, GCP, ACTIVEMQ_CLASSIC, ARTEMIS, RABBITMQ;

        /** The value of the {@code provider} tag of the meters and the {@code messaging.system} of the spans. */
        public String tag() {
            return name().toLowerCase().replace('_', '-');
        }
    }

    /**
     * Conexão com um provedor. Cada tipo tem o seu record, escolhido por {@code type}: o binding é estrito, então um
     * campo de outro tipo derruba o startup. {@code type} é o primeiro componente de cada record só para a chave
     * {@code type} do YAML ter onde ligar.
     */
    public sealed interface Provider permits AwsProvider, AzureProvider, GcpProvider, JmsProvider, RabbitProvider {

        Type type();
    }

    /**
     * @param region    região; sem ela, a cadeia padrão do SDK
     * @param endpoint  endpoint alternativo (LocalStack)
     * @param accessKey credencial estática; sem ela, a cadeia padrão do SDK
     * @param secretKey par de {@code accessKey}
     */
    public record AwsProvider(Type type, String region, URI endpoint, String accessKey, String secretKey)
            implements Provider {
    }

    /** @param connectionString connection string do namespace; sem ela, um bean {@code ServiceBusClientBuilder} */
    public record AzureProvider(Type type, String connectionString) implements Provider {
    }

    /**
     * @param project      projeto, usado para completar nomes curtos de tópico e subscription
     * @param emulatorHost {@code host:porta} do emulator (canal sem TLS e sem credenciais)
     */
    public record GcpProvider(Type type, String project, String emulatorHost) implements Provider {
    }

    /**
     * ActiveMQ Classic ({@code activemq-classic}) ou Artemis ({@code artemis}).
     *
     * @param brokerUrl URL do broker ({@code tcp://...}, {@code failover:(...)} no Classic); sem ela, o bean
     *                  {@code jakarta.jms.ConnectionFactory} da aplicação, sem pool por cima. No Artemis, ganha
     *                  {@code consumerWindowSize=0} quando não define a janela
     * @param user      usuário da conexão
     * @param password  senha de {@code user}
     */
    public record JmsProvider(Type type, String brokerUrl, String user, String password) implements Provider {
    }

    /**
     * @param brokerUrl URI AMQP ({@code amqp://host:5672/vhost}); sem ela, o bean
     *                  {@code com.rabbitmq.client.ConnectionFactory}
     * @param user      usuário da conexão
     * @param password  senha de {@code user}
     */
    public record RabbitProvider(Type type, String brokerUrl, String user, String password) implements Provider {
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
