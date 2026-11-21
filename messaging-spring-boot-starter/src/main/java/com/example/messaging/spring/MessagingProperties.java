package com.example.messaging.spring;

import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

    /** Chaves de conexão que o ADR-0011 tirou do YAML, sem hífen e em minúsculas, com o nome como se escreve. */
    private static final Map<String, String> REMOVED = Map.of("region", "region", "endpoint", "endpoint",
            "accesskey", "access-key", "secretkey", "secret-key", "connectionstring", "connection-string",
            "emulatorhost", "emulator-host", "brokerurl", "broker-url", "user", "user", "password", "password");

    /** O que a aplicação declara no lugar das chaves removidas de cada tipo. */
    private static String replacement(Type type) {
        return switch (type) {
            case AWS -> "o bean SqsClient (e o SnsClient, se enviar ao SNS)";
            case AZURE -> "o bean ServiceBusClientBuilder";
            case GCP -> "os beans TransportChannelProvider e CredentialsProvider";
            case ACTIVEMQ_CLASSIC, ARTEMIS -> "o bean jakarta.jms.ConnectionFactory";
            case RABBITMQ -> "o bean com.rabbitmq.client.ConnectionFactory";
        };
    }

    /** Só para listar os providers e ler o {@code type} de cada um; o resto é de {@link #bindProviders}. */
    record TypeOnly(Type type) {
    }

    /** Lê {@code type} de cada provider e liga o resto ao record daquele tipo, em modo estrito. */
    private static Map<String, Provider> bindProviders(Binder binder, BindHandler strict) {
        Map<String, Provider> providers = new LinkedHashMap<>();
        List<String> problems = new ArrayList<>();
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
                    // a chave removida seria "desconhecida" no binding estrito: aqui a mensagem diz o que fazer
                    int before = problems.size();
                    new TreeSet<>(binder.bind(node, Bindable.mapOf(String.class, Object.class)).orElse(Map.of()).keySet())
                            .forEach(key -> {
                                String known = REMOVED.get(key.replace("-", "").toLowerCase(java.util.Locale.ROOT));
                                if (known != null) {
                                    problems.add("messaging.providers." + name + "." + known + " foi removida (ADR-0011): "
                                            + "configure " + replacement(typeOnly.type()) + " na aplicação");
                                }
                            });
                    if (problems.size() > before) {
                        return;
                    }
                    try {
                        providers.put(name, switch (typeOnly.type()) {
                            case AWS -> binder.bind(node, Bindable.of(AwsProvider.class), strict).get();
                            case AZURE -> binder.bind(node, Bindable.of(AzureProvider.class), strict).get();
                            case GCP -> binder.bind(node, Bindable.of(GcpProvider.class), strict).get();
                            case ACTIVEMQ_CLASSIC, ARTEMIS ->
                                    binder.bind(node, Bindable.of(JmsProvider.class), strict).get();
                            case RABBITMQ -> binder.bind(node, Bindable.of(RabbitProvider.class), strict).get();
                        });
                    } catch (RuntimeException e) {
                        // junto dos outros problemas: o usuário corrige tudo de uma vez
                        Throwable root = e;
                        while (root.getCause() != null) {
                            root = root.getCause();
                        }
                        problems.add(root.getMessage());
                    }
                });
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Configuração inválida de messaging.providers:\n - "
                    + String.join("\n - ", problems));
        }
        return providers;
    }

    /** {@code activemq-classic}, {@code artemis} e {@code rabbitmq} no YAML. */
    public enum Type {
        AWS, AZURE, GCP, ACTIVEMQ_CLASSIC, ARTEMIS, RABBITMQ;

        /** The value of the {@code provider} tag of the meters and the {@code messaging.system} of the spans. */
        public String tag() {
            return name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
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

    /** AWS: os clientes {@code SqsClient} e {@code SnsClient} são beans da aplicação (ADR-0011). */
    public record AwsProvider(Type type) implements Provider {
    }

    /** Azure: o {@code ServiceBusClientBuilder} é um bean da aplicação (ADR-0011). */
    public record AzureProvider(Type type) implements Provider {
    }

    /**
     * GCP: {@code TransportChannelProvider} e {@code CredentialsProvider} são beans opcionais da aplicação (ADR-0011).
     *
     * @param project projeto, usado para completar nomes curtos de tópico e subscription
     */
    public record GcpProvider(Type type, String project) implements Provider {
    }

    /**
     * ActiveMQ Classic ({@code activemq-classic}) ou Artemis ({@code artemis}): o {@code jakarta.jms.ConnectionFactory}
     * é um bean da aplicação (ADR-0011). No Artemis, a factory precisa de {@code consumerWindowSize=0}.
     */
    public record JmsProvider(Type type) implements Provider {
    }

    /** RabbitMQ: o {@code com.rabbitmq.client.ConnectionFactory} é um bean da aplicação (ADR-0011). */
    public record RabbitProvider(Type type) implements Provider {
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
