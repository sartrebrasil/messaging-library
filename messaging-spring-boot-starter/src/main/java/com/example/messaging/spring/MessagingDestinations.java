package com.example.messaging.spring;

import com.example.messaging.Capabilities;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.spring.MessagingProperties.Destination;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Requirement;
import com.example.messaging.spring.MessagingProperties.Type;
import com.example.messaging.spring.aws.AwsProviderConnection;
import com.example.messaging.spring.azure.AzureProviderConnection;
import com.example.messaging.spring.gcp.GcpProviderConnection;
import com.example.messaging.spring.jms.JmsProviderConnection;
import com.example.messaging.spring.rabbitmq.RabbitProviderConnection;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Senders de {@code messaging.senders}, por nome, e senders e receivers por endereço ({@link Endpoint}), que a inbox
 * usa para {@code messaging.receivers} (ADR-0008). Providers e senders configurados são validados na construção, e
 * o startup falha listando todos os problemas; um endereço é validado no primeiro uso.
 *
 * <ul>
 *   <li>{@link #sender(String)} e {@link #sender(String, String, Endpoint)} devolvem sempre a mesma instância por
 *       nome (senders são thread-safe).</li>
 *   <li>{@link #receiver(String, String, Endpoint, MessageSender)} devolve uma instância nova a cada chamada,
 *       compartilhando o cliente nativo; no Service Bus com sessions, cada uma segura uma session. Quem chama fecha;
 *       as que sobrarem são fechadas no shutdown.</li>
 * </ul>
 *
 * O nome de um sender ou receiver é a tag {@code destination} das métricas e a chave do health check. Os criados
 * por endereço usam {@code messaging.receivers.<nome>} nas mensagens de erro.
 */
public final class MessagingDestinations implements AutoCloseable {

    /** Classe de cada adapter: o starter só cria a conexão de um tipo cujo adapter está no classpath. */
    private static final Map<Type, String> ADAPTERS = Map.of(
            Type.AWS, "com.example.messaging.aws.SqsMessageSender",
            Type.AZURE, "com.example.messaging.azure.ServiceBusMessageSender",
            Type.GCP, "com.example.messaging.gcp.PubSubMessageSender",
            Type.ACTIVEMQ_CLASSIC, "com.example.messaging.jms.JmsMessageSender",
            Type.ARTEMIS, "com.example.messaging.jms.JmsMessageSender",
            Type.RABBITMQ, "com.example.messaging.rabbitmq.RabbitMessageSender");
    private static final Map<Type, String> MODULES = Map.of(
            Type.AWS, "messaging-aws", Type.AZURE, "messaging-azure", Type.GCP, "messaging-gcp",
            Type.ACTIVEMQ_CLASSIC, "messaging-jms", Type.ARTEMIS, "messaging-jms", Type.RABBITMQ, "messaging-rabbitmq");
    /** Cliente do broker que o messaging-jms usa em cada tipo: classe que prova a dependência e o artefato. */
    private static final Map<Type, String[]> BROKER_CLIENTS = Map.of(
            Type.ACTIVEMQ_CLASSIC, new String[]{"org.apache.activemq.ActiveMQConnectionFactory",
                    "org.apache.activemq:activemq-client"},
            Type.ARTEMIS, new String[]{"org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory",
                    "org.apache.activemq:artemis-jakarta-client"});
    private static final String RECEIVERS = "messaging.receivers.";

    /** Um endereço resolvido: o provider e os campos dele. */
    private record Resolved(String provider, Destination destination) {
    }

    private final MessagingProperties properties;
    private final MeterRegistry meters;
    private final ObservationRegistry observations;
    private final Map<String, ProviderConnection> connections = new HashMap<>();
    private final Map<String, MessageSender> senders = new HashMap<>();
    private final Map<String, MessageSender> endpointSenders = new ConcurrentHashMap<>();
    /** Receivers já criados por endereço: o que o health check usa. */
    private final Map<String, Resolved> receivers = new ConcurrentHashMap<>();
    private final Map<String, MessageReceiver> healthReceivers = new HashMap<>();
    private final List<MessageReceiver> created = new CopyOnWriteArrayList<>();

    MessagingDestinations(MessagingProperties properties, ListableBeanFactory beans, MeterRegistry meters,
                          ObservationRegistry observations) {
        this.properties = properties;
        this.meters = meters;
        this.observations = observations;
        List<String> problems = new ArrayList<>();
        try {
            // Validação de forma antes de criar qualquer cliente: erro de configuração não abre conexão
            validateProviders(problems);
            Map<String, Resolved> configured = new LinkedHashMap<>();
            properties.senders().forEach((name, sender) -> {
                Resolved r = resolve("messaging.senders." + name, sender.provider(), sender.endpoint(), false,
                        problems);
                if (r != null) {
                    configured.put(name, r);
                }
            });
            if (problems.isEmpty()) {
                connect(beans);
                configured.forEach((name, r) -> senders.put(name, newSender(name, r)));
                configured.forEach((name, r) -> checkSenderRequirements("messaging.senders." + name, r,
                        senders.get(name), problems));
            }
        } catch (RuntimeException e) {
            close();
            throw e;
        }
        if (!problems.isEmpty()) {
            close();
            throw invalid(problems);
        }
    }

    /** Nomes no health check: senders de {@code messaging.senders}, os criados por endereço e os receivers. */
    public Set<String> names() {
        Set<String> names = new TreeSet<>(senders.keySet());
        names.addAll(endpointSenders.keySet());
        names.addAll(receivers.keySet());
        return names;
    }

    /**
     * @throws IllegalArgumentException sender não configurado em {@code messaging.senders}
     */
    public MessageSender sender(String name) {
        MessageSender sender = senders.get(name);
        if (sender == null) {
            throw new IllegalArgumentException("Sender não configurado em messaging.senders: " + name);
        }
        return sender;
    }

    /**
     * Problemas do endereço, sem abrir conexão.
     *
     * @param key      chave completa, para as mensagens
     * @param provider {@code null}: o único provider
     */
    public List<String> validate(String key, String provider, Endpoint endpoint, boolean receive) {
        List<String> problems = new ArrayList<>();
        resolve(key, provider, endpoint, receive, problems);
        return problems;
    }

    /**
     * Receiver novo por endereço. Na primeira chamada de um nome, confere {@code options.require}.
     *
     * @param provider   {@code null}: o único provider
     * @param deadLetter sender do dead-letter, para os adapters sem DLQ nativa; {@code null} sem DLQ
     * @throws IllegalStateException endereço inválido ou capacidade exigida ausente
     */
    public MessageReceiver receiver(String name, String provider, Endpoint endpoint, MessageSender deadLetter) {
        Resolved r = resolveOrThrow(RECEIVERS + name, provider, endpoint, true);
        MessageReceiver receiver = newReceiver(name, r, deadLetter);
        if (!receivers.containsKey(name)) {
            List<String> problems = new ArrayList<>();
            for (Requirement requirement : r.destination().require()) {
                String missing = missing(requirement, null, receiver.capabilities());
                if (missing != null) {
                    problems.add(RECEIVERS + name + ".options.require " + requirement + ": " + missing);
                }
            }
            if (!problems.isEmpty()) {
                receiver.close();
                throw invalid(problems);
            }
            receivers.put(name, r);
        }
        created.add(receiver);
        return receiver;
    }

    /**
     * Sender por endereço, criado uma vez por nome (o endereço da primeira chamada vale).
     *
     * @param provider {@code null}: o único provider
     * @throws IllegalStateException endereço inválido
     */
    public MessageSender sender(String name, String provider, Endpoint endpoint) {
        return endpointSenders.computeIfAbsent(name,
                n -> newSender(n, resolveOrThrow(RECEIVERS + n, provider, endpoint, false)));
    }

    /** Usado pelo health check: um receiver por nome, reaproveitado entre checagens; senão o sender. */
    synchronized void checkAccess(String name) {
        Resolved r = receivers.get(name);
        if (r != null) {
            healthReceivers.computeIfAbsent(name, n -> newReceiver(n, r, null)).checkAccess();
            return;
        }
        MessageSender sender = senders.containsKey(name) ? senders.get(name) : endpointSenders.get(name);
        if (sender == null) {
            throw new IllegalArgumentException("Nome desconhecido no health check de messaging: " + name);
        }
        sender.checkAccess();
    }

    private Resolved resolve(String key, String provider, Endpoint endpoint, boolean receive, List<String> problems) {
        String name = Endpoints.provider(key, provider, null, properties.providers(), problems);
        if (name == null) {
            return null;
        }
        Provider p = properties.providers().get(name);
        if (p.type() == null) {
            return null; // validateProviders já reportou
        }
        Destination d = Endpoints.resolve(key, name, p, endpoint, receive, problems);
        return d == null ? null : new Resolved(name, d);
    }

    private Resolved resolveOrThrow(String key, String provider, Endpoint endpoint, boolean receive) {
        List<String> problems = new ArrayList<>();
        Resolved r = resolve(key, provider, endpoint, receive, problems);
        if (!problems.isEmpty() || r == null) {
            throw invalid(problems);
        }
        if (!connections.containsKey(r.provider())) {
            throw invalid(List.of(key + ".provider '" + r.provider() + "' não tem conexão (veja messaging.providers)"));
        }
        return r;
    }

    private static IllegalStateException invalid(List<String> problems) {
        return new IllegalStateException("Configuração inválida de messaging.*:\n - " + String.join("\n - ", problems));
    }

    private MessageSender newSender(String name, Resolved r) {
        MessageSender sender = connections.get(r.provider()).sender(name, r.destination());
        return meters != null || observations != null
                ? new ObservedMessageSender(sender, providerType(r), name, meters, observations)
                : sender;
    }

    private MessageReceiver newReceiver(String name, Resolved r, MessageSender deadLetter) {
        MessageReceiver receiver = connections.get(r.provider()).receiver(name, r.destination(), deadLetter);
        if (meters == null) {
            return receiver;
        }
        return new MeteredMessageReceiver(receiver, providerType(r), name, meters);
    }

    private void validateProviders(List<String> problems) {
        properties.providers().forEach((name, provider) -> {
            if (provider.type() == null) {
                problems.add("messaging.providers." + name
                        + ".type é obrigatório (aws, azure, gcp, activemq-classic, artemis ou rabbitmq)");
            } else if (!ClassUtils.isPresent(ADAPTERS.get(provider.type()), getClass().getClassLoader())) {
                problems.add("messaging.providers." + name + ": adicione a dependência " + MODULES.get(provider.type()));
            } else if (BROKER_CLIENTS.containsKey(provider.type())
                    && !ClassUtils.isPresent(BROKER_CLIENTS.get(provider.type())[0], getClass().getClassLoader())) {
                problems.add("messaging.providers." + name + ": adicione a dependência "
                        + BROKER_CLIENTS.get(provider.type())[1]);
            }
        });
    }

    private void connect(ListableBeanFactory beans) {
        // as classes de cada provedor só são carregadas aqui, depois da checagem do adapter em validateProviders
        properties.providers().forEach((name, provider) -> connections.put(name, switch (provider.type()) {
            case AWS -> new AwsProviderConnection(provider, beans);
            case AZURE -> new AzureProviderConnection(name, provider, beans);
            case GCP -> new GcpProviderConnection(name, provider);
            case ACTIVEMQ_CLASSIC, ARTEMIS -> new JmsProviderConnection(name, provider, beans);
            case RABBITMQ -> new RabbitProviderConnection(name, provider, beans);
        }));
    }

    /** Um sender não tem como provar capacidade do lado de quem recebe: essas exigências são de receiver. */
    private static void checkSenderRequirements(String key, Resolved r, MessageSender sender, List<String> problems) {
        for (Requirement requirement : r.destination().require()) {
            String missing = missing(requirement, sender.capabilities(), null);
            if (missing != null) {
                problems.add(key + ".options.require " + requirement + ": " + missing);
            }
        }
    }

    /** {@code null} quando o lado que importa atende; senão o motivo. */
    private static String missing(Requirement requirement, Capabilities sender, Capabilities receiver) {
        return switch (requirement) {
            case ORDERED_DELIVERY -> both(sender, receiver, Capabilities::orderedDelivery);
            case PUBLISHER_DEDUPLICATION -> both(sender, receiver, Capabilities::publisherDeduplication);
            case DELAYED_REDELIVERY -> receiverOnly(receiver, Capabilities::delayedRedelivery);
            case NATIVE_DEAD_LETTER -> receiverOnly(receiver, Capabilities::nativeDeadLetter);
            case LEASE_EXPIRED_ON_ACK -> receiverOnly(receiver, Capabilities::reportsLeaseExpiredOnAck);
        };
    }

    private static String both(Capabilities sender, Capabilities receiver, Predicate<Capabilities> capability) {
        if (sender != null && !capability.test(sender)) {
            return "o sender não suporta";
        }
        if (receiver != null && !capability.test(receiver)) {
            return "o receiver não suporta";
        }
        return null;
    }

    private static String receiverOnly(Capabilities receiver, Predicate<Capabilities> capability) {
        if (receiver == null) {
            return "exige endereço de recebimento";
        }
        return capability.test(receiver) ? null : "o receiver não suporta";
    }

    private String providerType(Resolved r) {
        return properties.providers().get(r.provider()).type().name().toLowerCase().replace('_', '-');
    }

    /** Fecha receivers que sobraram, senders e os clientes criados pelo starter. */
    @Override
    public void close() {
        created.forEach(MessageReceiver::close);
        healthReceivers.values().forEach(MessageReceiver::close);
        senders.values().forEach(MessageSender::close);
        endpointSenders.values().forEach(MessageSender::close);
        connections.values().forEach(ProviderConnection::close);
    }
}
