package com.example.messaging.spring;

import com.example.messaging.Capabilities;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.spring.MessagingProperties.Destination;
import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Redelivery;
import com.example.messaging.spring.MessagingProperties.Requirement;
import com.example.messaging.spring.MessagingProperties.Type;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Senders e receivers dos destinos de {@code messaging.destinations}, por nome (ADR-0008). A
 * configuração inteira é validada na construção, e o startup falha listando todos os problemas.
 *
 * <ul>
 *   <li>{@link #sender(String)} devolve sempre a mesma instância (senders são thread-safe).</li>
 *   <li>{@link #receiver(String)} devolve uma instância nova a cada chamada, compartilhando o
 *       cliente nativo; no Service Bus com sessions, cada uma segura uma session. Quem chama fecha;
 *       as que sobrarem são fechadas no shutdown.</li>
 * </ul>
 */
public final class MessagingDestinations implements AutoCloseable {

    private static final Map<Type, String> ADAPTERS = Map.of(
            Type.AWS, "com.example.messaging.aws.SqsMessageSender",
            Type.AZURE, "com.example.messaging.azure.ServiceBusMessageSender",
            Type.GCP, "com.example.messaging.gcp.PubSubMessageSender",
            Type.ACTIVEMQ_CLASSIC, "com.example.messaging.jms.JmsMessageSender",
            Type.ARTEMIS, "com.example.messaging.jms.JmsMessageSender");
    private static final Map<Type, String> MODULES = Map.of(
            Type.AWS, "messaging-aws", Type.AZURE, "messaging-azure", Type.GCP, "messaging-gcp",
            Type.ACTIVEMQ_CLASSIC, "messaging-jms", Type.ARTEMIS, "messaging-jms");
    /** Cliente do broker que o messaging-jms usa em cada tipo: classe que prova a dependência e o artefato. */
    private static final Map<Type, String[]> BROKER_CLIENTS = Map.of(
            Type.ACTIVEMQ_CLASSIC, new String[]{"org.apache.activemq.ActiveMQConnectionFactory",
                    "org.apache.activemq:activemq-client"},
            Type.ARTEMIS, new String[]{"org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory",
                    "org.apache.activemq:artemis-jakarta-client"});
    private static final String VIRTUAL_TOPIC = "VirtualTopic.";

    private final MessagingProperties properties;
    private final MeterRegistry meters;
    private final ObservationRegistry observations;
    private final Map<String, ProviderFactory> factories = new HashMap<>();
    private final Map<String, MessageSender> senders = new HashMap<>();
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
            properties.destinations().forEach((name, destination) -> validateShape(name, destination, problems));
            if (problems.isEmpty()) {
                createFactories(beans);
                createSendersAndCheckRequirements(problems);
            }
        } catch (RuntimeException e) {
            close();
            throw e;
        }
        if (!problems.isEmpty()) {
            close();
            throw new IllegalStateException("Configuração inválida de messaging.*:\n - " + String.join("\n - ", problems));
        }
    }

    /** Nomes de {@code messaging.destinations}. */
    public Set<String> names() {
        return new TreeSet<>(properties.destinations().keySet());
    }

    /**
     * @throws IllegalArgumentException destino inexistente ou sem endereço de envio
     */
    public MessageSender sender(String name) {
        destination(name);
        MessageSender sender = senders.get(name);
        if (sender == null) {
            throw new IllegalArgumentException("Destino '" + name + "' não envia (sem tópico ou fila de envio)");
        }
        return sender;
    }

    /**
     * Receiver novo a cada chamada.
     *
     * @throws IllegalArgumentException destino inexistente ou sem endereço de recebimento
     */
    public MessageReceiver receiver(String name) {
        MessageReceiver receiver = newReceiver(name, destination(name));
        if (receiver == null) {
            throw new IllegalArgumentException("Destino '" + name + "' não recebe (sem fila ou subscription)");
        }
        created.add(receiver);
        return receiver;
    }

    /** Usado pelo health check: um receiver por destino, reaproveitado entre checagens. */
    synchronized void checkAccess(String name) {
        Destination destination = destination(name);
        MessageReceiver receiver = healthReceivers.get(name);
        if (receiver == null) {
            receiver = newReceiver(name, destination);
            if (receiver != null) {
                healthReceivers.put(name, receiver);
            }
        }
        if (receiver != null) {
            receiver.checkAccess();
        } else {
            senders.get(name).checkAccess();
        }
    }

    private Destination destination(String name) {
        Destination destination = properties.destinations().get(name);
        if (destination == null) {
            throw new IllegalArgumentException("Destino não configurado em messaging.destinations: " + name);
        }
        return destination;
    }

    private MessageReceiver newReceiver(String name, Destination destination) {
        MessageSender deadLetter = destination.deadLetter() == null ? null : senders.get(destination.deadLetter());
        MessageReceiver receiver = factories.get(destination.provider()).receiver(name, destination, deadLetter);
        if (receiver == null || meters == null) {
            return receiver;
        }
        return new MeteredMessageReceiver(receiver, providerType(destination), name, meters);
    }

    private void validateProviders(List<String> problems) {
        properties.providers().forEach((name, provider) -> {
            if (provider.type() == null) {
                problems.add("messaging.providers." + name
                        + ".type é obrigatório (aws, azure, gcp, activemq-classic ou artemis)");
            } else if (!ClassUtils.isPresent(ADAPTERS.get(provider.type()), getClass().getClassLoader())) {
                problems.add("messaging.providers." + name + ": adicione a dependência " + MODULES.get(provider.type()));
            } else if (BROKER_CLIENTS.containsKey(provider.type())
                    && !ClassUtils.isPresent(BROKER_CLIENTS.get(provider.type())[0], getClass().getClassLoader())) {
                problems.add("messaging.providers." + name + ": adicione a dependência "
                        + BROKER_CLIENTS.get(provider.type())[1]);
            }
        });
    }

    private void createFactories(ListableBeanFactory beans) {
        properties.providers().forEach((name, provider) -> {
            factories.put(name, switch (provider.type()) {
                case AWS -> new AwsProviderFactory(provider, beans);
                case AZURE -> new AzureProviderFactory(name, provider, beans);
                case GCP -> new GcpProviderFactory(name, provider);
                case ACTIVEMQ_CLASSIC, ARTEMIS -> new JmsProviderFactory(name, provider, beans);
            });
        });
    }

    private void validateShape(String name, Destination d, List<String> problems) {
        String prefix = "messaging.destinations." + name;
        Provider provider = d.provider() == null ? null : properties.providers().get(d.provider());
        if (provider == null) {
            problems.add(prefix + ".provider '" + d.provider() + "' não existe em messaging.providers");
            return;
        }
        if (provider.type() == null) {
            return;
        }
        Type type = provider.type();
        switch (type) {
            case AWS -> {
                if (d.queueUrl() == null && d.topicArn() == null) {
                    problems.add(prefix + ": defina queue-url ou topic-arn");
                }
                unexpected(prefix, problems, "queue", d.queue(), "topic", d.topic(), "subscription", d.subscription());
            }
            case AZURE -> {
                if (d.queue() == null && d.topic() == null) {
                    problems.add(prefix + ": defina queue ou topic");
                }
                if (d.subscription() != null && d.topic() == null) {
                    problems.add(prefix + ": subscription exige topic");
                }
                if (d.deadLetter() != null) {
                    problems.add(prefix + ".dead-letter: o Service Bus tem DLQ nativa; leia-a com dead-letter-queue: true");
                }
                if (d.deadLetterQueue() && d.topic() != null && d.subscription() == null) {
                    problems.add(prefix + ".dead-letter-queue em tópico exige subscription");
                }
                if (d.redelivery() == Redelivery.RESCHEDULE && (d.sessions() || d.queue() == null || d.topic() != null)) {
                    problems.add(prefix + ".redelivery=RESCHEDULE só vale para fila sem sessions");
                }
                unexpected(prefix, problems, "queue-url", d.queueUrl(), "topic-arn", d.topicArn());
            }
            case GCP -> {
                if (d.topic() == null && d.subscription() == null) {
                    problems.add(prefix + ": defina topic ou subscription");
                }
                if (d.subscription() != null && d.ackDeadline() == null) {
                    problems.add(prefix + ".ack-deadline é obrigatório para receber (o ack deadline da subscription)");
                }
                if (provider.project() == null && (isShort(d.topic()) || isShort(d.subscription()))) {
                    problems.add(prefix + ": nome curto exige messaging.providers." + d.provider() + ".project");
                }
                unexpected(prefix, problems, "queue-url", d.queueUrl(), "topic-arn", d.topicArn(), "queue", d.queue());
            }
            case ACTIVEMQ_CLASSIC, ARTEMIS -> {
                if (d.queue() == null && d.topic() == null) {
                    problems.add(prefix + ": defina queue ou topic");
                }
                if (d.subscription() != null && d.topic() == null) {
                    problems.add(prefix + ": subscription exige topic");
                }
                if (type == Type.ACTIVEMQ_CLASSIC && d.topic() != null && !d.topic().startsWith(VIRTUAL_TOPIC)) {
                    problems.add(prefix + ".topic precisa ser um Virtual Topic (" + VIRTUAL_TOPIC + "<nome>)");
                }
                if (d.lease() != null && (d.lease().isNegative() || d.lease().isZero())) {
                    problems.add(prefix + ".lease precisa ser positivo");
                }
                unexpected(prefix, problems, "queue-url", d.queueUrl(), "topic-arn", d.topicArn());
            }
        }
        if (d.lease() != null && type != Type.ACTIVEMQ_CLASSIC && type != Type.ARTEMIS) {
            problems.add(prefix + ".lease só vale para o ActiveMQ; nos outros, o lease é do destino");
        }
        if (d.sessions() && type != Type.AZURE) {
            problems.add(prefix + ".sessions só vale para o Service Bus");
        }
        if (d.deadLetterQueue() && type != Type.AZURE) {
            problems.add(prefix + ".dead-letter-queue só vale para o Service Bus; nos outros, a DLQ é um destino comum");
        }
        if (d.deadLetter() != null && type != Type.AZURE) {
            Destination target = properties.destinations().get(d.deadLetter());
            if (target == null) {
                problems.add(prefix + ".dead-letter '" + d.deadLetter() + "' não existe em messaging.destinations");
            }
        }
    }

    private void createSendersAndCheckRequirements(List<String> problems) {
        properties.destinations().forEach((name, destination) -> {
            MessageSender sender = factories.get(destination.provider()).sender(name, destination);
            if (sender != null) {
                senders.put(name, meters != null || observations != null
                        ? new ObservedMessageSender(sender, providerType(destination), name, meters, observations)
                        : sender);
            }
        });
        properties.destinations().forEach((name, destination) -> {
            if (destination.deadLetter() != null && !destination.deadLetter().equals(name)
                    && properties.destinations().containsKey(destination.deadLetter())
                    && !senders.containsKey(destination.deadLetter())) {
                problems.add("messaging.destinations." + name + ".dead-letter '" + destination.deadLetter()
                        + "' não tem endereço de envio");
            }
        });
        if (!problems.isEmpty()) {
            return;
        }
        properties.destinations().forEach((name, destination) -> {
            if (destination.require().isEmpty()) {
                return;
            }
            MessageSender sender = senders.get(name);
            try (MessageReceiver receiver = newReceiver(name, destination)) {
                for (Requirement requirement : destination.require()) {
                    String missing = missing(requirement, sender == null ? null : sender.capabilities(),
                            receiver == null ? null : receiver.capabilities());
                    if (missing != null) {
                        problems.add("messaging.destinations." + name + ".require " + requirement + ": " + missing);
                    }
                }
            }
        });
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

    private String providerType(Destination destination) {
        return properties.providers().get(destination.provider()).type().name().toLowerCase().replace('_', '-');
    }

    private static boolean isShort(String name) {
        return name != null && !name.startsWith("projects/");
    }

    private static void unexpected(String prefix, List<String> problems, String... namesAndValues) {
        for (int i = 0; i < namesAndValues.length; i += 2) {
            if (namesAndValues[i + 1] != null) {
                problems.add(prefix + "." + namesAndValues[i] + " não se aplica a este provedor");
            }
        }
    }

    /** Fecha receivers que sobraram, senders e os clientes criados pelo starter. */
    @Override
    public void close() {
        created.forEach(MessageReceiver::close);
        healthReceivers.values().forEach(MessageReceiver::close);
        senders.values().forEach(MessageSender::close);
        factories.values().forEach(ProviderFactory::close);
    }
}
