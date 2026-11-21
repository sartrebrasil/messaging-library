package com.example.messaging.spring;

import com.example.messaging.spring.MessagingProperties.Destination;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.spring.MessagingProperties.GcpProvider;
import com.example.messaging.spring.MessagingProperties.Kind;
import com.example.messaging.spring.MessagingProperties.Options;
import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Redelivery;
import com.example.messaging.spring.MessagingProperties.Type;

import java.lang.reflect.RecordComponent;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Traduz um {@link Endpoint} ({@code destination}, {@code subscription}, {@code kind}, {@code options}) nos campos
 * de cada provedor e valida a forma, sem abrir conexão (ADR-0008). Os problemas vão para a lista, com a chave
 * completa ({@code messaging.receivers.<n>} ou {@code messaging.senders.<n>}).
 */
public final class Endpoints {

    private static final String VIRTUAL_TOPIC = "VirtualTopic.";
    private static final String SNS_ARN = "arn:aws:sns:";
    private static final java.util.regex.Pattern SNS_ARN_ANY_PARTITION = java.util.regex.Pattern.compile("^arn:aws[a-z-]*:sns:.*");

    /**
     * Opção ({@code options.<nome>}, em kebab-case) → provedores que a aceitam. Opção definida fora da lista derruba o
     * startup em vez de ser ignorada; todo componente de {@link Options} precisa estar aqui.
     */
    static final Map<String, Set<Type>> ACCEPTED = Map.ofEntries(
            Map.entry("require", EnumSet.allOf(Type.class)),
            Map.entry("max-message-bytes", EnumSet.of(Type.AWS, Type.AZURE, Type.ACTIVEMQ_CLASSIC, Type.ARTEMIS,
                    Type.RABBITMQ)),
            Map.entry("sessions", EnumSet.of(Type.AZURE)),
            Map.entry("dead-letter-queue", EnumSet.of(Type.AZURE)),
            Map.entry("redelivery", EnumSet.of(Type.AZURE, Type.ACTIVEMQ_CLASSIC, Type.ARTEMIS, Type.RABBITMQ)),
            Map.entry("ack-deadline", EnumSet.of(Type.GCP)),
            Map.entry("ordered", EnumSet.of(Type.GCP)),
            Map.entry("exactly-once", EnumSet.of(Type.GCP)),
            Map.entry("lease", EnumSet.of(Type.ACTIVEMQ_CLASSIC, Type.ARTEMIS, Type.RABBITMQ)),
            Map.entry("routing-key", EnumSet.of(Type.RABBITMQ)),
            Map.entry("routing-by-ordering-key", EnumSet.of(Type.RABBITMQ)),
            Map.entry("confirm-timeout", EnumSet.of(Type.RABBITMQ)));

    private Endpoints() {
    }

    /**
     * {@code explicit}, senão {@code fallback} (de {@code receiver-defaults}), senão o único provider.
     *
     * @return {@code null} quando não há como decidir (problema registrado)
     */
    public static String provider(String key, String explicit, String fallback, Map<String, Provider> providers,
                                  List<String> problems) {
        String name = explicit != null ? explicit : fallback;
        if (name == null) {
            if (providers.size() == 1) {
                return providers.keySet().iterator().next();
            }
            problems.add(providers.isEmpty() ? key + ".provider: nenhum provider em messaging.providers"
                    : key + ".provider é obrigatório com mais de um provider");
            return null;
        }
        if (!providers.containsKey(name)) {
            problems.add(key + ".provider '" + name + "' não existe em messaging.providers");
            return null;
        }
        return name;
    }

    /**
     * @param providerName nome em {@code messaging.providers}, para as mensagens
     * @param receive      endereço de recebimento; senão de envio
     * @return {@code null} quando falta {@code destination} (problema registrado)
     */
    public static Destination resolve(String key, String providerName, Provider provider, Endpoint e, boolean receive,
                                      List<String> problems) {
        if (e.destination() == null || e.destination().isBlank()) {
            problems.add(key + ".destination é obrigatório");
            return null;
        }
        Type type = provider.type();
        String dest = e.destination();
        String queueUrl = null;
        String topicArn = null;
        String queue = null;
        String topic = null;
        String subscription = null;
        if (receive && e.kind() != null) {
            problems.add(key + ".kind só vale para enviar");
        }
        if (!receive && e.subscription() != null) {
            problems.add(key + ".subscription só vale para receber");
        }
        switch (type) {
            case AWS -> {
                notApplicable(key, problems, "subscription", e.subscription());
                if (!receive && SNS_ARN_ANY_PARTITION.matcher(dest).matches()) {
                    topicArn = dest;
                } else if (!receive && e.kind() == Kind.TOPIC) {
                    problems.add(key + ".kind: topic exige um ARN do SNS (" + SNS_ARN + "...)");
                } else if (dest.startsWith("https://") || dest.startsWith("http://")) {
                    queueUrl = dest;
                } else {
                    queue = dest;
                }
            }
            case GCP -> {
                notApplicable(key, problems, "subscription", e.subscription());
                if (e.kind() == Kind.QUEUE) {
                    problems.add(key + ".kind não se aplica a este provedor (o GCP só envia a tópicos)");
                }
                if (receive) {
                    subscription = dest;
                } else {
                    topic = dest;
                }
                if (((GcpProvider) provider).project() == null && !dest.startsWith("projects/")) {
                    problems.add(key + ".destination: nome curto exige messaging.providers." + providerName
                            + ".project");
                }
            }
            case AZURE, ACTIVEMQ_CLASSIC, ARTEMIS, RABBITMQ -> {
                if (receive ? e.subscription() != null : e.kind() == Kind.TOPIC) {
                    topic = dest;
                    subscription = receive ? e.subscription() : null;
                } else {
                    queue = dest;
                }
                if (type == Type.ACTIVEMQ_CLASSIC && topic != null && !topic.startsWith(VIRTUAL_TOPIC)) {
                    problems.add(key + ".destination precisa ser um Virtual Topic (" + VIRTUAL_TOPIC + "<nome>)");
                }
            }
        }
        Options o = e.options();
        rejectNotAccepted(key + ".options", type, o, problems);
        Destination d = new Destination(queueUrl, topicArn, queue, topic, subscription, o.require(),
                is(o.sessions()), o.redelivery(), o.maxMessageBytes(), o.ackDeadline(), is(o.ordered()),
                is(o.exactlyOnce()), is(o.deadLetterQueue()), o.lease(), o.routingKey(),
                is(o.routingByOrderingKey()), o.confirmTimeout());
        validateOptions(key + ".options", type, d, receive, problems);
        return d;
    }

    /** Só conta como definida a opção com valor diferente do padrão ({@code null}, {@code false}, vazio, {@code ABANDON}). */
    private static void rejectNotAccepted(String prefix, Type type, Options options, List<String> problems) {
        for (RecordComponent component : Options.class.getRecordComponents()) {
            String name = component.getName().replaceAll("([A-Z])", "-$1").toLowerCase(Locale.ROOT);
            Object value;
            try {
                value = component.getAccessor().invoke(options);
            } catch (ReflectiveOperationException ex) {
                throw new IllegalStateException(ex);
            }
            boolean set = value != null && !Boolean.FALSE.equals(value) && value != Redelivery.ABANDON
                    && !(value instanceof Set<?> s && s.isEmpty());
            Set<Type> accepted = ACCEPTED.get(name);
            if (set && !accepted.contains(type)) {
                problems.add(prefix + "." + name + " não se aplica ao provedor " + type.tag() + "; vale para: "
                        + accepted.stream().map(Type::tag).sorted().collect(Collectors.joining(", ")));
            }
        }
    }

    private static void validateOptions(String prefix, Type type, Destination d, boolean receive,
                                        List<String> problems) {
        switch (type) {
            case AZURE -> {
                if (d.redelivery() == Redelivery.RESCHEDULE && (d.sessions() || d.queue() == null)) {
                    problems.add(prefix + ".redelivery=RESCHEDULE só vale para fila sem sessions");
                }
                if (d.deadLetterQueue() && !receive) {
                    problems.add(prefix + ".dead-letter-queue só vale para receber");
                }
            }
            case GCP -> {
                if (receive && d.ackDeadline() == null) {
                    problems.add(prefix + ".ack-deadline é obrigatório para receber (o ack deadline da subscription)");
                }
            }
            case RABBITMQ -> {
                if (d.redelivery() == Redelivery.RESCHEDULE) {
                    problems.add(prefix + ".redelivery=RESCHEDULE: o RabbitMQ não tem atraso no nack");
                }
            }
            default -> {
            }
        }
        if (d.lease() != null && (d.lease().isNegative() || d.lease().isZero())) {
            problems.add(prefix + ".lease precisa ser positivo");
        }
        if (d.routingKey() != null && d.topic() == null) {
            problems.add(prefix + ".routing-key só vale para o RabbitMQ com kind: topic (exchange)");
        }
        if (d.routingByOrderingKey() && d.topic() == null) {
            problems.add(prefix + ".routing-by-ordering-key só vale para o RabbitMQ com kind: topic (exchange)");
        }
        if (d.confirmTimeout() != null && (d.confirmTimeout().isNegative() || d.confirmTimeout().isZero())) {
            problems.add(prefix + ".confirm-timeout precisa ser positivo");
        }
        if (d.routingByOrderingKey() && d.routingKey() != null) {
            problems.add(prefix + ".routing-by-ordering-key não combina com routing-key");
        }
    }

    private static void notApplicable(String key, List<String> problems, String field, String value) {
        if (value != null) {
            problems.add(key + "." + field + " não se aplica a este provedor");
        }
    }

    private static boolean is(Boolean value) {
        return Boolean.TRUE.equals(value);
    }
}
