package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.EnvelopeMode;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.inbox.PayloadDeserializationException;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.ThrowableTypes;
import com.example.messaging.inbox.spring.InboxProperties.Backoff;
import com.example.messaging.inbox.spring.InboxProperties.ConsumerOptions;
import org.springframework.boot.convert.DurationStyle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Merges {@code inbox.consumers.<name>} over {@code inbox.defaults} over the library defaults, property by property
 * (ADR-0008), and validates everything that does not need a broker. All problems are reported at once.
 */
final class InboxConfig {

    static final String MESSAGING = "messaging";
    static final Set<String> CONFIGURATION_KEYS = Set.of("max-messages-per-poll", "poll-wait", "max-processing",
            "shutdown-timeout");

    private static final Backoff IMMEDIATE = backoff(3, Duration.ofMillis(100), 2.0, Duration.ofSeconds(2), 0.2);
    private static final Backoff DELAYED = backoff(5, Duration.ofSeconds(30), 2.0, Duration.ofMinutes(15), 0.2);
    private static final List<String> NON_RETRYABLE = List.of(PayloadDeserializationException.class.getName(),
            IllegalArgumentException.class.getName());

    /** A queue band before its destination is looked up. */
    record QueueBand(Duration delay, String destination) {
    }

    /**
     * One consumer after the merge.
     *
     * @param deadLetter      {@code null} without a dead-letter destination
     * @param delayedMaxSet   {@code retry.delayed.max} was set by the user; when not, the starter caps it to the
     *                        receiver's limit
     * @param reservationTtl  {@code null}: derived from the loop settings
     */
    record Consumer(String name, String destination, String deadLetter, String failed, int concurrency,
                    Ordering ordering, EnvelopeMode envelope, boolean includeExceptionMessage,
                    BackoffSpec immediate, BackoffSpec delayed, boolean delayedMaxSet, DelayedStrategy strategy,
                    List<QueueBand> queues, List<Class<? extends Throwable>> nonRetryable,
                    List<Class<? extends Throwable>> skipImmediate, List<Class<? extends Throwable>> retryable,
                    boolean idempotency, Duration completedTtl, Duration reservationTtl,
                    int maxMessagesPerPoll, Duration pollWait, Duration maxProcessing, Duration shutdownTimeout) {
    }

    private InboxConfig() {
    }

    /** @throws IllegalStateException listing every problem */
    static List<Consumer> resolve(InboxProperties properties, ClassLoader loader) {
        List<String> problems = new ArrayList<>();
        properties.getBrokers().forEach((name, broker) -> {
            if (!MESSAGING.equals(broker.getType())) {
                problems.add("inbox.brokers." + name + ".type must be 'messaging' (Kafka is not part of this library)");
            }
            unknownConfiguration("inbox.brokers." + name + ".configuration", broker.getConfiguration(), problems);
        });
        ConsumerOptions defaults = properties.getDefaults() == null ? new ConsumerOptions() : properties.getDefaults();
        forbiddenInDefaults(defaults, problems);
        unsupported("inbox.defaults", defaults, problems);
        List<Consumer> consumers = new ArrayList<>();
        properties.getConsumers().forEach((name, options) -> {
            Consumer consumer = consumer(name, options, defaults, properties, loader, problems);
            if (consumer != null) {
                consumers.add(consumer);
            }
        });
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Invalid inbox.* configuration:\n - " + String.join("\n - ", problems));
        }
        return consumers;
    }

    private static Consumer consumer(String name, ConsumerOptions c, ConsumerOptions d, InboxProperties properties,
                                     ClassLoader loader, List<String> problems) {
        String prefix = "inbox.consumers." + name;
        int before = problems.size();
        unsupported(prefix, c, problems);
        String broker = first(c.getBroker(), d.getBroker());
        Map<String, String> brokerConfiguration = Map.of();
        if (properties.getBrokers().isEmpty()) {
            if (broker != null && !MESSAGING.equals(broker)) {
                problems.add(prefix + ".broker '" + broker + "' does not exist (the implicit broker is 'messaging')");
            }
        } else if (broker == null) {
            if (properties.getBrokers().size() == 1) {
                brokerConfiguration = properties.getBrokers().values().iterator().next().getConfiguration();
            } else {
                problems.add(prefix + ".broker is required with more than one broker in inbox.brokers");
            }
        } else if (!properties.getBrokers().containsKey(broker)) {
            problems.add(prefix + ".broker '" + broker + "' does not exist in inbox.brokers");
        } else {
            brokerConfiguration = properties.getBrokers().get(broker).getConfiguration();
        }
        if (c.getDestination() == null) {
            problems.add(prefix + ".destination is required");
        }
        if (c.getFailed() == null) {
            problems.add(prefix + ".failed is required (ADR-0002)");
        }
        unknownConfiguration(prefix + ".configuration", c.getConfiguration(), problems);
        Map<String, String> configuration = new LinkedHashMap<>();
        if (brokerConfiguration != null) {
            configuration.putAll(brokerConfiguration);
        }
        if (c.getConfiguration() != null) {
            configuration.putAll(c.getConfiguration());
        }

        InboxProperties.Retry cr = c.getRetry() == null ? new InboxProperties.Retry() : c.getRetry();
        InboxProperties.Retry dr = d.getRetry() == null ? new InboxProperties.Retry() : d.getRetry();
        BackoffSpec immediate = spec(prefix + ".retry.immediate", IMMEDIATE, dr.getImmediate(), cr.getImmediate(), problems);
        BackoffSpec delayed = spec(prefix + ".retry.delayed", DELAYED, dr.getDelayed(), cr.getDelayed(), problems);
        boolean delayedMaxSet = (cr.getDelayed() != null && cr.getDelayed().getMax() != null)
                || (dr.getDelayed() != null && dr.getDelayed().getMax() != null);
        DelayedStrategy strategy = first(cr.getDelayed() == null ? null : cr.getDelayed().getStrategy(),
                dr.getDelayed() == null ? null : dr.getDelayed().getStrategy(), DelayedStrategy.AUTO);
        List<QueueBand> queues = new ArrayList<>();
        if (cr.getDelayed() != null && cr.getDelayed().getQueues() != null) {
            for (int i = 0; i < cr.getDelayed().getQueues().size(); i++) {
                InboxProperties.Queue q = cr.getDelayed().getQueues().get(i);
                if (q.getDelay() == null || q.getDestination() == null) {
                    problems.add(prefix + ".retry.delayed.queues[" + i + "] needs delay and destination");
                } else {
                    queues.add(new QueueBand(q.getDelay(), q.getDestination()));
                }
            }
        }
        var nonRetryable = types(prefix + ".retry.non-retryable",
                first(cr.getNonRetryable(), dr.getNonRetryable(), NON_RETRYABLE), loader, problems);
        var skipImmediate = types(prefix + ".retry.skip-immediate",
                first(cr.getSkipImmediate(), dr.getSkipImmediate(), List.of()), loader, problems);
        var retryable = types(prefix + ".retry.retryable",
                first(cr.getRetryable(), dr.getRetryable(), List.of()), loader, problems);
        boolean includeMessage = first(cr.getIncludeExceptionMessage(), dr.getIncludeExceptionMessage(), true);

        InboxProperties.Idempotency ci = c.getIdempotency() == null ? new InboxProperties.Idempotency() : c.getIdempotency();
        InboxProperties.Idempotency di = d.getIdempotency() == null ? new InboxProperties.Idempotency() : d.getIdempotency();
        boolean idempotency = first(ci.getEnabled(), di.getEnabled(), false);
        Duration completedTtl = first(ci.getTtl(), di.getTtl(), Duration.ofDays(7));
        Duration reservationTtl = first(ci.getReservationTtl(), di.getReservationTtl());

        int concurrency = first(c.getConcurrency(), d.getConcurrency(), 1);
        if (concurrency < 1) {
            problems.add(prefix + ".concurrency must be at least 1");
        }
        int maxMessages = parse(prefix, configuration, "max-messages-per-poll", Integer::parseInt, 1, problems);
        Duration pollWait = parse(prefix, configuration, "poll-wait", DurationStyle::detectAndParse,
                Duration.ofSeconds(20), problems);
        Duration maxProcessing = parse(prefix, configuration, "max-processing", DurationStyle::detectAndParse,
                Duration.ofMinutes(15), problems);
        Duration shutdownTimeout = parse(prefix, configuration, "shutdown-timeout", DurationStyle::detectAndParse,
                Duration.ofSeconds(30), problems);
        if (problems.size() > before) {
            return null;
        }
        return new Consumer(name, c.getDestination(), c.getDeadLetter(), c.getFailed(), concurrency,
                first(c.getOrdering(), d.getOrdering(), Ordering.RELAXED),
                first(c.getEnvelope(), d.getEnvelope(), EnvelopeMode.AUTO), includeMessage, immediate, delayed,
                delayedMaxSet, strategy, List.copyOf(queues), nonRetryable, skipImmediate, retryable, idempotency,
                completedTtl, reservationTtl, maxMessages, pollWait, maxProcessing, shutdownTimeout);
    }

    private static void forbiddenInDefaults(ConsumerOptions d, List<String> problems) {
        String why = " belongs to one consumer and is not accepted in inbox.defaults";
        if (d.getDestination() != null) {
            problems.add("inbox.defaults.destination" + why);
        }
        if (d.getDeadLetter() != null) {
            problems.add("inbox.defaults.dead-letter" + why);
        }
        if (d.getFailed() != null) {
            problems.add("inbox.defaults.failed" + why);
        }
        if (d.getConfiguration() != null) {
            problems.add("inbox.defaults.configuration is not accepted; use inbox.brokers.<name>.configuration");
        }
        if (d.getRetry() != null && d.getRetry().getDelayed() != null && d.getRetry().getDelayed().getQueues() != null) {
            problems.add("inbox.defaults.retry.delayed.queues" + why);
        }
    }

    private static void unsupported(String prefix, ConsumerOptions o, List<String> problems) {
        String later = " is not supported yet";
        if (o.getRetry() != null && o.getRetry().getDeadLetter() != null) {
            problems.add(prefix + ".retry.dead-letter (level 3)" + later + " (F5)");
        }
        if (o.getStaleCheck() != null) {
            problems.add(prefix + ".stale-check" + later + " (F6)");
        }
        if (o.getExpiration() != null) {
            problems.add(prefix + ".expiration" + later + " (F6)");
        }
        if (o.getCircuitBreaker() != null) {
            problems.add(prefix + ".circuit-breaker" + later + " (F6)");
        }
        if (o.getBatch() != null) {
            problems.add(prefix + ".batch" + later + " (F6)");
        }
        if (o.getKeyLanes() != null) {
            problems.add(prefix + ".key-lanes" + later + " (F6)");
        }
    }

    private static void unknownConfiguration(String prefix, Map<String, String> configuration, List<String> problems) {
        if (configuration == null) {
            return;
        }
        configuration.keySet().stream().filter(k -> !CONFIGURATION_KEYS.contains(k)).forEach(k -> problems.add(
                prefix + "." + k + " is unknown; accepted: " + String.join(", ", CONFIGURATION_KEYS.stream().sorted().toList())));
    }

    private static BackoffSpec spec(String prefix, Backoff library, Backoff defaults, Backoff consumer,
                                    List<String> problems) {
        Backoff d = defaults == null ? new Backoff() : defaults;
        Backoff c = consumer == null ? new Backoff() : consumer;
        try {
            return new BackoffSpec(first(c.getMaxAttempts(), d.getMaxAttempts(), library.getMaxAttempts()),
                    first(c.getInitial(), d.getInitial(), library.getInitial()),
                    first(c.getMultiplier(), d.getMultiplier(), library.getMultiplier()),
                    first(c.getMax(), d.getMax(), library.getMax()),
                    first(c.getJitter(), d.getJitter(), library.getJitter()));
        } catch (IllegalArgumentException e) {
            problems.add(prefix + ": " + e.getMessage());
            return null;
        }
    }

    private static List<Class<? extends Throwable>> types(String prefix, List<String> names, ClassLoader loader,
                                                          List<String> problems) {
        try {
            return ThrowableTypes.load(names, loader);
        } catch (IllegalArgumentException e) {
            problems.add(prefix + ": " + e.getMessage());
            return List.of();
        }
    }

    private static <T> T parse(String prefix, Map<String, String> configuration, String key, Function<String, T> parser,
                               T fallback, List<String> problems) {
        String value = configuration.get(key);
        if (value == null) {
            return fallback;
        }
        try {
            return parser.apply(value.trim());
        } catch (RuntimeException e) {
            problems.add(prefix + ".configuration." + key + ": invalid value '" + value + "'");
            return fallback;
        }
    }

    private static Backoff backoff(int maxAttempts, Duration initial, double multiplier, Duration max, double jitter) {
        Backoff b = new Backoff();
        b.setMaxAttempts(maxAttempts);
        b.setInitial(initial);
        b.setMultiplier(multiplier);
        b.setMax(max);
        b.setJitter(jitter);
        return b;
    }

    @SafeVarargs
    private static <T> T first(T... values) {
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
