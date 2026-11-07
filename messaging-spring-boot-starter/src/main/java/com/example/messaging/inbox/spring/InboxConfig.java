package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.EnvelopeMode;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.inbox.PayloadDeserializationException;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.ThrowableTypes;
import com.example.messaging.inbox.spring.InboxProperties.Backoff;
import com.example.messaging.inbox.spring.InboxProperties.ConsumerOptions;
import com.example.messaging.spring.MessagingProperties.Endpoint;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Merges {@code messaging.receivers.<name>} over {@code messaging.receiver-defaults} over the library defaults,
 * property by property (ADR-0008), and validates everything that does not need a provider. All problems are reported
 * at once; the address is validated against its provider by {@link InboxDestinations#validate}.
 */
final class InboxConfig {

    static final String RECEIVERS = "messaging.receivers.";
    static final String DEFAULTS = "messaging.receiver-defaults";

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
     * @param enabled         {@code false}: validated, never started
     * @param provider        {@code null}: the only provider
     * @param endpoint        receive address: {@code destination}, {@code subscription}, {@code options}
     * @param deadLetter      {@code null} without a dead-letter destination
     * @param delayedMaxSet   {@code retry.delayed.max} was set by the user; when not, the starter caps it to the
     *                        receiver's limit
     * @param reservationTtl  {@code null}: derived from the loop settings
     */
    record Consumer(String name, boolean enabled, String provider, Endpoint endpoint, String destination, String deadLetter, String failed, int concurrency,
                    Ordering ordering, EnvelopeMode envelope, boolean includeExceptionMessage,
                    BackoffSpec immediate, BackoffSpec delayed, boolean delayedMaxSet, DelayedStrategy strategy,
                    List<QueueBand> queues, List<Class<? extends Throwable>> nonRetryable,
                    List<Class<? extends Throwable>> skipImmediate, List<Class<? extends Throwable>> retryable,
                    boolean idempotency, Duration completedTtl, Duration reservationTtl,
                    int maxMessagesPerPoll, Duration pollWait, Duration maxProcessing, Duration shutdownTimeout,
                    int keyLanes, int deadLetterMaxAttempts, Duration deadLetterInterval, int deadLetterMaxPerPass) {
    }

    private InboxConfig() {
    }

    /** @throws IllegalStateException listing every problem */
    static List<Consumer> resolve(InboxProperties properties, ClassLoader loader) {
        List<String> problems = new ArrayList<>();
        ConsumerOptions defaults = properties.getDefaults() == null ? new ConsumerOptions() : properties.getDefaults();
        forbiddenInDefaults(defaults, problems);
        unsupported(DEFAULTS, defaults, problems);
        List<Consumer> consumers = new ArrayList<>();
        properties.getConsumers().forEach((name, options) -> {
            Consumer consumer = consumer(name, options, defaults, loader, problems);
            if (consumer != null) {
                consumers.add(consumer);
            }
        });
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Invalid messaging.receivers configuration:\n - "
                    + String.join("\n - ", problems));
        }
        return consumers;
    }

    private static Consumer consumer(String name, ConsumerOptions c, ConsumerOptions d, ClassLoader loader,
                                     List<String> problems) {
        String prefix = RECEIVERS + name;
        int before = problems.size();
        unsupported(prefix, c, problems);
        if (c.getDestination() == null) {
            problems.add(prefix + ".destination is required");
        }
        if (c.getFailed() == null) {
            problems.add(prefix + ".failed is required (ADR-0002)");
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
        int keyLanes = first(c.getKeyLanes(), d.getKeyLanes(), 1);
        if (keyLanes < 1) {
            problems.add(prefix + ".key-lanes must be at least 1");
        }
        int maxMessages = first(c.getMaxMessagesPerPoll(), d.getMaxMessagesPerPoll(), 1);
        if (maxMessages < 1) {
            problems.add(prefix + ".max-messages-per-poll must be at least 1");
        }
        Duration pollWait = first(c.getPollWait(), d.getPollWait(), Duration.ofSeconds(20));
        Duration maxProcessing = first(c.getMaxProcessing(), d.getMaxProcessing(), Duration.ofMinutes(15));
        Duration shutdownTimeout = first(c.getShutdownTimeout(), d.getShutdownTimeout(), Duration.ofSeconds(30));
        if (problems.size() > before) {
            return null;
        }
        Endpoint endpoint = new Endpoint(c.getDestination(), c.getSubscription(), null, c.getOptions());
        InboxProperties.DeadLetterRetry cl = cr.getDeadLetter() == null ? new InboxProperties.DeadLetterRetry()
                : cr.getDeadLetter();
        InboxProperties.DeadLetterRetry dl = dr.getDeadLetter() == null ? new InboxProperties.DeadLetterRetry()
                : dr.getDeadLetter();
        int deadLetterAttempts = first(cl.getMaxAttempts(), dl.getMaxAttempts(), 3);
        Duration deadLetterInterval = first(cl.getInterval(), dl.getInterval(), Duration.ofMinutes(15));
        int deadLetterMaxPerPass = first(cl.getMaxPerPass(), dl.getMaxPerPass(), 100);
        if (deadLetterAttempts < 0) {
            problems.add(prefix + ".retry.dead-letter.max-attempts must be at least 0");
        }
        if (deadLetterInterval.isNegative() || deadLetterInterval.isZero()) {
            problems.add(prefix + ".retry.dead-letter.interval must be positive");
        }
        if (deadLetterMaxPerPass < 1) {
            problems.add(prefix + ".retry.dead-letter.max-per-pass must be at least 1");
        }
        if (problems.size() > before) {
            return null;
        }
        return new Consumer(name, first(c.getEnabled(), true), first(c.getProvider(), d.getProvider()), endpoint,
                c.getDestination(), c.getDeadLetter(), c.getFailed(), concurrency,
                first(c.getOrdering(), d.getOrdering(), Ordering.RELAXED),
                first(c.getEnvelope(), d.getEnvelope(), EnvelopeMode.AUTO), includeMessage, immediate, delayed,
                delayedMaxSet, strategy, List.copyOf(queues), nonRetryable, skipImmediate, retryable, idempotency,
                completedTtl, reservationTtl, maxMessages, pollWait, maxProcessing, shutdownTimeout, keyLanes,
                deadLetterAttempts, deadLetterInterval, deadLetterMaxPerPass);
    }

    private static void forbiddenInDefaults(ConsumerOptions d, List<String> problems) {
        String why = " belongs to one receiver and is not accepted in " + DEFAULTS;
        forbidden(problems, why, "destination", d.getDestination(), "subscription", d.getSubscription(),
                "dead-letter", d.getDeadLetter(), "failed", d.getFailed(), "enabled", d.getEnabled(),
                "options", d.getOptions());
        if (d.getRetry() != null && d.getRetry().getDelayed() != null && d.getRetry().getDelayed().getQueues() != null) {
            problems.add(DEFAULTS + ".retry.delayed.queues" + why);
        }
    }

    private static void forbidden(List<String> problems, String why, Object... namesAndValues) {
        for (int i = 0; i < namesAndValues.length; i += 2) {
            if (namesAndValues[i + 1] != null) {
                problems.add(DEFAULTS + "." + namesAndValues[i] + why);
            }
        }
    }

    private static void unsupported(String prefix, ConsumerOptions o, List<String> problems) {
        String later = " is not supported yet";
        if (o.getRetry() != null && o.getRetry().getDeadLetter() != null
                && o.getRetry().getDeadLetter().getConcurrency() != null) {
            problems.add(prefix + ".retry.dead-letter.concurrency" + later + " (passes run one message at a time)");
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
