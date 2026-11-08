package com.example.messaging.inbox.spring;

import com.example.messaging.Capabilities;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.spring.MessagingProperties.Endpoint;
import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.ConsumerSettings;
import com.example.messaging.inbox.Destinations;
import com.example.messaging.inbox.EnvelopeReader;
import com.example.messaging.inbox.InboxConsumer;
import com.example.messaging.inbox.InboxIdempotency;
import com.example.messaging.inbox.Pipeline;
import com.example.messaging.inbox.PayloadDeserializer;
import com.example.messaging.inbox.RedriveRoutes;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.redrive.Source;
import com.example.messaging.inbox.StopSignal;
import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import org.springframework.beans.factory.ListableBeanFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/** Builds one {@link InboxConsumer} from its merged configuration, handler and the broker capabilities. */
final class ConsumerBuilder {

    private static final System.Logger LOG = System.getLogger(ConsumerBuilder.class.getName());

    /** Default margin of the reservation TTL over max-processing plus the hold delays. */
    static final Duration RESERVATION_MARGIN = Duration.ofMinutes(5);

    private final InboxDestinations destinations;
    private final PayloadDeserializer deserializer;
    private final ListableBeanFactory beans;
    private final Function<InboxConfig.Consumer, ConsumerObserver> observers;
    private final Function<InboxConfig.Consumer, ConsumerObserver> passObservers;
    private final Clock clock;

    ConsumerBuilder(InboxDestinations destinations, PayloadDeserializer deserializer, ListableBeanFactory beans,
                    Function<InboxConfig.Consumer, ConsumerObserver> observers,
                    Function<InboxConfig.Consumer, ConsumerObserver> passObservers, Clock clock) {
        this.destinations = destinations;
        this.deserializer = deserializer;
        this.beans = beans;
        this.observers = observers;
        this.passObservers = passObservers;
        this.clock = clock;
    }

    /** Address problems of one receiver and of its copies, without opening a connection. */
    List<String> validate(InboxConfig.Consumer c) {
        String prefix = InboxConfig.RECEIVERS + c.name();
        List<String> problems = new ArrayList<>(destinations.validate(prefix, c.provider(), c.endpoint(), true));
        if (c.deadLetter() != null) {
            problems.addAll(destinations.validate(prefix + ".dead-letter", c.provider(), copy(c.deadLetter()), false));
        }
        problems.addAll(destinations.validate(prefix + ".failed", c.provider(), copy(c.failed()), false));
        for (int i = 0; i < c.queues().size(); i++) {
            problems.addAll(destinations.validate(prefix + ".retry.delayed.queues[" + i + "].destination",
                    c.provider(), copy(c.queues().get(i).destination()), false));
        }
        return problems;
    }

    InboxConsumer build(InboxConfig.Consumer c, HandlerBinding.Bound handler) {
        MessageSender deadLetter = c.deadLetter() == null ? null
                : destinations.sender(c.name() + ".dead-letter", c.provider(), copy(c.deadLetter()));
        // the probe receiver becomes worker 0's: on Service Bus with sessions an extra one would hold a session
        MessageReceiver probe = destinations.receiver(c.name(), c.provider(), c.endpoint(), deadLetter);
        try {
            return build(c, handler, probe, deadLetter);
        } catch (RuntimeException e) {
            probe.close();
            throw e;
        }
    }

    private InboxConsumer build(InboxConfig.Consumer c, HandlerBinding.Bound handler, MessageReceiver probe,
                                MessageSender deadLetter) {
        String prefix = InboxConfig.RECEIVERS + c.name();
        Capabilities caps = probe.capabilities();
        BackoffSpec delayed = c.delayed();
        if (!c.delayedMaxSet() && caps.delayedRedelivery()
                && (c.strategy() == DelayedStrategy.AUTO || c.strategy() == DelayedStrategy.BROKER_DELAY)
                && caps.maxRedeliveryDelay().compareTo(delayed.max()) < 0) {
            if (caps.maxRedeliveryDelay().compareTo(delayed.initial()) < 0) {
                throw new IllegalStateException(prefix + ": the receiver accepts delays up to "
                        + caps.maxRedeliveryDelay() + ", below retry.delayed.initial " + delayed.initial());
            }
            // ADR-0002: without an explicit delayed.max, the default is min(15 min, maxRedeliveryDelay)
            delayed = new BackoffSpec(delayed.maxAttempts(), delayed.initial(), delayed.multiplier(),
                    caps.maxRedeliveryDelay(), delayed.jitter());
        }
        RetryPolicy policy = new RetryPolicy(c.immediate(), delayed, c.deadLetter() != null, c.deadLetterMaxAttempts(),
                c.nonRetryable(),
                c.skipImmediate(), c.retryable());
        // the destination of the settings is the metrics tag: the receiver name, never the address
        ConsumerSettings settings = new ConsumerSettings(c.name(), c.name(), c.concurrency(),
                c.maxMessagesPerPoll(), c.pollWait(), c.maxProcessing(), c.shutdownTimeout(), c.ordering(),
                c.strategy(), c.includeExceptionMessage(), c.keyLanes(), c.deadLetterInterval(),
                c.deadLetterMaxPerPass());
        InboxIdempotency idempotency = null;
        if (c.idempotency()) {
            Duration reservation = c.reservationTtl() != null ? c.reservationTtl()
                    : c.maxProcessing().plus(delayed.totalBaseDelay(delayed.maxAttempts())).plus(RESERVATION_MARGIN);
            idempotency = IdempotencySupport.of(c.name(), beans, reservation, c.completedTtl(), clock);
            if (idempotency == null) {
                throw new IllegalStateException(prefix + ".idempotency.enabled needs one IdempotencyStore bean "
                        + "(idempotency-spring-boot-starter with idempotency.store)");
            }
        }
        StopSignal stop = new StopSignal();
        Pipeline<Object> pipeline = new Pipeline<>(c.name(), new EnvelopeReader(c.envelope()), deserializer,
                handler.payloadType(), handler.handler().get(), policy, stop, () -> ThreadLocalRandom.current().nextDouble(),
                idempotency);
        AtomicBoolean probeUsed = new AtomicBoolean();
        Supplier<MessageReceiver> receivers = () -> probeUsed.compareAndSet(false, true)
                ? probe : destinations.receiver(c.name(), c.provider(), c.endpoint(), deadLetter);
        // by index: two bands may round to the same delay, and each needs its own sender
        List<RetryQueue> queues = new ArrayList<>();
        for (int i = 0; i < c.queues().size(); i++) {
            InboxConfig.QueueBand q = c.queues().get(i);
            queues.add(new RetryQueue(q.delay(),
                    destinations.sender(c.name() + ".retry." + i, c.provider(), copy(q.destination()))));
        }
        // level 3 (ADR-0002): the passes read the DLQ under the name of its sender, so metrics and health group them
        Supplier<MessageReceiver> deadLetterReceivers = readableDeadLetter(c)
                ? () -> destinations.receiver(c.name() + ".dead-letter", c.provider(), copy(c.deadLetter()), null)
                : null;
        Destinations broker = new Destinations(receivers,
                destinations.sender(c.name() + ".failed", c.provider(), copy(c.failed())), deadLetter, queues,
                deadLetterReceivers);
        return new InboxConsumer(settings, pipeline, broker, stop, clock, () -> ThreadLocalRandom.current().nextDouble(),
                observers.apply(c), passObservers.apply(c));
    }

    /**
     * Where a redrive of {@code c} reads and writes (ADR-0012): {@code failed} and the DLQ under the names of their
     * senders, the main destination as {@code <receiver>.redrive} only when it is a queue. Senders are created on first
     * use, receivers per run; a source that cannot be read (Pub/Sub: a topic) is unavailable with the reason.
     */
    RedriveRoutes routes(InboxConfig.Consumer c) {
        String prefix = InboxConfig.RECEIVERS + c.name();
        Map<Source, String> unavailable = new EnumMap<>(Source.class);
        List<String> failedProblems = destinations.validate(prefix + ".failed", c.provider(), copy(c.failed()), true);
        if (!failedProblems.isEmpty()) {
            unavailable.put(Source.FAILED, String.join("; ", failedProblems));
        }
        Supplier<MessageReceiver> deadLetterReceivers = null;
        MessageSender deadLetter = null;
        if (c.deadLetter() != null) {
            deadLetter = destinations.sender(c.name() + ".dead-letter", c.provider(), copy(c.deadLetter()));
            List<String> problems = destinations.validate(prefix + ".dead-letter", c.provider(), copy(c.deadLetter()),
                    true);
            if (!problems.isEmpty()) {
                unavailable.put(Source.DEAD_LETTER, String.join("; ", problems));
            }
            deadLetterReceivers = () -> destinations.receiver(c.name() + ".dead-letter", c.provider(),
                    copy(c.deadLetter()), null);
        }
        MessageSender main = c.endpoint().subscription() == null
                ? destinations.sender(c.name() + ".redrive", c.provider(), copy(c.endpoint().destination()))
                : null;
        return new RedriveRoutes(() -> destinations.receiver(c.name() + ".failed", c.provider(), copy(c.failed()), null),
                destinations.sender(c.name() + ".failed", c.provider(), copy(c.failed())), deadLetterReceivers,
                deadLetter, main, unavailable);
    }

    /**
     * Level 3 reads the DLQ through the same address it sends to. Where that address cannot be read (on Pub/Sub the DLQ
     * is a topic, and a topic is not a subscription), level 3 is turned off with a warning instead of failing every
     * pass, or the startup of applications that configured the DLQ only as a parking queue.
     */
    private boolean readableDeadLetter(InboxConfig.Consumer c) {
        if (c.deadLetter() == null || c.deadLetterMaxAttempts() == 0) {
            return false;
        }
        List<String> problems = destinations.validate(InboxConfig.RECEIVERS + c.name() + ".dead-letter", c.provider(),
                copy(c.deadLetter()), true);
        if (!problems.isEmpty()) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: level 3 is off, the dead-letter destination cannot be "
                    + "read ({1}); set retry.dead-letter.max-attempts: 0 to silence this", c.name(), problems);
            return false;
        }
        return true;
    }

    /** dead-letter, failed and the retry queues: plain queues in the provider of the receiver (ADR-0008). */
    private static Endpoint copy(String destination) {
        return new Endpoint(destination, null, null, null);
    }
}
