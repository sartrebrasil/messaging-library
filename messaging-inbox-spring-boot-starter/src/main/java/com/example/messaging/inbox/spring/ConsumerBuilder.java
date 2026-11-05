package com.example.messaging.inbox.spring;

import com.example.messaging.Capabilities;
import com.example.messaging.MessageReceiver;
import com.example.messaging.inbox.ConsumerObserver;
import com.example.messaging.inbox.ConsumerSettings;
import com.example.messaging.inbox.Destinations;
import com.example.messaging.inbox.EnvelopeReader;
import com.example.messaging.inbox.InboxConsumer;
import com.example.messaging.inbox.InboxIdempotency;
import com.example.messaging.inbox.Pipeline;
import com.example.messaging.inbox.PayloadDeserializer;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.StopSignal;
import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import org.springframework.beans.factory.ListableBeanFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/** Builds one {@link InboxConsumer} from its merged configuration, handler and the broker capabilities. */
final class ConsumerBuilder {

    /** Default margin of the reservation TTL over max-processing plus the hold delays. */
    static final Duration RESERVATION_MARGIN = Duration.ofMinutes(5);

    private final InboxDestinations destinations;
    private final PayloadDeserializer deserializer;
    private final ListableBeanFactory beans;
    private final Function<InboxConfig.Consumer, ConsumerObserver> observers;
    private final Clock clock;

    ConsumerBuilder(InboxDestinations destinations, PayloadDeserializer deserializer, ListableBeanFactory beans,
                    Function<InboxConfig.Consumer, ConsumerObserver> observers, Clock clock) {
        this.destinations = destinations;
        this.deserializer = deserializer;
        this.beans = beans;
        this.observers = observers;
        this.clock = clock;
    }

    InboxConsumer build(InboxConfig.Consumer c, HandlerBinding.Bound handler) {
        // the probe receiver becomes worker 0's: on Service Bus with sessions an extra one would hold a session
        MessageReceiver probe = destinations.receiver(c.destination());
        try {
            return build(c, handler, probe);
        } catch (RuntimeException e) {
            probe.close();
            throw e;
        }
    }

    private InboxConsumer build(InboxConfig.Consumer c, HandlerBinding.Bound handler, MessageReceiver probe) {
        String prefix = "inbox.consumers." + c.name();
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
        RetryPolicy policy = new RetryPolicy(c.immediate(), delayed, c.deadLetter() != null, 3, c.nonRetryable(),
                c.skipImmediate(), c.retryable());
        ConsumerSettings settings = new ConsumerSettings(c.name(), c.destination(), c.concurrency(),
                c.maxMessagesPerPoll(), c.pollWait(), c.maxProcessing(), c.shutdownTimeout(), c.ordering(),
                c.strategy(), c.includeExceptionMessage());
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
                ? probe : destinations.receiver(c.destination());
        List<RetryQueue> queues = c.queues().stream()
                .map(q -> new RetryQueue(q.delay(), destinations.sender(q.destination()))).toList();
        Destinations broker = new Destinations(receivers, destinations.sender(c.failed()),
                c.deadLetter() == null ? null : destinations.sender(c.deadLetter()), queues);
        return new InboxConsumer(settings, pipeline, broker, stop, clock, () -> ThreadLocalRandom.current().nextDouble(),
                observers.apply(c));
    }
}
