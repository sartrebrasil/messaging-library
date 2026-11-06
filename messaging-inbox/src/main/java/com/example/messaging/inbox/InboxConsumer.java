package com.example.messaging.inbox;

import com.example.messaging.Capabilities;
import com.example.messaging.MessageReceiver;
import com.example.messaging.inbox.retry.RetryPolicy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

/**
 * Polling consumer over {@link MessageReceiver} (ADR-0009): {@code concurrency} platform-thread workers, each with
 * its own receiver, applying the {@link Outcome} of the {@link Pipeline}. The constructor validates the combination
 * of settings, policy and capabilities and fails fast (ADR-0009, startup validation).
 *
 * <p>{@link #stop()} follows the four steps of ADR-0009: stop receiving, cut waits through the {@link StopSignal}
 * (messages in a wait or not started go back by {@code nack(0)}), wait for running handlers up to
 * {@code shutdownTimeout}, close the receivers.</p>
 */
public final class InboxConsumer {

    private static final System.Logger LOG = System.getLogger(InboxConsumer.class.getName());

    private final ConsumerSettings settings;
    private final Pipeline<?> pipeline;
    private final Destinations destinations;
    private final StopSignal stop;
    private final Clock clock;
    private final DoubleSupplier random;
    private final ConsumerObserver observer;
    private final DelayedStrategy strategy;
    private final Duration leaseBudget;
    private final OutcomeApplier applier;
    private final AtomicReference<Instant> lastReceive;
    private final MessageReceiver firstReceiver;
    private final List<Thread> threads = new ArrayList<>();
    private final List<MessageReceiver> receivers = new ArrayList<>();
    private boolean started;
    private boolean stopped;

    /**
     * @param stop the same signal given to the {@link Pipeline} as its sleeper
     * @throws IllegalStateException when the settings cannot work with the receiver capabilities
     */
    public InboxConsumer(ConsumerSettings settings, Pipeline<?> pipeline, Destinations destinations, StopSignal stop,
                         Clock clock, DoubleSupplier random) {
        this(settings, pipeline, destinations, stop, clock, random, ConsumerObserver.NONE);
    }

    /** @param observer metrics and tracing hook; its failures never affect messages */
    public InboxConsumer(ConsumerSettings settings, Pipeline<?> pipeline, Destinations destinations, StopSignal stop,
                         Clock clock, DoubleSupplier random, ConsumerObserver observer) {
        this.observer = new SafeObserver(Objects.requireNonNull(observer, "observer"));
        this.settings = Objects.requireNonNull(settings, "settings");
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
        this.destinations = Objects.requireNonNull(destinations, "destinations");
        this.stop = Objects.requireNonNull(stop, "stop");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        RetryPolicy policy = pipeline.policy();
        if (policy.deadLetterEnabled() != (destinations.deadLetter() != null)) {
            throw new IllegalStateException("Consumer " + settings.consumer() + ": the retry policy "
                    + (policy.deadLetterEnabled() ? "uses" : "does not use")
                    + " a dead-letter destination, but one is " + (destinations.deadLetter() != null ? "" : "not ")
                    + "configured");
        }
        this.firstReceiver = destinations.receivers().get();
        try {
            Capabilities caps = firstReceiver.capabilities();
            DelayedStrategy.Choice choice = DelayedStrategy.resolve(settings.strategy(), settings.ordering(), caps,
                    !destinations.retryQueues().isEmpty(), policy);
            this.strategy = choice.strategy();
            LOG.log(System.Logger.Level.INFO, "Consumer {0}: delayed strategy {1} ({2})", settings.consumer(),
                    strategy, choice.reason());
        } catch (RuntimeException e) {
            firstReceiver.close();
            throw e;
        }
        // broker-delay holds in memory too when a message has no delivery count (Worker.holdsInMemory)
        Duration hold = strategy == DelayedStrategy.HOLD || strategy == DelayedStrategy.BROKER_DELAY
                ? policy.delayed().totalBaseDelay(policy.delayed().maxAttempts()) : Duration.ZERO;
        this.leaseBudget = settings.maxProcessing().plus(hold);
        InboxIdempotency idempotency = pipeline.idempotency();
        if (idempotency != null && idempotency.reservationTtl().compareTo(leaseBudget) <= 0) {
            firstReceiver.close();
            throw new IllegalStateException("Consumer " + settings.consumer() + ": idempotency reservation TTL "
                    + idempotency.reservationTtl() + " must be above max-processing plus the hold delays ("
                    + leaseBudget + "), or a slow delivery loses its key to a redelivery (ADR-0004)");
        }
        this.applier = new OutcomeApplier(settings, strategy, destinations, clock);
        this.lastReceive = new AtomicReference<>(clock.instant());
        if (settings.keyLanes() > 1 && settings.maxMessagesPerPoll() == 1) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: key-lanes {1} with max-messages-per-poll 1 has "
                    + "nothing to parallelize (ADR-0016)", settings.consumer(), settings.keyLanes());
        }
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        if (stopped) {
            throw new IllegalStateException("Consumer " + settings.consumer() + " already stopped");
        }
        started = true;
        lastReceive.set(clock.instant());
        for (int i = 0; i < settings.concurrency(); i++) {
            MessageReceiver receiver = i == 0 ? firstReceiver : destinations.receivers().get();
            receivers.add(receiver);
            Thread thread = Thread.ofPlatform()
                    .name("inbox-" + settings.consumer() + "-" + i)
                    .uncaughtExceptionHandler((t, e) -> LOG.log(System.Logger.Level.ERROR,
                            "Consumer " + settings.consumer() + ": worker " + t.getName() + " died", e))
                    .unstarted(new Worker(settings, receiver, pipeline, applier, strategy, leaseBudget, stop,
                            lastReceive, clock, random, observer));
            threads.add(thread);
            thread.start();
        }
    }

    /** Idempotent; safe before {@link #start()}. */
    public void stop() {
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            if (!started) {
                firstReceiver.close();
            }
        }
        stop.stop();
        long deadline = System.nanoTime() + settings.shutdownTimeout().toNanos();
        for (Thread thread : threads) {
            try {
                thread.join(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        for (int i = 0; i < threads.size(); i++) {
            if (threads.get(i).isAlive()) {
                LOG.log(System.Logger.Level.WARNING, "Consumer {0}: worker {1} still running after {2}; closing its "
                        + "receiver, unfinished messages come back when the lease expires", settings.consumer(),
                        threads.get(i).getName(), settings.shutdownTimeout());
                receivers.get(i).close();
            }
        }
    }

    public synchronized boolean running() {
        return started && !stopped;
    }

    /** End of the last {@code receive} of any worker, empty or not: the liveness signal (ADR-0007). */
    public Instant lastReceive() {
        return lastReceive.get();
    }

    public DelayedStrategy strategy() {
        return strategy;
    }

    public ConsumerSettings settings() {
        return settings;
    }
}
