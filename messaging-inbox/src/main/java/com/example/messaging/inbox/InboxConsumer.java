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
    /** Level 3 (ADR-0002); {@code null} without a dead-letter destination or with no passes. */
    private final DeadLetterPass deadLetterPass;
    private java.util.concurrent.ScheduledExecutorService passScheduler;

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
        this(settings, pipeline, destinations, stop, clock, random, observer, ConsumerObserver.NONE);
    }

    /** @param passObserver the same hook for the level-3 passes, usually tagged {@code <consumer>.dead-letter} */
    public InboxConsumer(ConsumerSettings settings, Pipeline<?> pipeline, Destinations destinations, StopSignal stop,
                         Clock clock, DoubleSupplier random, ConsumerObserver observer, ConsumerObserver passObserver) {
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
        this.deadLetterPass = destinations.deadLetter() != null && destinations.deadLetterReceivers() != null
                && policy.deadLetterMaxAttempts() > 0
                ? new DeadLetterPass(settings, pipeline, applier, destinations, stop, clock,
                new SafeObserver(Objects.requireNonNull(passObserver, "passObserver")), settings.deadLetterMaxPerPass())
                : null;
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
        if (deadLetterPass != null) {
            long interval = settings.deadLetterInterval().toMillis();
            passScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().name("inbox-" + settings.consumer() + "-dead-letter").factory());
            // fixed delay: a pass never overlaps the next one, which starts an interval after it ends
            passScheduler.scheduleWithFixedDelay(this::runPass, interval, interval,
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    private void runPass() {
        long begin = System.nanoTime();
        try {
            deadLetterPass.runPass();
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (Throwable t) {
            // anything escaping would cancel every later pass of scheduleWithFixedDelay, silently
            LOG.log(System.Logger.Level.ERROR, "Consumer " + settings.consumer() + ": dead-letter pass failed", t);
        }
        Duration took = Duration.ofNanos(System.nanoTime() - begin);
        if (took.compareTo(settings.deadLetterInterval()) > 0) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: dead-letter pass took {1}, above the interval {2}",
                    settings.consumer(), took, settings.deadLetterInterval());
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
        stopPass(deadline);
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

    /** The pass sees the stop signal between messages; a running handler is not interrupted before the timeout. */
    private void stopPass(long deadline) {
        if (passScheduler == null) {
            return;
        }
        passScheduler.shutdown();
        try {
            if (!passScheduler.awaitTermination(Math.max(0, deadline - System.nanoTime()),
                    java.util.concurrent.TimeUnit.NANOSECONDS)) {
                LOG.log(System.Logger.Level.WARNING, "Consumer {0}: dead-letter pass still running after {1}; "
                        + "interrupting it", settings.consumer(), settings.shutdownTimeout());
                passScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            passScheduler.shutdownNow();
            Thread.currentThread().interrupt();
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
