package com.example.messaging.inbox;

import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessagingException;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.BackoffSpec;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

/**
 * One worker of a consumer: its own receiver, a loop of {@code receive}, and the messages of each poll in sequence
 * (ADR-0009). Nothing that one message does stops the loop: only {@link OutOfMemoryError} ends the worker.
 */
final class Worker implements Runnable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    // ADR-0009: 1 s, x2, up to 30 s (retryable) or 1 min (not retryable), jitter 0.2
    private static final BackoffSpec RECEIVE_RETRYABLE =
            new BackoffSpec(Integer.MAX_VALUE, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), 0.2);
    private static final BackoffSpec RECEIVE_NOT_RETRYABLE =
            new BackoffSpec(Integer.MAX_VALUE, Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1), 0.2);

    private final ConsumerSettings settings;
    private final MessageReceiver receiver;
    private final Pipeline<?> pipeline;
    private final OutcomeApplier applier;
    private final DelayedStrategy strategy;
    private final Duration leaseBudget;
    private final StopSignal stop;
    private final AtomicReference<Instant> lastReceive;
    private final Clock clock;
    private final DoubleSupplier random;
    private final ConsumerObserver observer;
    private final ExecutorService lanePool;
    /** One warning per worker; lanes may race on it, and a second warning is harmless. */
    private volatile boolean warnedNoDeliveryCount;

    Worker(ConsumerSettings settings, MessageReceiver receiver, Pipeline<?> pipeline, OutcomeApplier applier,
           DelayedStrategy strategy, Duration leaseBudget, StopSignal stop, AtomicReference<Instant> lastReceive,
           Clock clock, DoubleSupplier random, ConsumerObserver observer) {
        this.observer = observer;
        // ADR-0016: concurrency x key-lanes platform threads per consumer, created once per worker
        this.lanePool = settings.keyLanes() > 1
                ? Executors.newFixedThreadPool(settings.keyLanes(),
                Thread.ofPlatform().name("inbox-" + settings.consumer() + "-lane-", 0).factory())
                : null;
        this.settings = settings;
        this.receiver = receiver;
        this.pipeline = pipeline;
        this.applier = applier;
        this.strategy = strategy;
        this.leaseBudget = leaseBudget;
        this.stop = stop;
        this.lastReceive = lastReceive;
        this.clock = clock;
        this.random = random;
    }

    @Override
    public void run() {
        int receiveFailures = 0;
        try {
            while (!stop.stopping()) {
                List<ReceivedMessage> messages;
                try {
                    messages = receiver.receive(settings.maxMessagesPerPoll(), settings.pollWait());
                } catch (RuntimeException e) {
                    if (stop.stopping()) {
                        return;
                    }
                    receiveFailures++;
                    observer.receiveFailed(e);
                    boolean retryable = !(e instanceof MessagingException me) || me.retryable();
                    Duration wait = (retryable ? RECEIVE_RETRYABLE : RECEIVE_NOT_RETRYABLE).delayFor(receiveFailures, random);
                    LOG.log(retryable ? System.Logger.Level.WARNING : System.Logger.Level.ERROR,
                            "Consumer " + settings.consumer() + ": receive failed, retrying in " + wait, e);
                    stop.sleep(wait);
                    continue;
                }
                receiveFailures = 0;
                lastReceive.set(clock.instant());
                handleAll(messages);
            }
        } catch (InterruptedException e) {
            // stop signal during a receive backoff
        } finally {
            if (lanePool != null) {
                lanePool.shutdown();
            }
            receiver.close();
        }
    }

    /**
     * The messages of one {@code receive}, split in key lanes (ADR-0016): lanes run in parallel, a lane in order,
     * and the next {@code receive} waits for all of them. With one lane, everything runs on this thread.
     */
    private void handleAll(List<ReceivedMessage> messages) {
        List<List<Integer>> lanes = lanes(messages);
        KeepAlive[] leases = new KeepAlive[messages.size()];
        try {
            for (List<Integer> lane : lanes) {
                for (int position = 0; position < lane.size(); position++) {
                    int index = lane.get(position);
                    // a message waits for the ones before it in its lane (ADR-0009, ADR-0016)
                    leases[index] = keepAlive(messages.get(index), leaseBudget.multipliedBy(position + 1));
                }
            }
            messages.forEach(observer::received);
            if (lanes.size() == 1) {
                runLane(messages, lanes.getFirst(), leases);
                return;
            }
            List<Future<?>> running = new ArrayList<>(lanes.size());
            for (List<Integer> lane : lanes) {
                running.add(lanePool.submit(() -> runLane(messages, lane, leases)));
            }
            for (Future<?> lane : running) {
                await(lane);
            }
        } finally {
            for (KeepAlive lease : leases) {
                if (lease != null) {
                    lease.close();
                }
            }
        }
    }

    private void runLane(List<ReceivedMessage> messages, List<Integer> lane, KeepAlive[] leases) {
        for (int index : lane) {
            ReceivedMessage message = messages.get(index);
            try {
                if (stop.stopping()) {
                    report(message, null, Outcome.RELEASE,
                            applier.release(receiver, message, Effect.RELEASED), System.nanoTime());
                } else {
                    handle(message);
                }
            } finally {
                leases[index].close();
            }
        }
    }

    /** Same {@code orderingKey}, same lane; no key, the lane with fewest messages. Empty lanes are left out. */
    private List<List<Integer>> lanes(List<ReceivedMessage> messages) {
        int count = Math.min(settings.keyLanes(), messages.size());
        if (count <= 1) {
            List<Integer> all = new ArrayList<>(messages.size());
            for (int i = 0; i < messages.size(); i++) {
                all.add(i);
            }
            return List.of(all);
        }
        List<List<Integer>> lanes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            lanes.add(new ArrayList<>());
        }
        List<Integer> unkeyed = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            String key = messages.get(i).orderingKey();
            if (key == null) {
                unkeyed.add(i);
            } else {
                lanes.get(Math.floorMod(key.hashCode(), count)).add(i);
            }
        }
        for (int i : unkeyed) {
            lanes.stream().min(java.util.Comparator.comparingInt(List::size)).orElseThrow().add(i);
        }
        lanes.removeIf(List::isEmpty);
        return lanes;
    }

    /** Lanes never throw (handle catches everything but OutOfMemoryError); shutdown waits for them to finish. */
    private void await(Future<?> lane) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    lane.get();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof OutOfMemoryError oom) {
                        throw oom;
                    }
                    LOG.log(System.Logger.Level.ERROR, "inbox lane failed", e.getCause());
                    observer.unexpectedFailure(ConsumerObserver.Stage.LANE, e.getCause());
                    return;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private KeepAlive keepAlive(ReceivedMessage message, Duration budget) {
        try {
            return receiver.keepAlive(message, budget);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer " + settings.consumer() + ": keepAlive of message "
                    + message.messageId() + " failed; processing without lease renewal", e);
            observer.keepAliveFailed(message, e);
            return NO_LEASE;
        }
    }

    private void handle(ReceivedMessage message) {
        long start = System.nanoTime();
        InboxMessage inbox = null;
        Outcome outcome;
        OutcomeApplier.Applied applied;
        try {
            int hold = 0;
            while (true) {
                Pipeline.Execution execution = pipeline.process(message, settings.destination(), hold, observer);
                inbox = execution.message();
                outcome = execution.outcome();
                if (outcome instanceof Outcome.Retry retry && holdsInMemory(message)) {
                    // in_progress waits spend no attempt, so they are bounded by time: past the keepAlive budget the
                    // lease would be lost while holding; release it and let the broker bring it back
                    if (retry.reason() == Outcome.RetryReason.IN_PROGRESS
                            && Duration.ofNanos(System.nanoTime() - start).plus(retry.delay()).compareTo(leaseBudget) > 0) {
                        applied = applier.release(receiver, message, Effect.RELEASED);
                        break;
                    }
                    observer.held(inbox, retry);
                    try {
                        stop.sleep(retry.delay());
                    } catch (InterruptedException e) {
                        outcome = Outcome.RELEASE;
                        applied = applier.release(receiver, message, Effect.RELEASED);
                        break;
                    }
                    if (retry.reason() == Outcome.RetryReason.FAILURE) {
                        hold++; // in_progress waits do not spend an attempt (ADR-0004)
                    }
                    continue;
                }
                applied = applier.apply(receiver, message, outcome);
                break;
            }
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (Throwable t) {
            // Errors too (StackOverflowError, AssertionError, NoClassDefFoundError): the worker must not die (T-88/B20)
            LOG.log(System.Logger.Level.ERROR, "Consumer " + settings.consumer() + ": unexpected failure on message "
                    + message.messageId() + "; released", t);
            observer.unexpectedFailure(ConsumerObserver.Stage.DELIVERY, t);
            outcome = Outcome.RELEASE;
            applied = applier.release(receiver, message, Effect.RELEASED);
        }
        report(message, inbox, outcome, applied, start);
    }

    /**
     * {@code hold}, or {@code broker-delay} on a message without {@code deliveryCount} (Pub/Sub without a dead-letter
     * policy): the broker cannot count the delayed attempts, so they stay in memory instead of going to the
     * dead-letter destination on the first one.
     */
    private boolean holdsInMemory(ReceivedMessage message) {
        if (strategy == DelayedStrategy.HOLD) {
            return true;
        }
        if (strategy != DelayedStrategy.BROKER_DELAY || message.deliveryCount().isPresent()) {
            return false;
        }
        if (!warnedNoDeliveryCount) {
            warnedNoDeliveryCount = true;
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: messages without a delivery count; broker-delay holds "
                    + "the delayed attempts in memory (configure a dead-letter policy, or delayed.strategy: hold)",
                    settings.consumer());
        }
        return true;
    }

    private void report(ReceivedMessage message, InboxMessage inbox, Outcome outcome, OutcomeApplier.Applied applied,
                        long startNanos) {
        observer.delivered(new ConsumerObserver.Report(message, inbox, outcome, applied.effect(), strategy,
                applied.retrySkipped(), applied.budgetExceeded(), Duration.ofNanos(System.nanoTime() - startNanos),
                applied.failedCall()));
    }

    private static final KeepAlive NO_LEASE = new KeepAlive() {
        @Override
        public boolean lost() {
            return false;
        }

        @Override
        public void close() {
        }
    };
}
