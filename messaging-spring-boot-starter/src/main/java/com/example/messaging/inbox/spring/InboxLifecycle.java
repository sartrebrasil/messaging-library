package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.InboxConsumer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Starts and stops every consumer with the application context (ADR-0009). Configuration and handlers are checked
 * once all singletons exist; the consumers are built on {@link #start()} (which opens receivers) and rebuilt on every
 * start after a stop, because a stopped consumer cannot restart.
 *
 * <p>Stops before the messaging-lib clients close (singletons are destroyed after the lifecycle stops), all
 * consumers in parallel, so the shutdown takes one {@code shutdown-timeout}, not one per consumer.</p>
 */
public final class InboxLifecycle implements SmartLifecycle, SmartInitializingSingleton, DisposableBean {

    private static final System.Logger LOG = System.getLogger(InboxLifecycle.class.getName());

    private final InboxProperties properties;
    private final ConfigurableListableBeanFactory beans;
    private final ConsumerBuilder builder;
    private final Map<String, AtomicReference<InboxConsumer>> current = new LinkedHashMap<>();
    private List<InboxConfig.Consumer> configs = List.of();
    private Map<String, HandlerBinding.Bound> handlers = Map.of();
    private volatile boolean running;

    InboxLifecycle(InboxProperties properties, ConfigurableListableBeanFactory beans, ConsumerBuilder builder) {
        this.properties = properties;
        this.beans = beans;
        this.builder = builder;
    }

    @Override
    public void afterSingletonsInstantiated() {
        List<InboxConfig.Consumer> all = InboxConfig.resolve(properties, beans.getBeanClassLoader());
        // disabled receivers are validated too: a bad address fails now, not when someone turns it on
        List<String> problems = all.stream().flatMap(c -> builder.validate(c).stream()).toList();
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Invalid messaging.receivers configuration:\n - "
                    + String.join("\n - ", problems));
        }
        configs = all.stream().filter(InboxConfig.Consumer::enabled).toList();
        handlers = HandlerBinding.bind(names(all), names(configs), beans);
        configs.forEach(c -> current.put(c.name(), new AtomicReference<>()));
    }

    private static Set<String> names(List<InboxConfig.Consumer> consumers) {
        return consumers.stream().map(InboxConfig.Consumer::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        List<InboxConsumer> built = new ArrayList<>();
        try {
            for (InboxConfig.Consumer c : configs) {
                built.add(builder.build(c, handlers.get(c.name())));
            }
        } catch (RuntimeException e) {
            built.forEach(InboxConsumer::stop);
            throw e;
        }
        for (InboxConsumer consumer : built) {
            current.get(consumer.settings().consumer()).set(consumer);
            consumer.start();
        }
        running = true;
    }

    @Override
    public void stop() {
        stopAll();
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stopAll();
        } finally {
            callback.run();
        }
    }

    private synchronized void stopAll() {
        if (!running) {
            return;
        }
        running = false;
        List<Thread> stopping = new ArrayList<>();
        for (AtomicReference<InboxConsumer> ref : current.values()) {
            InboxConsumer consumer = ref.get();
            if (consumer != null) {
                stopping.add(Thread.ofPlatform().name("inbox-stop-" + consumer.settings().consumer())
                        .start(consumer::stop));
            }
        }
        for (Thread thread : stopping) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.log(System.Logger.Level.WARNING, "interrupted while stopping inbox consumers");
                return;
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** End of the last receive of {@code consumer}, for the liveness gauge; {@code null} before the first start. */
    Instant lastReceive(String consumer) {
        AtomicReference<InboxConsumer> ref = current.get(consumer);
        InboxConsumer c = ref == null ? null : ref.get();
        return c == null ? null : c.lastReceive();
    }

    /** The running consumer, for tests and diagnostics. */
    public InboxConsumer consumer(String name) {
        AtomicReference<InboxConsumer> ref = current.get(name);
        return ref == null ? null : ref.get();
    }

    @Override
    public void destroy() {
        stopAll();
    }
}
