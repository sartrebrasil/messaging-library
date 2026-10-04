package com.example.messaging.spi;

import com.example.messaging.KeepAlive;
import com.example.messaging.ReceivedMessage;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implementação de {@link com.example.messaging.MessageReceiver#keepAlive} para os adapters
 * (ADR-0002). Um por receiver: o adapter delega {@code keepAlive}, chama {@link #release} em
 * {@code ack}/{@code nack}/{@code deadLetter} e {@link #close} no próprio {@code close}.
 *
 * <p>Renova quando falta 1/3 do lease, estendendo pela duração observada no receive
 * ({@link ReceivedMessage#leaseDuration()}). Não agenda renovação que cairia depois de
 * {@code maxTotal}. Cada renovação roda numa virtual thread; o agendador só é criado no
 * primeiro uso.</p>
 */
public final class LeaseKeeper implements AutoCloseable {

    /** A chamada de {@code extendLease} do adapter. */
    @FunctionalInterface
    public interface Extender {
        void extend(ReceivedMessage message, Duration lease);
    }

    private final Extender extender;
    private final InstantSource clock;
    private final Map<Object, Task> active = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;
    private boolean closed;

    public LeaseKeeper(Extender extender) {
        this(extender, InstantSource.system());
    }

    LeaseKeeper(Extender extender, InstantSource clock) {
        this.extender = extender;
        this.clock = clock;
    }

    /**
     * @throws IllegalArgumentException {@code maxTotal} não positivo ou mensagem com lease não positivo
     * @throws IllegalStateException    depois de {@link #close()}
     */
    public KeepAlive keep(ReceivedMessage message, Duration maxTotal) {
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("LeaseKeeper fechado");
            }
        }
        if (maxTotal.isNegative() || maxTotal.isZero()) {
            throw new IllegalArgumentException("maxTotal precisa ser positivo: " + maxTotal);
        }
        Duration lease = message.leaseDuration();
        if (lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("Mensagem com lease não positivo: " + lease);
        }
        Task task = new Task(message, lease, clock.instant().plus(maxTotal));
        Task previous = active.put(key(message), task);
        if (previous != null) {
            previous.close();
        }
        task.schedule(message.leaseExpiresAt());
        return task;
    }

    /** Para a renovação da mensagem, se houver. Chamado pelo adapter ao confirmar ou devolver. */
    public void release(ReceivedMessage message) {
        Task task = active.get(key(message));
        if (task != null) {
            task.close();
        }
    }

    /** Para todas as renovações e o agendador. */
    @Override
    public void close() {
        ScheduledExecutorService toStop;
        synchronized (this) {
            closed = true;
            toStop = scheduler;
        }
        active.values().forEach(Task::close);
        if (toStop != null) {
            toStop.shutdownNow();
        }
    }

    private synchronized ScheduledExecutorService scheduler() {
        if (closed) {
            throw new IllegalStateException("LeaseKeeper fechado");
        }
        if (scheduler == null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofVirtual().name("messaging-keepalive-", 0).factory());
        }
        return scheduler;
    }

    private static Object key(ReceivedMessage message) {
        return message.handle() != null ? message.handle() : message;
    }

    private final class Task implements KeepAlive {

        private final ReceivedMessage message;
        private final Duration lease;
        private final Instant deadline;
        private volatile boolean lost;
        private boolean done;
        private ScheduledFuture<?> next;

        Task(ReceivedMessage message, Duration lease, Instant deadline) {
            this.message = message;
            this.lease = lease;
            this.deadline = deadline;
        }

        synchronized void schedule(Instant expiresAt) {
            if (done) {
                return;
            }
            Instant renewAt = expiresAt.minus(lease.dividedBy(3));
            if (!renewAt.isBefore(deadline)) {
                finish();
                return;
            }
            long delay = Math.max(0, Duration.between(clock.instant(), renewAt).toMillis());
            try {
                next = scheduler().schedule(() -> Thread.startVirtualThread(this::renew), delay, TimeUnit.MILLISECONDS);
            } catch (IllegalStateException | RejectedExecutionException e) {
                // keeper fechado durante uma renovação
                finish();
            }
        }

        private void renew() {
            synchronized (this) {
                if (done) {
                    return;
                }
            }
            try {
                extender.extend(message, lease);
            } catch (RuntimeException e) {
                lost = true;
                close();
                return;
            }
            schedule(clock.instant().plus(lease));
        }

        @Override
        public boolean lost() {
            return lost;
        }

        @Override
        public synchronized void close() {
            if (next != null) {
                next.cancel(false);
            }
            finish();
        }

        private void finish() {
            done = true;
            active.remove(key(message), this);
        }
    }
}
