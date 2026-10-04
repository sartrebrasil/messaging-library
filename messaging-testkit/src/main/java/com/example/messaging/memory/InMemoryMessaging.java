package com.example.messaging.memory;

import com.example.messaging.Capabilities;
import com.example.messaging.DeadLetterInfo;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.KeepAlive;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.SendResult;
import com.example.messaging.spi.LeaseKeeper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Filas e tópicos em memória para testes, com lease, reentrega, atraso, ordem por chave e DLQ.
 * Uma subscription é uma fila inscrita num tópico, como SNS → SQS.
 *
 * <pre>{@code
 * InMemoryMessaging broker = new InMemoryMessaging();
 * broker.createQueue("pedidos", InMemoryMessaging.QueueOptions.defaults());
 * MessageSender sender = broker.queueSender("pedidos");
 * MessageReceiver receiver = broker.receiver("pedidos");
 * }</pre>
 *
 * <p>Senders e receivers não validam o destino na criação: fila ou tópico inexistente lança
 * {@link DestinationNotFoundException} no uso, como nos provedores.</p>
 */
public final class InMemoryMessaging {

    static final String PROVIDER = "memory";

    /**
     * @param lease           duração do lease de cada entrega
     * @param ordered         entrega em ordem por {@code orderingKey}, que passa a ser obrigatória (como SQS FIFO)
     * @param deadLetterQueue fila de destino de {@code deadLetter}; {@code null} sem DLQ
     * @param maxMessageBytes limite de corpo + atributos
     */
    public record QueueOptions(Duration lease, boolean ordered, String deadLetterQueue, long maxMessageBytes) {

        public static QueueOptions defaults() {
            return new QueueOptions(Duration.ofSeconds(30), false, null, 256 * 1024);
        }

        public QueueOptions withLease(Duration newLease) {
            return new QueueOptions(newLease, ordered, deadLetterQueue, maxMessageBytes);
        }

        public QueueOptions withOrdered(boolean newOrdered) {
            return new QueueOptions(lease, newOrdered, deadLetterQueue, maxMessageBytes);
        }

        public QueueOptions withDeadLetterQueue(String queue) {
            return new QueueOptions(lease, ordered, queue, maxMessageBytes);
        }

        public QueueOptions withMaxMessageBytes(long bytes) {
            return new QueueOptions(lease, ordered, deadLetterQueue, bytes);
        }
    }

    static final Duration MAX_REDELIVERY_DELAY = Duration.ofHours(12);
    private static final long TOPIC_MAX_MESSAGE_BYTES = 256 * 1024;

    private final Map<String, Queue> queues = new ConcurrentHashMap<>();
    private final Map<String, List<String>> topics = new ConcurrentHashMap<>();

    public void createQueue(String name, QueueOptions options) {
        queues.put(name, new Queue(name, options));
    }

    public void createTopic(String name) {
        topics.putIfAbsent(name, new CopyOnWriteArrayList<>());
    }

    /** Inscreve uma fila existente num tópico existente. */
    public void subscribe(String topic, String queue) {
        queue(queue);
        List<String> subscribers = topics.get(topic);
        if (subscribers == null) {
            throw new DestinationNotFoundException(PROVIDER, "Tópico não encontrado: " + topic, null);
        }
        subscribers.add(queue);
    }

    public void deleteQueue(String name) {
        queues.remove(name);
    }

    /** Mensagens na fila, entregues ou não; para asserções. */
    public int size(String queue) {
        return queue(queue).size();
    }

    public MessageSender queueSender(String queue) {
        return new Sender(() -> {
            Queue target = queue(queue);
            return new Target(List.of(target), target.options.ordered(), target.options.maxMessageBytes());
        });
    }

    public MessageSender topicSender(String topic) {
        return new Sender(() -> {
            List<String> subscribers = topics.get(topic);
            if (subscribers == null) {
                throw new DestinationNotFoundException(PROVIDER, "Tópico não encontrado: " + topic, null);
            }
            return new Target(subscribers.stream().map(this::queue).toList(), false, TOPIC_MAX_MESSAGE_BYTES);
        });
    }

    /** Receiver novo a cada chamada. */
    public MessageReceiver receiver(String queue) {
        return new Receiver(queue);
    }

    private Queue queue(String name) {
        Queue queue = queues.get(name);
        if (queue == null) {
            throw new DestinationNotFoundException(PROVIDER, "Fila não encontrada: " + name, null);
        }
        return queue;
    }

    static long sizeOf(OutgoingMessage message) {
        long size = message.body().length;
        for (Map.Entry<String, String> attribute : message.attributes().entrySet()) {
            size += attribute.getKey().length() + attribute.getValue().length();
        }
        return size;
    }

    private record Target(List<Queue> queues, boolean ordered, long maxMessageBytes) {
    }

    private interface TargetResolver {
        Target resolve();
    }

    private static final class Entry {
        final String id = UUID.randomUUID().toString();
        final OutgoingMessage message;
        final Instant enqueuedAt = Instant.now();
        final DeadLetterInfo deadLetter;
        Instant visibleAt;
        int deliveries;
        long token;

        Entry(OutgoingMessage message, DeadLetterInfo deadLetter) {
            this.message = message;
            this.deadLetter = deadLetter;
            this.visibleAt = enqueuedAt;
        }
    }

    /** Handle opaco: dono, entrada e o token da entrega, que muda a cada receive. */
    private record Handle(Receiver owner, Entry entry, long token) {
    }

    private static final class Queue {

        final String name;
        final QueueOptions options;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private final List<Entry> entries = new ArrayList<>();
        private long tokens;

        Queue(String name, QueueOptions options) {
            this.name = name;
            this.options = Objects.requireNonNull(options, "options");
        }

        void add(Entry entry) {
            lock.lock();
            try {
                entries.add(entry);
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }

        int size() {
            lock.lock();
            try {
                return entries.size();
            } finally {
                lock.unlock();
            }
        }

        List<Handle> take(Receiver owner, int max, Duration maxWait) throws InterruptedException {
            long deadline = System.nanoTime() + maxWait.toNanos();
            lock.lock();
            try {
                while (true) {
                    Instant now = Instant.now();
                    List<Handle> taken = new ArrayList<>();
                    Set<String> blockedKeys = new HashSet<>();
                    Instant nextVisible = null;
                    for (Entry entry : entries) {
                        String key = entry.message.orderingKey();
                        boolean visible = !entry.visibleAt.isAfter(now);
                        if (options.ordered() && key != null && blockedKeys.contains(key)) {
                            continue;
                        }
                        if (!visible) {
                            if (options.ordered() && key != null) {
                                blockedKeys.add(key);
                            }
                            nextVisible = nextVisible == null || entry.visibleAt.isBefore(nextVisible)
                                    ? entry.visibleAt : nextVisible;
                            continue;
                        }
                        if (taken.size() < max) {
                            entry.deliveries++;
                            entry.token = ++tokens;
                            entry.visibleAt = now.plus(options.lease());
                            taken.add(new Handle(owner, entry, entry.token));
                        }
                    }
                    if (!taken.isEmpty()) {
                        return taken;
                    }
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return List.of();
                    }
                    if (nextVisible != null) {
                        remaining = Math.min(remaining, Math.max(1, Duration.between(now, nextVisible).toNanos()));
                    }
                    changed.awaitNanos(remaining);
                }
            } finally {
                lock.unlock();
            }
        }

        /** Executa {@code action} se a entrega ainda tem o lease; senão lança {@link LeaseExpiredException}. */
        void withLease(Handle handle, Runnable action) {
            lock.lock();
            try {
                Entry entry = handle.entry();
                if (entry.token != handle.token() || !entries.contains(entry) || !entry.visibleAt.isAfter(Instant.now())) {
                    throw new LeaseExpiredException(PROVIDER, "Lease vencido ou perdido: " + entry.id, null);
                }
                action.run();
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }

        void remove(Entry entry) {
            entries.remove(entry);
        }
    }

    private static final class Sender implements MessageSender {

        private final TargetResolver resolver;
        private volatile boolean closed;

        Sender(TargetResolver resolver) {
            this.resolver = resolver;
        }

        @Override
        public SendResult send(OutgoingMessage message) {
            ensureOpen(closed);
            Target target = resolver.resolve();
            if (target.ordered() && message.orderingKey() == null) {
                throw new IllegalArgumentException("Fila ordenada exige orderingKey");
            }
            long size = sizeOf(message);
            if (size > target.maxMessageBytes()) {
                throw new MessageTooLargeException(PROVIDER, "Mensagem com " + size + " bytes; limite "
                        + target.maxMessageBytes(), null);
            }
            String id = null;
            for (Queue queue : target.queues()) {
                Entry entry = new Entry(message, message.deadLetter());
                queue.add(entry);
                id = id == null ? entry.id : id;
            }
            return new SendResult(id == null ? UUID.randomUUID().toString() : id);
        }

        @Override
        public void checkAccess() {
            ensureOpen(closed);
            resolver.resolve();
        }

        @Override
        public Capabilities capabilities() {
            Target target = resolver.resolve();
            return new Capabilities(target.maxMessageBytes(), 10, false, Duration.ZERO, false,
                    target.ordered(), false, false);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final class Receiver implements MessageReceiver {

        private final String queueName;
        private final LeaseKeeper keeper = new LeaseKeeper(this::extendLease);
        private volatile boolean closed;

        Receiver(String queueName) {
            this.queueName = queueName;
        }

        @Override
        public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
            ensureOpen(closed);
            if (maxMessages < 1) {
                throw new IllegalArgumentException("maxMessages precisa ser >= 1: " + maxMessages);
            }
            if (maxWait.isNegative()) {
                throw new IllegalArgumentException("maxWait negativo: " + maxWait);
            }
            Queue queue = queue(queueName);
            try {
                Instant receivedAt = Instant.now();
                return queue.take(this, Math.min(maxMessages, 10), maxWait).stream()
                        .map(handle -> toReceived(handle, receivedAt))
                        .toList();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            }
        }

        private ReceivedMessage toReceived(Handle handle, Instant receivedAt) {
            Entry entry = handle.entry();
            OutgoingMessage message = entry.message;
            return new ReceivedMessage(entry.id, message.body(), message.attributes(), message.contentType(),
                    message.orderingKey(), OptionalInt.of(entry.deliveries), entry.enqueuedAt, receivedAt,
                    entry.visibleAt, message.traceparent(), entry.deadLetter, handle);
        }

        @Override
        public void ack(ReceivedMessage message) {
            Handle handle = own(message);
            keeper.release(message);
            Queue queue = queue(queueName);
            queue.withLease(handle, () -> queue.remove(handle.entry()));
        }

        @Override
        public void nack(ReceivedMessage message, Duration redeliverAfter) {
            Handle handle = own(message);
            if (redeliverAfter.isNegative() || redeliverAfter.compareTo(MAX_REDELIVERY_DELAY) > 0) {
                throw new IllegalArgumentException("Atraso fora de 0.." + MAX_REDELIVERY_DELAY + ": " + redeliverAfter);
            }
            keeper.release(message);
            queue(queueName).withLease(handle, () -> handle.entry().visibleAt = Instant.now().plus(redeliverAfter));
        }

        @Override
        public void extendLease(ReceivedMessage message, Duration lease) {
            Handle handle = own(message);
            queue(queueName).withLease(handle, () -> handle.entry().visibleAt = Instant.now().plus(lease));
        }

        @Override
        public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
            own(message);
            return keeper.keep(message, maxTotal);
        }

        @Override
        public void deadLetter(ReceivedMessage message, String reason) {
            Handle handle = own(message);
            Queue queue = queue(queueName);
            if (queue.options.deadLetterQueue() == null) {
                throw new UnsupportedOperationException("Fila " + queueName + " sem DLQ configurada");
            }
            Queue dlq = queue(queue.options.deadLetterQueue());
            keeper.release(message);
            queue.withLease(handle, () -> {
                queue.remove(handle.entry());
                dlq.add(new Entry(handle.entry().message, new DeadLetterInfo(handle.entry().id, reason)));
            });
        }

        @Override
        public void checkAccess() {
            ensureOpen(closed);
            queue(queueName);
        }

        @Override
        public Capabilities capabilities() {
            QueueOptions options = queue(queueName).options;
            return new Capabilities(options.maxMessageBytes(), 10, true, MAX_REDELIVERY_DELAY,
                    options.deadLetterQueue() != null, options.ordered(), false, true);
        }

        @Override
        public void close() {
            closed = true;
            keeper.close();
        }

        private Handle own(ReceivedMessage message) {
            ensureOpen(closed);
            if (!(message.handle() instanceof Handle handle) || handle.owner() != this) {
                throw new IllegalArgumentException("Mensagem recebida por outro receiver: " + message.messageId());
            }
            return handle;
        }
    }

    private static void ensureOpen(boolean closed) {
        if (closed) {
            throw new IllegalStateException("Fechado");
        }
    }
}
