package com.example.messaging.azure;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.example.messaging.Capabilities;
import com.example.messaging.KeepAlive;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessagingException;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.LeaseKeeper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Recebimento de uma fila ou subscription do Service Bus com sessions (ADR-0006): uma session por
 * instância, em ordem dentro dela. Paralelismo entre sessions = mais instâncias.
 *
 * <ul>
 *   <li>Sem session ativa, {@code receive} pede a próxima ({@code acceptNextSession}) e espera até
 *       {@code maxWait}. O SDK bloqueia até o {@code tryTimeout} do cliente quando não há session;
 *       o pedido continua em andamento e o próximo {@code receive} o aproveita.</li>
 *   <li>Quando um {@code receive} volta vazio e não há mensagem pendente de confirmação, a session é
 *       liberada e o próximo {@code receive} aceita outra.</li>
 *   <li>O lease é o lock da session: {@code extendLease} e {@code keepAlive} renovam a session
 *       inteira. Lock perdido descarta a session.</li>
 *   <li>{@code nack} é sempre {@code abandon}: a mensagem volta para o início da session e a ordem
 *       se mantém; não há atraso.</li>
 *   <li>Chamadas de {@code receive} na mesma instância são serializadas.</li>
 * </ul>
 */
public final class ServiceBusSessionMessageReceiver implements MessageReceiver {

    private static final Executor ACCEPT = Executors.newVirtualThreadPerTaskExecutor();

    private final ServiceBusSessionReceiverClient sessions;
    private final ServiceBusReceiverClient peekClient;
    private final long maxMessageBytes;
    private final boolean ownsClients;
    private final LeaseKeeper keeper = new LeaseKeeper(this::extendLease);
    private final ReentrantLock receiveLock = new ReentrantLock();
    private Session current;
    private CompletableFuture<ServiceBusReceiverClient> pendingAccept;
    private volatile boolean closed;

    /**
     * @param peekClient receiver comum da mesma entidade, usado só por {@link #checkAccess()};
     *                   {@code null} faz {@code checkAccess} lançar {@link UnsupportedOperationException}
     */
    public ServiceBusSessionMessageReceiver(ServiceBusSessionReceiverClient sessions,
                                            ServiceBusReceiverClient peekClient, long maxMessageBytes) {
        this(sessions, peekClient, maxMessageBytes, false);
    }

    private ServiceBusSessionMessageReceiver(ServiceBusSessionReceiverClient sessions,
                                             ServiceBusReceiverClient peekClient, long maxMessageBytes,
                                             boolean ownsClients) {
        this.sessions = sessions;
        this.peekClient = peekClient;
        this.maxMessageBytes = maxMessageBytes;
        this.ownsClients = ownsClients;
    }

    public static ServiceBusSessionMessageReceiver forQueue(ServiceBusClientBuilder builder, String queue,
                                                            long maxMessageBytes) {
        return new ServiceBusSessionMessageReceiver(
                configure(builder.sessionReceiver().queueName(queue)).buildClient(),
                ServiceBusMessageReceiver.configure(builder.receiver().queueName(queue)).buildClient(),
                maxMessageBytes, true);
    }

    public static ServiceBusSessionMessageReceiver forSubscription(ServiceBusClientBuilder builder, String topic,
                                                                   String subscription, long maxMessageBytes) {
        return new ServiceBusSessionMessageReceiver(
                configure(builder.sessionReceiver().topicName(topic).subscriptionName(subscription)).buildClient(),
                ServiceBusMessageReceiver.configure(builder.receiver().topicName(topic)
                        .subscriptionName(subscription)).buildClient(),
                maxMessageBytes, true);
    }

    private static ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder configure(
            ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder builder) {
        return builder.receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                .disableAutoComplete()
                .maxAutoLockRenewDuration(Duration.ZERO)
                .prefetchCount(0);
    }

    /** Session aceita e quantas mensagens dela ainda não foram confirmadas. */
    private static final class Session {
        final ServiceBusReceiverClient client;
        final AtomicInteger outstanding = new AtomicInteger();

        Session(ServiceBusReceiverClient client) {
            this.client = client;
        }
    }

    private record Handle(ServiceBusSessionMessageReceiver owner, Session session, ServiceBusReceivedMessage message) {
    }

    @Override
    public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
        ensureOpen();
        ServiceBusMessageReceiver.validate(maxMessages, maxWait);
        receiveLock.lock();
        try {
            Session session = current != null ? current : acceptNext(maxWait);
            if (session == null) {
                return List.of();
            }
            Instant receivedAt = Instant.now();
            List<ReceivedMessage> result = new ArrayList<>();
            try {
                for (ServiceBusReceivedMessage message : session.client.receiveMessages(maxMessages,
                        ServiceBusMessageReceiver.positive(maxWait))) {
                    session.outstanding.incrementAndGet();
                    result.add(ServiceBusCodec.decode(message, receivedAt, new Handle(this, session, message)));
                }
            } catch (RuntimeException e) {
                MessagingException mapped = AzureErrors.map("receiveMessages (session)", e);
                if (mapped instanceof LeaseExpiredException) {
                    drop(session);
                }
                throw mapped;
            }
            if (result.isEmpty() && session.outstanding.get() == 0) {
                drop(session);
            }
            return result;
        } finally {
            receiveLock.unlock();
        }
    }

    private Session acceptNext(Duration maxWait) {
        if (pendingAccept == null) {
            pendingAccept = CompletableFuture.supplyAsync(sessions::acceptNextSession, ACCEPT);
        }
        try {
            ServiceBusReceiverClient client = pendingAccept.get(maxWait.toNanos(), TimeUnit.NANOSECONDS);
            pendingAccept = null;
            current = new Session(client);
            return current;
        } catch (TimeoutException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            pendingAccept = null;
            // nenhuma session disponível no tryTimeout do cliente: o SDK lança IllegalStateException
            if (e.getCause() instanceof IllegalStateException) {
                return null;
            }
            throw e.getCause() instanceof RuntimeException runtime
                    ? AzureErrors.map("acceptNextSession", runtime)
                    : new MessagingException(AzureErrors.PROVIDER, "acceptNextSession", e.getCause(), false);
        }
    }

    private void drop(Session session) {
        if (current == session) {
            current = null;
        }
        try {
            session.client.close();
        } catch (RuntimeException ignored) {
            // a session já pode estar perdida
        }
    }

    @Override
    public void ack(ReceivedMessage message) {
        Handle handle = own(message);
        keeper.release(message);
        settle(handle, "complete", () -> handle.session().client.complete(handle.message()));
    }

    /** Sempre {@code abandon}; o atraso é ignorado para manter a ordem da session. */
    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        Handle handle = own(message);
        if (redeliverAfter.isNegative()) {
            throw new IllegalArgumentException("Atraso negativo: " + redeliverAfter);
        }
        keeper.release(message);
        settle(handle, "abandon", () -> handle.session().client.abandon(handle.message()));
    }

    /** {@code renewSessionLock}: renova a session inteira pelo lock duration da entidade. */
    @Override
    public void extendLease(ReceivedMessage message, Duration lease) {
        Handle handle = own(message);
        try {
            handle.session().client.renewSessionLock();
        } catch (RuntimeException e) {
            throw lost(handle.session(), AzureErrors.map("renewSessionLock", e));
        }
    }

    @Override
    public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
        own(message);
        return keeper.keep(message, maxTotal);
    }

    @Override
    public void deadLetter(ReceivedMessage message, String reason) {
        Handle handle = own(message);
        keeper.release(message);
        settle(handle, "deadLetter", () -> handle.session().client.deadLetter(handle.message(),
                new DeadLetterOptions().setDeadLetterReason(reason)));
    }

    private void settle(Handle handle, String operation, Runnable action) {
        try {
            action.run();
            handle.session().outstanding.decrementAndGet();
        } catch (RuntimeException e) {
            throw lost(handle.session(), AzureErrors.map(operation, e));
        }
    }

    private MessagingException lost(Session session, MessagingException mapped) {
        if (mapped instanceof LeaseExpiredException) {
            receiveLock.lock();
            try {
                drop(session);
            } finally {
                receiveLock.unlock();
            }
        }
        return mapped;
    }

    /** {@code peekMessage} no receiver comum da mesma entidade: sem lock e sem efeito (exige Listen). */
    @Override
    public void checkAccess() {
        ensureOpen();
        if (peekClient == null) {
            throw new UnsupportedOperationException("Sem receiver de peek para checkAccess");
        }
        try {
            peekClient.peekMessage();
        } catch (RuntimeException e) {
            throw AzureErrors.map("peekMessage " + peekClient.getEntityPath(), e);
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(maxMessageBytes, 100, false, Duration.ZERO, true, true, true, true);
    }

    @Override
    public void close() {
        closed = true;
        keeper.close();
        receiveLock.lock();
        try {
            if (current != null) {
                drop(current);
            }
            if (pendingAccept != null) {
                // a session aceita depois do close é liberada assim que chegar
                pendingAccept.thenAccept(ServiceBusReceiverClient::close);
            }
        } finally {
            receiveLock.unlock();
        }
        if (ownsClients) {
            sessions.close();
            if (peekClient != null) {
                peekClient.close();
            }
        }
    }

    private Handle own(ReceivedMessage message) {
        ensureOpen();
        if (!(message.handle() instanceof Handle handle) || handle.owner() != this) {
            throw new IllegalArgumentException("Mensagem recebida por outro receiver: " + message.messageId());
        }
        return handle;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ServiceBusSessionMessageReceiver fechado");
        }
    }
}
