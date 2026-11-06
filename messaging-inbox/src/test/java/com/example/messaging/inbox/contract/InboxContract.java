package com.example.messaging.inbox.contract;

import com.example.messaging.Capabilities;
import com.example.messaging.KeepAlive;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.ConsumerSettings;
import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Delivery;
import com.example.messaging.inbox.Destinations;
import com.example.messaging.inbox.EnvelopeMode;
import com.example.messaging.inbox.EnvelopeReader;
import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.InboxConsumer;
import com.example.messaging.inbox.InboxResultHandler;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.inbox.Pipeline;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.StopSignal;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Semântica da inbox sobre um broker: um {@link InboxConsumer} real, sem Spring, sobre os senders e receivers da
 * subclasse (F3, spec {@code 2026-11-06-f3-contrato-inbox-design}). Estenda apontando os métodos para um backend real
 * ou emulado; casos de capability ausente são pulados.
 *
 * <p>Cada teste marca as próprias mensagens com {@value #RUN_ATTRIBUTE}; as de outros testes são confirmadas e
 * ignoradas. Os destinos são drenados antes de cada teste.</p>
 */
@TestMethodOrder(MethodOrderer.Random.class)
public abstract class InboxContract {

    public static final String RUN_ATTRIBUTE = "contract_inbox_run";
    /** Entregas que o broker vê no {@code broker-delay} com os padrões da lib: 1 + 5 do nível 2. */
    public static final int DEFAULT_BROKER_DELAY_DELIVERIES = 6;
    private static final int NATIVE_LIMIT_CEILING = 30;

    private final String run = UUID.randomUUID().toString();
    private final List<AutoCloseable> opened = new ArrayList<>();
    /** Entregas desta execução vistas pelo consumer, na ordem do receive. */
    private final List<ReceivedMessage> delivered = new CopyOnWriteArrayList<>();
    private InboxConsumer consumer;

    /** Envia ao destino principal. */
    protected abstract MessageSender newSender();

    /** Recebe do destino principal; instância nova a cada chamada. */
    protected abstract MessageReceiver newReceiver();

    protected abstract MessageSender failedSender();

    protected abstract MessageReceiver failedReceiver();

    protected abstract MessageSender deadLetterSender();

    protected abstract MessageReceiver deadLetterReceiver();

    /** Lease do destino principal. */
    protected abstract Duration lease();

    /** Bandas do {@code retry-queues}; {@code null} pula o caso. */
    protected List<RetryQueue> retryQueues() {
        return null;
    }

    /** Recebe da banda de {@link #retryQueues()}. */
    protected MessageReceiver retryQueueReceiver() {
        return null;
    }

    /** Entregas até o broker tirar a mensagem sozinho; {@code null}: sem limite padrão (caso pulado). */
    protected Integer nativeDeliveryLimit() {
        return null;
    }

    /** O atraso do nível 2 é por cópia ({@code RESCHEDULE}, {@code SCHEDULED}): a contagem nativa recomeça. */
    protected boolean delayCopies() {
        return false;
    }

    /**
     * O receiver informa {@code deliveryCount}. Sem ele, o {@code broker-delay} não conta as tentativas do nível 2 e
     * manda à DLQ na primeira (o Pub/Sub sem dead-letter policy); o caso de {@code broker-delay} é pulado.
     */
    protected boolean reportsDeliveryCount() {
        return true;
    }

    /** Atraso do nível 2 nos casos de {@code broker-delay}; o mínimo que o broker aceita. */
    protected Duration delayedInitial() {
        return Duration.ofSeconds(1);
    }

    /** {@code maxWait} de cada {@code receive} do contrato e o {@code poll-wait} do consumer. */
    protected Duration receiveWait() {
        return Duration.ofSeconds(1);
    }

    /** Teto das esperas. */
    protected Duration settle() {
        return Duration.ofSeconds(15);
    }

    @BeforeEach
    void drain() {
        drain(newReceiver());
        drain(failedReceiver());
        drain(deadLetterReceiver());
        if (retryQueueReceiver() != null) {
            drain(retryQueueReceiver());
        }
    }

    @AfterEach
    void closeAll() {
        if (consumer != null) {
            consumer.stop();
        }
        for (int i = opened.size() - 1; i >= 0; i--) {
            try {
                opened.get(i).close();
            } catch (Exception ignored) {
                // o próximo teste drena o que sobrou
            }
        }
    }

    @Test
    void successIsAcked() {
        AtomicInteger calls = new AtomicInteger();
        start(DelayedStrategy.AUTO, immediatePolicy(), 1, m -> {
            calls.incrementAndGet();
            return HandlerResult.ack();
        });
        send("ok");

        await(() -> calls.get() == 1);
        consumer.stop();
        assertNothingFor(newReceiver(), lease().plus(receiveWait()));
        assertNothingFor(failedReceiver(), receiveWait());
        assertNothingFor(deadLetterReceiver(), receiveWait());
        assertEquals(1, delivered.size());
    }

    @Test
    void immediateRetryThenSuccess() {
        List<Delivery> levels = new CopyOnWriteArrayList<>();
        start(DelayedStrategy.AUTO, immediatePolicy(), 1, m -> {
            levels.add(m.message().delivery());
            if (levels.size() < 3) {
                throw new Transient();
            }
            return HandlerResult.ack();
        });
        send("flaky");

        await(() -> levels.size() == 3);
        consumer.stop();
        assertTrue(levels.stream().allMatch(d -> d.level() == Delivery.Level.IMMEDIATE), levels.toString());
        assertEquals(1, delivered.size(), "nível 1 não volta ao broker");
        assertNothingFor(failedReceiver(), receiveWait());
        assertNothingFor(deadLetterReceiver(), receiveWait());
    }

    @Test
    void nonRetryableGoesToFailed() {
        AtomicInteger calls = new AtomicInteger();
        start(DelayedStrategy.AUTO, immediatePolicy(), 1, m -> {
            calls.incrementAndGet();
            throw new IllegalArgumentException("invalid order");
        });
        String id = send("invalid");

        ReceivedMessage copy = copies(failedReceiver(), 1).getFirst();
        assertEquals(1, calls.get());
        assertTrue(copy.attributes().get("inbox_failure").contains("exception=java.lang.IllegalArgumentException"),
                copy.attributes().toString());
        assertEquals("java.lang.IllegalArgumentException: invalid order", copy.deadLetter().reason());
        assertEquals(id, copy.deadLetter().originalMessageId());
        consumer.stop();
        assertNothingFor(newReceiver(), lease().plus(receiveWait()));
    }

    @Test
    void rejectCarriesTheReason() {
        start(DelayedStrategy.AUTO, immediatePolicy(), 1, m -> HandlerResult.reject("not today"));
        send("rejected");

        ReceivedMessage copy = copies(failedReceiver(), 1).getFirst();
        assertEquals("rejected: not today", copy.deadLetter().reason());
    }

    @Test
    void brokerDelayThenDeadLetter() {
        assumeTrue(capabilities().delayedRedelivery(), "o receiver não tem atraso no nack");
        assumeTrue(reportsDeliveryCount(), "sem deliveryCount o broker-delay não conta o nível 2");
        AtomicInteger calls = new AtomicInteger();
        start(DelayedStrategy.BROKER_DELAY, delayedPolicy(delayedInitial()), 1, m -> {
            calls.incrementAndGet();
            throw new Transient();
        });
        send("delayed");

        ReceivedMessage copy = copies(deadLetterReceiver(), 1).getFirst();
        assertEquals(3, calls.get());
        assertEquals("l2=2;l3=0", copy.attributes().get("inbox_attempts"));
        assertEquals(3, delivered.size(), "o nível 2 volta ao broker a cada tentativa");
    }

    @Test
    void holdRetriesWithoutReturning() {
        AtomicInteger calls = new AtomicInteger();
        // espera do nível 2 igual ao lease: sem keepAlive, a mensagem voltaria ao broker
        start(DelayedStrategy.HOLD, delayedPolicy(lease()), 1, m -> {
            calls.incrementAndGet();
            throw new Transient();
        });
        send("held");

        ReceivedMessage copy = copies(deadLetterReceiver(), 1).getFirst();
        assertEquals(3, calls.get());
        assertEquals("l2=2;l3=0", copy.attributes().get("inbox_attempts"));
        assertEquals(1, delivered.size(), "o hold não devolve ao broker");
    }

    @Test
    void retryQueuesCopyAndAck() {
        assumeTrue(retryQueues() != null, "sem bandas de retry");
        start(DelayedStrategy.RETRY_QUEUES, delayedPolicy(retryQueues().getFirst().delay()), 1, m -> {
            throw new Transient();
        });
        send("banded");

        ReceivedMessage copy = copies(retryQueueReceiver(), 1).getFirst();
        assertEquals("l2=1;l3=0", copy.attributes().get("inbox_attempts"));
        assertNull(copy.attributes().get("inbox_failure"));
        consumer.stop();
        assertNothingFor(newReceiver(), lease().plus(receiveWait()));
    }

    @Test
    void slowHandlerKeepsTheLease() {
        AtomicInteger calls = new AtomicInteger();
        Duration work = lease().multipliedBy(3).dividedBy(2);
        start(DelayedStrategy.AUTO, immediatePolicy(), 1, m -> {
            Thread.sleep(work.toMillis());
            calls.incrementAndGet();
            return HandlerResult.ack();
        });
        send("slow");

        await(() -> calls.get() == 1);
        consumer.stop();
        assertNothingFor(newReceiver(), lease().plus(receiveWait()));
        assertEquals(1, delivered.size(), "o lease foi mantido");
    }

    @Test
    void stopReleasesUnstarted() {
        List<String> handled = new CopyOnWriteArrayList<>();
        send("a");
        send("b");
        send("c");
        start(DelayedStrategy.AUTO, immediatePolicy(), 3, m -> {
            Thread.sleep(1000);
            handled.add(m.payload());
            return HandlerResult.ack();
        });
        await(() -> !delivered.isEmpty());

        consumer.stop();

        // as do poll que não começaram voltam já; as que nem foram recebidas continuam lá
        Set<String> back = copies(newReceiver(), 3 - handled.size()).stream()
                .map(ReceivedMessage::bodyAsString).collect(Collectors.toSet());
        Set<String> expected = new java.util.HashSet<>(Set.of("a", "b", "c"));
        handled.forEach(expected::remove);
        assertEquals(expected, back);
    }

    @Test
    void nativeDeliveryLimitIsWhatTheBrokerDoes() {
        Integer limit = nativeDeliveryLimit();
        assumeTrue(limit != null, "sem limite nativo padrão");
        send("poison");
        MessageReceiver receiver = open(newReceiver());

        int deliveries = 0;
        long quietUntil = deadline(settle().dividedBy(3));
        while (System.nanoTime() < quietUntil) {
            List<ReceivedMessage> mine = ours(receiver, receiver.receive(1, receiveWait()));
            if (mine.isEmpty()) {
                continue;
            }
            if (++deliveries > NATIVE_LIMIT_CEILING) {
                fail("teto de " + NATIVE_LIMIT_CEILING + " entregas atingido");
            }
            receiver.nack(mine.getFirst(), Duration.ZERO);
            quietUntil = deadline(settle().dividedBy(3));
        }
        assertEquals(limit, deliveries, "entregas até o broker tirar a mensagem");
        if (!delayCopies()) {
            assertTrue(limit > DEFAULT_BROKER_DELAY_DELIVERIES,
                    "limite nativo " + limit + " não cobre o broker-delay padrão da inbox");
        }
    }

    // --- helpers

    /** Falha que o nível 1 e o nível 2 repetem. */
    static final class Transient extends RuntimeException {
        Transient() {
            super("transient");
        }
    }

    private static RetryPolicy immediatePolicy() {
        return new RetryPolicy(new BackoffSpec(2, Duration.ofMillis(10), 1.0, Duration.ofMillis(10), 0),
                BackoffSpec.none(), true, 3, List.of(IllegalArgumentException.class), List.of(), List.of());
    }

    private static RetryPolicy delayedPolicy(Duration delay) {
        return new RetryPolicy(BackoffSpec.none(), new BackoffSpec(2, delay, 1.0, delay, 0), true, 3,
                List.of(IllegalArgumentException.class), List.of(), List.of());
    }

    private Capabilities capabilities() {
        return open(newReceiver()).capabilities();
    }

    private void start(DelayedStrategy strategy, RetryPolicy policy, int maxPerPoll,
                       InboxResultHandler<String> handler) {
        ConsumerSettings settings = new ConsumerSettings("contract", "inbox-main", 1, maxPerPoll, receiveWait(),
                lease().multipliedBy(6), Duration.ofSeconds(5), Ordering.RELAXED, strategy, true);
        StopSignal stop = new StopSignal();
        InboxResultHandler<String> mine = m -> run.equals(m.message().headers().get(RUN_ATTRIBUTE))
                ? handler.handle(m) : HandlerResult.ack();
        Pipeline<String> pipeline = new Pipeline<>(settings.consumer(), new EnvelopeReader(EnvelopeMode.AUTO),
                (m, t) -> m.payloadAsString(), String.class, mine, policy, stop, () -> 0.0);
        List<RetryQueue> bands = strategy == DelayedStrategy.RETRY_QUEUES ? retryQueues() : List.of();
        Destinations destinations = new Destinations(() -> new Recording(newReceiver()), failedSender(),
                deadLetterSender(), bands);
        consumer = new InboxConsumer(settings, pipeline, destinations, stop, Clock.systemUTC(), () -> 0.0);
        consumer.start();
    }

    private String send(String body) {
        return open(newSender()).send(OutgoingMessage.ofText(body).withAttribute(RUN_ATTRIBUTE, run)).messageId();
    }

    /** Espera {@code expected} mensagens desta execução no destino, confirma e devolve. */
    private List<ReceivedMessage> copies(MessageReceiver receiver, int expected) {
        MessageReceiver r = open(receiver);
        List<ReceivedMessage> found = new ArrayList<>();
        long until = deadline(settle());
        while (found.size() < expected && System.nanoTime() < until) {
            List<ReceivedMessage> mine = ours(r, r.receive(10, receiveWait()));
            mine.forEach(r::ack);
            found.addAll(mine);
        }
        assertEquals(expected, found.size(), "mensagens desta execução no destino");
        return found;
    }

    private void assertNothingFor(MessageReceiver receiver, Duration window) {
        MessageReceiver r = open(receiver);
        long until = deadline(window);
        while (System.nanoTime() < until) {
            List<ReceivedMessage> mine = ours(r, r.receive(10, receiveWait()));
            if (!mine.isEmpty()) {
                mine.forEach(r::ack);
                fail("mensagem inesperada: " + mine.getFirst().attributes());
            }
        }
    }

    /** As desta execução; as de outros testes recebem {@code ack} e somem. */
    private List<ReceivedMessage> ours(MessageReceiver receiver, List<ReceivedMessage> received) {
        List<ReceivedMessage> mine = new ArrayList<>();
        for (ReceivedMessage m : received) {
            if (run.equals(m.attributes().get(RUN_ATTRIBUTE))) {
                mine.add(m);
            } else {
                receiver.ack(m);
            }
        }
        return mine;
    }

    private void drain(MessageReceiver receiver) {
        MessageReceiver r = open(receiver);
        long until = deadline(settle().dividedBy(3));
        while (System.nanoTime() < until) {
            List<ReceivedMessage> stale = r.receive(10, receiveWait());
            if (stale.isEmpty()) {
                return;
            }
            stale.forEach(r::ack);
        }
    }

    private void await(BooleanSupplier condition) {
        long until = deadline(settle());
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > until) {
                fail("condição não atingida em " + settle());
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    private static long deadline(Duration d) {
        return System.nanoTime() + d.toNanos();
    }

    private <T extends AutoCloseable> T open(T resource) {
        opened.add(resource);
        return resource;
    }

    /** Registra as entregas desta execução que o consumer recebe. */
    private final class Recording implements MessageReceiver {
        private final MessageReceiver delegate;

        Recording(MessageReceiver delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
            List<ReceivedMessage> received = delegate.receive(maxMessages, maxWait);
            received.stream().filter(m -> run.equals(m.attributes().get(RUN_ATTRIBUTE))).forEach(delivered::add);
            return received;
        }

        @Override
        public void ack(ReceivedMessage message) {
            delegate.ack(message);
        }

        @Override
        public void nack(ReceivedMessage message, Duration redeliverAfter) {
            delegate.nack(message, redeliverAfter);
        }

        @Override
        public void extendLease(ReceivedMessage message, Duration lease) {
            delegate.extendLease(message, lease);
        }

        @Override
        public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
            return delegate.keepAlive(message, maxTotal);
        }

        @Override
        public void deadLetter(ReceivedMessage message, String reason) {
            delegate.deadLetter(message, reason);
        }

        @Override
        public void checkAccess() {
            delegate.checkAccess();
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
