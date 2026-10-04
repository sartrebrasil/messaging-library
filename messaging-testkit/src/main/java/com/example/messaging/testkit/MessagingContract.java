package com.example.messaging.testkit;

import com.example.messaging.AccessDeniedException;
import com.example.messaging.AckResult;
import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.KeepAlive;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.SendResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Comportamento que todo par {@link MessageSender}/{@link MessageReceiver} deve ter. Estenda esta
 * classe apontando os métodos {@code new*} para um backend real ou emulado.
 *
 * <p>Os métodos {@code new*} devolvem instâncias novas a cada chamada; o contrato fecha todas no
 * fim do teste. Cada teste marca as próprias mensagens com o atributo {@value #RUN_ATTRIBUTE}, e
 * mensagens de outros testes que aparecerem são confirmadas e descartadas, então o destino pode
 * ser compartilhado (os entities do Service Bus emulator são fixos, por exemplo).</p>
 *
 * <p>Testes de recursos opcionais (ordem, DLQ, atraso, destino inexistente) são pulados quando o
 * método correspondente devolve {@code null} ou a capability é {@code false}.</p>
 */
public abstract class MessagingContract {

    public static final String RUN_ATTRIBUTE = "contract_run";

    private final String run = UUID.randomUUID().toString();
    private final List<AutoCloseable> opened = new ArrayList<>();

    /** Sender de um destino cujas mensagens chegam em {@link #newReceiver()}. */
    protected abstract MessageSender newSender();

    protected abstract MessageReceiver newReceiver();

    /** Lease do destino de {@link #newReceiver()} (visibility timeout, lock duration, ack deadline). */
    protected abstract Duration lease();

    /** {@code maxWait} de cada {@code receive} do contrato. */
    protected Duration maxWait() {
        return Duration.ofSeconds(2);
    }

    /** Folga para a mensagem enviada aparecer e para um {@code receive} vazio voltar. */
    protected Duration settle() {
        return Duration.ofSeconds(10);
    }

    /** Par com ordem por {@code orderingKey} (SQS FIFO, Service Bus com sessions); {@code null} pula. */
    protected MessageSender newOrderedSender() {
        return null;
    }

    protected MessageReceiver newOrderedReceiver() {
        return null;
    }

    /** Receiver da DLQ de {@link #newReceiver()}; {@code null} quando o receiver não tem DLQ. */
    protected MessageReceiver newDeadLetterReceiver() {
        return null;
    }

    /** Sender de um destino inexistente; {@code null} pula. */
    protected MessageSender newMissingDestinationSender() {
        return null;
    }

    /** Receiver de um destino inexistente; {@code null} pula. */
    protected MessageReceiver newMissingDestinationReceiver() {
        return null;
    }

    @AfterEach
    void closeOpened() {
        for (AutoCloseable closeable : opened) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // fechar não deve derrubar o teste
            }
        }
    }

    // ---------------------------------------------------------------- envio e recebimento

    @Test
    void sendAndReceiveRoundTrip() {
        MessageSender sender = sender();
        MessageReceiver receiver = receiver();
        OutgoingMessage message = OutgoingMessage.ofText("olá, mundo")
                .withContentType("application/json")
                .withAttribute("tenant", "acme");

        SendResult result = sender.send(own(message));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();

        assertNotNull(result.messageId());
        assertFalse(received.messageId().isBlank());
        assertEquals("olá, mundo", received.bodyAsString());
        assertEquals("acme", received.attributes().get("tenant"));
        assertEquals("application/json", received.contentType());
        assertTrue(received.leaseExpiresAt().isAfter(received.receivedAt()));
        received.deliveryCount().ifPresent(count -> assertEquals(1, count));
        receiver.ack(received);
    }

    @Test
    void binaryBodyRoundTrip() {
        byte[] body = new byte[256];
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte) i;
        }
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.of(body).withContentType("application/octet-stream")));

        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();
        assertArrayEquals(body, received.body());
        receiver.ack(received);
    }

    @Test
    void binaryBodyWithoutContentTypeRoundTrip() {
        byte[] body = {(byte) 0xC3, (byte) 0x28, 0x00, (byte) 0xFF};
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.of(body)));

        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();
        assertArrayEquals(body, received.body());
        receiver.ack(received);
    }

    @Test
    void maxAttributesRoundTrip() {
        Map<String, String> attributes = new HashMap<>();
        for (int i = 0; i < OutgoingMessage.MAX_ATTRIBUTES - 1; i++) {
            attributes.put("attr_" + i, "valor \"" + i + "\" \\ fim");
        }
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x").withAttributes(attributes)));

        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();
        Map<String, String> expected = new HashMap<>(attributes);
        expected.put(RUN_ATTRIBUTE, run);
        assertEquals(expected, received.attributes());
        receiver.ack(received);
    }

    @Test
    void traceparentRoundTrip() {
        String traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x").withTraceparent(traceparent)));

        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();
        assertEquals(traceparent, received.traceparent());
        assertFalse(received.attributes().containsKey("traceparent"));
        receiver.ack(received);
    }

    @Test
    void emptyReceiveReturnsWithinMaxWait() {
        MessageReceiver receiver = receiver();
        long start = System.nanoTime();
        List<ReceivedMessage> messages = receiver.receive(10, maxWait());
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        ackForeign(receiver, messages);
        assertTrue(elapsed.compareTo(maxWait().plus(settle())) < 0, "receive demorou " + elapsed);
    }

    @Test
    void receiveRejectsInvalidArguments() {
        MessageReceiver receiver = receiver();
        assertThrows(IllegalArgumentException.class, () -> receiver.receive(0, maxWait()));
        assertThrows(IllegalArgumentException.class, () -> receiver.receive(1, Duration.ofSeconds(-1)));
    }

    @Test
    void sendAllAndAckAll() {
        MessageReceiver receiver = receiver();
        List<OutgoingMessage> batch = List.of(own(OutgoingMessage.ofText("1")), own(OutgoingMessage.ofText("2")),
                own(OutgoingMessage.ofText("3")));

        BatchSendResult sent = sender().sendAll(batch);
        assertTrue(sent.isSuccess(), () -> "falhas: " + sent.failures());
        assertEquals(3, sent.sent().size());

        List<ReceivedMessage> received = awaitOwn(receiver, 3);
        assertEquals(List.of("1", "2", "3"), received.stream().map(ReceivedMessage::bodyAsString).sorted().toList());
        AckResult acked = receiver.ackAll(received);
        assertTrue(acked.isSuccess(), () -> "falhas: " + acked.failures());
        assertNoneOwn(receiver(), lease().multipliedBy(3).dividedBy(2));
    }

    @Test
    void tooLargeMessageIsRejectedBeforeSending() {
        MessageSender sender = sender();
        long limit = sender.capabilities().maxMessageBytes();
        assumeTrue(limit < 64L * 1024 * 1024, "limite grande demais para alocar no teste");
        byte[] body = new byte[(int) limit + 1];

        assertThrows(MessageTooLargeException.class, () -> sender.send(own(OutgoingMessage.of(body))));
    }

    // ---------------------------------------------------------------- confirmação e lease

    @Test
    void ackedMessageIsNotRedelivered() {
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        receiver.ack(awaitOwn(receiver, 1).getFirst());

        assertNoneOwn(receiver(), lease().multipliedBy(3).dividedBy(2));
    }

    @Test
    void unackedMessageIsRedeliveredAfterLease() {
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage first = awaitOwn(receiver, 1).getFirst();

        ReceivedMessage second = awaitOwn(receiver(), 1, lease().plus(settle())).getFirst();
        assertEquals(first.messageId(), second.messageId());
        second.deliveryCount().ifPresent(count -> assertTrue(count >= 2, "deliveryCount " + count));
    }

    @Test
    void nackRedeliversImmediately() {
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage first = awaitOwn(receiver, 1).getFirst();
        receiver.nack(first, Duration.ZERO);

        ReceivedMessage second = awaitOwn(receiver, 1).getFirst();
        assertEquals(first.messageId(), second.messageId());
        second.deliveryCount().ifPresent(count -> assertEquals(2, count));
        receiver.ack(second);
    }

    @Test
    void nackWithDelayWaitsBeforeRedelivery() {
        MessageReceiver receiver = receiver();
        assumeTrue(receiver.capabilities().delayedRedelivery(), "adapter não respeita atraso no nack");
        Duration delay = lease();
        sender().send(own(OutgoingMessage.ofText("x")));
        receiver.nack(awaitOwn(receiver, 1).getFirst(), delay);

        assertNoneOwn(receiver, delay.dividedBy(2));
        receiver.ack(awaitOwn(receiver, 1, delay.plus(settle())).getFirst());
    }

    @Test
    void nackAboveMaximumDelayIsRejected() {
        MessageReceiver receiver = receiver();
        Capabilities capabilities = receiver.capabilities();
        assumeTrue(capabilities.delayedRedelivery(), "adapter não respeita atraso no nack");
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();

        assertThrows(IllegalArgumentException.class,
                () -> receiver.nack(received, capabilities.maxRedeliveryDelay().plusSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> receiver.nack(received, Duration.ofSeconds(-1)));
        receiver.ack(received);
    }

    @Test
    void extendLeaseKeepsMessageInvisible() {
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();
        receiver.extendLease(received, lease().multipliedBy(3));

        assertNoneOwn(receiver(), lease().multipliedBy(3).dividedBy(2));
        receiver.ack(received);
    }

    @Test
    void keepAliveHoldsLeaseUntilClosed() {
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();

        try (KeepAlive keepAlive = receiver.keepAlive(received, lease().multipliedBy(10))) {
            assertNoneOwn(receiver(), lease().multipliedBy(2));
            assertFalse(keepAlive.lost());
            receiver.ack(received);
        }
    }

    @Test
    void keepAliveStopsAtMaxTotal() {
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();
        receiver.keepAlive(received, lease());

        ReceivedMessage redelivered = awaitOwn(receiver(), 1, lease().multipliedBy(3).plus(settle())).getFirst();
        assertEquals(received.messageId(), redelivered.messageId());
    }

    @Test
    void keepAliveRejectsNonPositiveMaxTotal() {
        MessageReceiver receiver = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();

        assertThrows(IllegalArgumentException.class, () -> receiver.keepAlive(received, Duration.ZERO));
        receiver.ack(received);
    }

    @Test
    void ackAfterLeaseExpiredFails() throws InterruptedException {
        MessageReceiver receiver = receiver();
        assumeTrue(receiver.capabilities().reportsLeaseExpiredOnAck(), "provedor não informa lease vencido no ack");
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();
        Thread.sleep(lease().plusMillis(lease().toMillis() / 2 + 200).toMillis());

        assertThrows(LeaseExpiredException.class, () -> receiver.ack(received));
    }

    @Test
    void messageFromAnotherReceiverIsRejected() {
        MessageReceiver receiver = receiver();
        MessageReceiver other = receiver();
        sender().send(own(OutgoingMessage.ofText("x")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();

        assertThrows(IllegalArgumentException.class, () -> other.ack(received));
        assertThrows(IllegalArgumentException.class, () -> other.nack(received, Duration.ZERO));
        receiver.ack(received);
    }

    // ---------------------------------------------------------------- dead letter

    @Test
    void deadLetterMovesMessageToDeadLetterQueue() {
        MessageReceiver receiver = receiver();
        MessageReceiver deadLetters = track(newDeadLetterReceiver());
        sender().send(own(OutgoingMessage.ofText("veneno").withAttribute("tenant", "acme")));
        ReceivedMessage received = awaitOwn(receiver, 1).getFirst();

        if (deadLetters == null) {
            assertThrows(UnsupportedOperationException.class, () -> receiver.deadLetter(received, "motivo"));
            receiver.ack(received);
            return;
        }
        receiver.deadLetter(received, "motivo do teste");

        ReceivedMessage dead = awaitOwn(deadLetters, 1).getFirst();
        assertEquals("veneno", dead.bodyAsString());
        assertEquals("acme", dead.attributes().get("tenant"));
        assertNotNull(dead.deadLetter());
        assertEquals("motivo do teste", dead.deadLetter().reason());
        deadLetters.ack(dead);
        assertNoneOwn(receiver(), lease().multipliedBy(3).dividedBy(2));
    }

    // ---------------------------------------------------------------- ordem

    @Test
    void orderedDeliveryPerKey() {
        MessageSender sender = track(newOrderedSender());
        MessageReceiver receiver = track(newOrderedReceiver());
        assumeTrue(sender != null && receiver != null, "sem par ordenado");
        String keyA = "a-" + run;
        String keyB = "b-" + run;
        for (int i = 1; i <= 3; i++) {
            sender.send(own(OutgoingMessage.ofText("a" + i).withOrderingKey(keyA)));
            sender.send(own(OutgoingMessage.ofText("b" + i).withOrderingKey(keyB)));
        }

        Map<String, List<String>> byKey = new HashMap<>();
        long deadline = System.nanoTime() + settle().multipliedBy(3).toNanos();
        int total = 0;
        while (total < 6 && System.nanoTime() < deadline) {
            for (ReceivedMessage message : receiver.receive(10, maxWait())) {
                if (!isOwn(message)) {
                    ackQuietly(receiver, message);
                    continue;
                }
                assertTrue(receiver.capabilities().orderedDelivery());
                byKey.computeIfAbsent(message.orderingKey(), key -> new ArrayList<>()).add(message.bodyAsString());
                receiver.ack(message);
                total++;
            }
        }
        assertEquals(List.of("a1", "a2", "a3"), byKey.get(keyA));
        assertEquals(List.of("b1", "b2", "b3"), byKey.get(keyB));
    }

    @Test
    void orderedSenderRequiresOrderingKey() {
        MessageSender sender = track(newOrderedSender());
        assumeTrue(sender != null, "sem sender ordenado");

        assertThrows(IllegalArgumentException.class, () -> sender.send(own(OutgoingMessage.ofText("x"))));
    }

    // ---------------------------------------------------------------- acesso e ciclo de vida

    @Test
    void checkAccessSucceedsOnExistingDestination() {
        assertDoesNotThrow(() -> sender().checkAccess());
        assertDoesNotThrow(() -> receiver().checkAccess());
    }

    @Test
    void missingDestinationIsReported() {
        MessageSender sender = track(newMissingDestinationSender());
        MessageReceiver receiver = track(newMissingDestinationReceiver());
        assumeTrue(sender != null || receiver != null, "sem destino inexistente configurado");

        if (sender != null) {
            assertMissing(sender::checkAccess);
            assertMissing(() -> sender.send(own(OutgoingMessage.ofText("x"))));
        }
        if (receiver != null) {
            assertMissing(receiver::checkAccess);
            assertMissing(() -> receiver.receive(1, maxWait()));
        }
    }

    @Test
    void closedSenderAndReceiverRejectUse() {
        MessageSender sender = sender();
        MessageReceiver receiver = receiver();
        sender.close();
        receiver.close();

        assertThrows(IllegalStateException.class, () -> sender.send(own(OutgoingMessage.ofText("x"))));
        assertThrows(IllegalStateException.class, () -> receiver.receive(1, maxWait()));
    }

    // ---------------------------------------------------------------- utilitários

    private MessageSender sender() {
        return track(newSender());
    }

    private MessageReceiver receiver() {
        return track(newReceiver());
    }

    private <T extends AutoCloseable> T track(T closeable) {
        if (closeable != null) {
            opened.add(closeable);
        }
        return closeable;
    }

    private OutgoingMessage own(OutgoingMessage message) {
        return message.withAttribute(RUN_ATTRIBUTE, run);
    }

    private boolean isOwn(ReceivedMessage message) {
        return run.equals(message.attributes().get(RUN_ATTRIBUTE));
    }

    private List<ReceivedMessage> awaitOwn(MessageReceiver receiver, int count) {
        return awaitOwn(receiver, count, settle());
    }

    /** Recebe até ter {@code count} mensagens deste teste; as de outros testes são descartadas. */
    private List<ReceivedMessage> awaitOwn(MessageReceiver receiver, int count, Duration timeout) {
        List<ReceivedMessage> own = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (own.size() < count && System.nanoTime() < deadline) {
            for (ReceivedMessage message : receiver.receive(count - own.size(), maxWait())) {
                if (isOwn(message)) {
                    own.add(message);
                } else {
                    ackQuietly(receiver, message);
                }
            }
        }
        assertEquals(count, own.size(), "mensagens recebidas em " + timeout);
        return own;
    }

    /** Falha se uma mensagem deste teste aparecer durante {@code during}. */
    private void assertNoneOwn(MessageReceiver receiver, Duration during) {
        long deadline = System.nanoTime() + during.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            Duration wait = Duration.ofNanos(Math.min(remaining, maxWait().toNanos()));
            for (ReceivedMessage message : receiver.receive(10, wait)) {
                if (isOwn(message)) {
                    fail("Mensagem entregue quando não devia: " + message);
                }
                ackQuietly(receiver, message);
            }
        }
    }

    private void ackForeign(MessageReceiver receiver, List<ReceivedMessage> messages) {
        for (ReceivedMessage message : messages) {
            if (!isOwn(message)) {
                ackQuietly(receiver, message);
            }
        }
    }

    private static void ackQuietly(MessageReceiver receiver, ReceivedMessage message) {
        try {
            receiver.ack(message);
        } catch (MessagingException ignored) {
            // sobra de outro teste com lease vencido
        }
    }

    private static void assertMissing(Runnable action) {
        MessagingException e = assertThrows(MessagingException.class, action::run);
        assertTrue(e instanceof DestinationNotFoundException || e instanceof AccessDeniedException,
                () -> "esperado DestinationNotFoundException ou AccessDeniedException: " + e);
    }
}
