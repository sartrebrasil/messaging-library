package com.example.messaging.inbox;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

class CopiesTest {

    private static final Instant NOW = Instant.parse("2026-11-05T10:00:00Z");

    @Test
    void copyKeepsTheOriginalAndRewritesInboxAttributes() {
        var received = received("m-1", Map.of("tenant", "t", "inbox_attempts", "l2=9;l3=9",
                "inbox_failure", "consumer=c;destination=d;exception=X;first_failure_at=2026-11-01T00:00:00Z"), "k-1");

        var copy = Copies.build(received, new AttemptState(1, 0), failure(new IllegalStateException("down")));

        assertEquals("t", copy.message().attributes().get("tenant"));
        assertEquals("l2=1;l3=0", copy.message().attributes().get("inbox_attempts"));
        String failureAttr = copy.message().attributes().get("inbox_failure");
        assertTrue(failureAttr.startsWith("consumer=billing;destination=orders;exception=java.lang.IllegalStateException"));
        assertTrue(failureAttr.endsWith("first_failure_at=2026-11-01T00:00:00Z"), "keeps the first failure");
        assertEquals("m-1", copy.message().deduplicationId());
        assertEquals("k-1", copy.message().orderingKey());
        assertEquals("m-1", copy.message().deadLetter().originalMessageId());
        assertEquals("java.lang.IllegalStateException: down", copy.message().deadLetter().reason());
        assertFalse(copy.budgetExceeded());
    }

    @Test
    void attributeBudgetDropsTheFailureFirstThenTheAttempts() {
        var fourteen = Copies.build(received("m", userAttributes(14), null), AttemptState.initial(), failure(null));
        var fifteen = Copies.build(received("m", userAttributes(15), null), AttemptState.initial(), failure(null));
        var sixteen = Copies.build(received("m", userAttributes(16), null), AttemptState.initial(), failure(null));

        assertTrue(fourteen.message().attributes().containsKey("inbox_failure"));
        assertFalse(fourteen.budgetExceeded());
        assertFalse(fifteen.message().attributes().containsKey("inbox_failure"));
        assertTrue(fifteen.message().attributes().containsKey("inbox_attempts"));
        assertTrue(fifteen.budgetExceeded());
        assertFalse(fifteen.attemptsDropped());
        assertFalse(sixteen.message().attributes().containsKey("inbox_attempts"));
        assertTrue(sixteen.attemptsDropped());
    }

    @Test
    void invalidMessageIdBecomesAStableHash() {
        String id = "id with spaces";

        String dedup = Copies.deduplicationId(id);

        assertEquals(64, dedup.length());
        assertEquals(dedup, Copies.deduplicationId(id));
        assertEquals("ok-id", Copies.deduplicationId("ok-id"));
    }

    @Test
    void aMessageThatBreaksTheSendingRulesCannotBeCopied() {
        assertThrows(IllegalArgumentException.class,
                () -> Copies.build(received("m", Map.of(), "key with spaces"), AttemptState.initial(), null));
    }

    @Test
    void exceptionMessageCanBeLeftOut() {
        var f = Copies.Failure.of("billing", "orders", new IllegalStateException("cpf 123"), "x", null, false, NOW);

        assertEquals("java.lang.IllegalStateException", f.reason());
        assertEquals("rejected", Copies.Failure.of("billing", "orders", null, "rejected", null, true, NOW).reason());
        assertEquals("rejected: no cnpj", Copies.Failure.of("billing", "orders", null, "rejected", "no cnpj", true, NOW).reason());
        assertEquals("rejected: no cnpj (java.lang.IllegalStateException: down)", Copies.Failure.of("billing", "orders",
                new IllegalStateException("down"), "rejected", "no cnpj", true, NOW).reason());
    }

    private static Copies.Failure failure(Throwable cause) {
        return Copies.Failure.of("billing", "orders", cause, "rejected", null, true, NOW);
    }

    private static Map<String, String> userAttributes(int n) {
        var attributes = new HashMap<String, String>();
        for (int i = 0; i < n; i++) {
            attributes.put("a" + i, "v");
        }
        return attributes;
    }

    static ReceivedMessage received(String id, Map<String, String> attributes, String orderingKey) {
        return new ReceivedMessage(id, "body".getBytes(StandardCharsets.UTF_8), attributes, null, orderingKey,
                OptionalInt.of(1), NOW, NOW, NOW.plusSeconds(30), null, null, new Object());
    }

    /** Atributos de um produtor de fora da lib que a cópia não aceitaria ficam de fora, e a cópia sai. */
    @Test
    void attributesACopyCannotCarryAreLeftOut() {
        var received = received("m-1", Map.of("order_id", "o-1", "orderId", "o-1", "note", "café"), null);

        var copy = Copies.build(received, AttemptState.initial(), null);

        assertEquals("o-1", copy.message().attributes().get("order_id"));
        assertFalse(copy.message().attributes().containsKey("orderId"));
        assertFalse(copy.message().attributes().containsKey("note"));
    }
}
