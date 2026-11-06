package com.example.messaging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CoreModelTest {

    @Test
    void emptyBodyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> OutgoingMessage.of(new byte[0]));
        assertThrows(NullPointerException.class, () -> OutgoingMessage.of(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Tenant", "1abc", "com-hifen", "com.ponto", "google_id", "goog", "", "content_type",
            "traceparent", "dead_letter", "delivery_count", "attributes"})
    void invalidOrReservedAttributeKeysAreRejected(String key) {
        assertThrows(IllegalArgumentException.class, () -> OutgoingMessage.ofText("x").withAttribute(key, "v"));
    }

    @Test
    void attributeKeyAndValueLimits() {
        OutgoingMessage message = OutgoingMessage.ofText("x");
        assertDoesNotThrow(() -> message.withAttribute("a".repeat(64), "v".repeat(1024)));
        assertThrows(IllegalArgumentException.class, () -> message.withAttribute("a".repeat(65), "v"));
        assertThrows(IllegalArgumentException.class, () -> message.withAttribute("k", "v".repeat(1025)));
        assertThrows(IllegalArgumentException.class, () -> message.withAttribute("k", "ação"));
        assertThrows(IllegalArgumentException.class, () -> message.withAttribute("k", "linha\nnova"));
    }

    @Test
    void atMostSixteenAttributes() {
        Map<String, String> attributes = new HashMap<>();
        for (int i = 0; i < 16; i++) {
            attributes.put("k" + i, "v");
        }
        OutgoingMessage sixteen = OutgoingMessage.ofText("x").withAttributes(attributes);
        assertEquals(16, sixteen.attributes().size());

        attributes.put("k16", "v");
        assertThrows(IllegalArgumentException.class, () -> OutgoingMessage.ofText("x").withAttributes(attributes));
    }

    @Test
    void idsAndTraceparentAreValidated() {
        OutgoingMessage message = OutgoingMessage.ofText("x");
        assertDoesNotThrow(() -> message.withOrderingKey("pedido-123").withDeduplicationId("e".repeat(128)));
        assertThrows(IllegalArgumentException.class, () -> message.withOrderingKey("com espaço"));
        assertThrows(IllegalArgumentException.class, () -> message.withOrderingKey(""));
        assertThrows(IllegalArgumentException.class, () -> message.withDeduplicationId("e".repeat(129)));
        assertDoesNotThrow(() -> message.withTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
        assertThrows(IllegalArgumentException.class, () -> message.withTraceparent("00-abc-01"));
        assertThrows(IllegalArgumentException.class, () -> message.withContentType(" "));
    }

    @Test
    void outgoingMessageComparesBodyByContent() {
        assertEquals(OutgoingMessage.ofText("x").withAttribute("k", "v"), OutgoingMessage.ofText("x").withAttribute("k", "v"));
        assertNotEquals(OutgoingMessage.ofText("x"), OutgoingMessage.ofText("y"));
        assertFalse(OutgoingMessage.ofText("segredo").toString().contains("segredo"));
    }

    @Test
    void deadLetterInfoRoundTripAndTruncation() {
        DeadLetterInfo info = new DeadLetterInfo("msg-1", "falhou; de novo");
        assertEquals("msg-1;falhou; de novo", info.toAttributeValue());
        assertEquals(info, DeadLetterInfo.fromAttributeValue(info.toAttributeValue()));

        assertEquals(new DeadLetterInfo(null, "sem id"), DeadLetterInfo.fromAttributeValue(";sem id"));
        assertEquals("?rro", DeadLetterInfo.fromAttributeValue(new DeadLetterInfo(null, "érro").toAttributeValue()).reason());

        String truncated = new DeadLetterInfo("id", "x".repeat(5000)).toAttributeValue();
        assertEquals(DeadLetterInfo.MAX_ATTRIBUTE_LENGTH, truncated.length());
    }

    @Test
    void copyOfKeepsContentAndLeavesDeduplicationAndDeadLetterToTheCaller() {
        var now = java.time.Instant.parse("2026-11-05T10:00:00Z");
        var received = new ReceivedMessage("m-1", "body".getBytes(), Map.of("tenant", "a"), "application/json",
                "order-7", java.util.OptionalInt.of(3), now, now, now.plusSeconds(30),
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", new DeadLetterInfo("m-0", "x"), new Object());

        OutgoingMessage copy = OutgoingMessage.copyOf(received);

        assertArrayEquals("body".getBytes(), copy.body());
        assertEquals(Map.of("tenant", "a"), copy.attributes());
        assertEquals("application/json", copy.contentType());
        assertEquals("order-7", copy.orderingKey());
        assertEquals(received.traceparent(), copy.traceparent());
        assertNull(copy.deduplicationId());
        assertNull(copy.deadLetter());
    }

    /** Atributos de um produtor de fora da lib que uma cópia não aceitaria: a cópia os deixa de fora, sem falhar. */
    @Test
    void copyOfDropsAttributesACopyCouldNotCarry() {
        var now = java.time.Instant.parse("2026-11-05T10:00:00Z");
        var received = new ReceivedMessage("m-1", "body".getBytes(),
                Map.of("order_id", "o-1", "orderId", "o-1", "goog_x", "1", "note", "café"), null, null,
                java.util.OptionalInt.of(1), now, now, now.plusSeconds(30), null, null, new Object());

        OutgoingMessage copy = OutgoingMessage.copyOf(received);

        assertEquals(Map.of("order_id", "o-1"), copy.attributes());
    }
}
