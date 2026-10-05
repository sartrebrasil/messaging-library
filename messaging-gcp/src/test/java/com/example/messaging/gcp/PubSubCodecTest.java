package com.example.messaging.gcp;

import com.example.messaging.DeadLetterInfo;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PubSubCodecTest {

    @Test
    void roundTripKeepsReservedFieldsOutOfUserAttributes() {
        OutgoingMessage message = OutgoingMessage.of(new byte[]{0, (byte) 0xFF})
                .withContentType("application/avro")
                .withAttribute("tenant", "acme")
                .withOrderingKey("pedido-1")
                .withTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .withDeadLetter(new DeadLetterInfo("orig", "motivo"));

        PubsubMessage wire = PubSubCodec.encode(message);
        assertEquals("pedido-1", wire.getOrderingKey());
        assertEquals("application/avro", wire.getAttributesOrThrow("content_type"));

        Instant now = Instant.now();
        ReceivedMessage received = PubSubCodec.decode(wire.toBuilder().setMessageId("m1").build(), 0, now,
                Duration.ofSeconds(10), null);
        assertArrayEquals(message.body(), received.body());
        assertEquals(Map.of("tenant", "acme"), received.attributes());
        assertEquals("application/avro", received.contentType());
        assertEquals("pedido-1", received.orderingKey());
        assertEquals(message.traceparent(), received.traceparent());
        assertEquals(new DeadLetterInfo("orig", "motivo"), received.deadLetter());
        assertTrue(received.deliveryCount().isEmpty(), "deliveryAttempt 0 = sem dead letter policy");
        assertEquals(now.plusSeconds(10), received.leaseExpiresAt());
    }

    @Test
    void deliveryAttemptIsKeptWhenPresent() {
        ReceivedMessage received = PubSubCodec.decode(PubsubMessage.newBuilder().setMessageId("m1").build(), 3,
                Instant.now(), Duration.ofSeconds(10), null);
        assertEquals(3, received.deliveryCount().orElseThrow());
        assertNull(received.orderingKey());
    }

    @Test
    void messagesAboveTenMegabytesAreRejected() {
        byte[] body = new byte[(int) PubSubCodec.MAX_MESSAGE_BYTES];
        assertThrows(MessageTooLargeException.class,
                () -> PubSubCodec.encode(OutgoingMessage.of(body).withAttribute("k", "v")));
    }
}
