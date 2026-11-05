package com.example.messaging.inbox;

import com.example.messaging.ReceivedMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

class EnvelopeReaderTest {

    private static final Instant NOW = Instant.parse("2026-11-05T10:00:00Z");
    private static final String BODY_TRACE = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private static final String TRANSPORT_TRACE = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private final EnvelopeReader auto = new EnvelopeReader(EnvelopeMode.AUTO);

    @Test
    void outboxBodyFromTheOutboxLibrary() throws IOException {
        InboxMessage m = auto.read(received(fixture("outbox-document-received.json"),
                Map.of("tenant", "t1", "inbox_attempts", "l2=0;l3=0"), TRANSPORT_TRACE, 1), "documents");

        assertEquals("6f1c2a4e-9b7d-4c1e-8a2f-3d5e7b9c1a20", m.id());
        assertEquals("DocumentReceived", m.type());
        assertNull(m.source());
        assertEquals(Instant.parse("2026-10-27T00:00:00Z"), m.time());
        assertEquals("d-1", m.subject());
        assertEquals("Document", m.aggregateType());
        assertEquals(3L, m.aggregateVersion());
        assertEquals("{\"accessKey\":\"x\",\"items\":[{\"n\":1},{\"n\":2}],\"note\":\"a \\\"quoted\\\" }\"}",
                m.payloadAsString());
        assertEquals(Map.of("tenant", "t1", "traceparent", BODY_TRACE, "tracestate", "vendor=1"), m.headers());
        assertEquals("documents", m.destination());
        assertEquals(Delivery.first(), m.delivery());
    }

    @Test
    void traceparentPrefersTheOutboxBodyThenTheTransport() {
        String withTrace = outbox("{\"traceparent\":\"" + BODY_TRACE + "\"}");
        String invalidTrace = outbox("{\"traceparent\":\"not-a-trace\"}");
        String noTrace = outbox("{}");

        assertEquals(BODY_TRACE, auto.read(received(withTrace, Map.of(), TRANSPORT_TRACE, 1), "q").traceparent());
        assertEquals(TRANSPORT_TRACE, auto.read(received(invalidTrace, Map.of(), TRANSPORT_TRACE, 1), "q").traceparent());
        assertEquals(TRANSPORT_TRACE, auto.read(received(noTrace, Map.of(), TRANSPORT_TRACE, 1), "q").traceparent());
        assertNull(auto.read(received(noTrace, Map.of(), null, 1), "q").traceparent());
    }

    @Test
    void outboxHeadersWinOverTransportAttributes() {
        InboxMessage m = auto.read(received(outbox("{\"tenant\":\"from-body\"}"), Map.of("tenant", "from-attr"), null, 1), "q");

        assertEquals("from-body", m.headers().get("tenant"));
    }

    @Test
    void cloudEventsBinary() {
        Map<String, String> attributes = new HashMap<>(Map.of("ce_id", "e-1", "ce_type", "OrderCreated",
                "ce_source", "/orders", "ce_time", "2026-11-05T09:00:00Z", "ce_subject", "o-1",
                "ce_aggregatetype", "Order", "ce_aggregateversion", "7", "ce_specversion", "1.0"));
        attributes.put("inbox_attempts", "l2=2;l3=0");

        InboxMessage m = auto.read(received("{\"total\":10}", attributes, TRANSPORT_TRACE, 1), "orders");

        assertEquals("e-1", m.id());
        assertEquals("OrderCreated", m.type());
        assertEquals("/orders", m.source());
        assertEquals(Instant.parse("2026-11-05T09:00:00Z"), m.time());
        assertEquals("o-1", m.subject());
        assertEquals("Order", m.aggregateType());
        assertEquals(7L, m.aggregateVersion());
        assertEquals(TRANSPORT_TRACE, m.traceparent());
        assertEquals("{\"total\":10}", m.payloadAsString());
        assertFalse(m.headers().containsKey("inbox_attempts"));
        assertEquals("1.0", m.headers().get("ce_specversion"));
        assertEquals(new Delivery(Delivery.Level.DELAYED, 2), m.delivery());
    }

    @Test
    void deliveryCountAddsDelayedAttempts() {
        InboxMessage m = auto.read(received("plain", Map.of("inbox_attempts", "l2=1;l3=0"), null, 3), "q");

        assertEquals(new Delivery(Delivery.Level.DELAYED, 3), m.delivery());
    }

    @Test
    void rawWhenNoEnvelopeIsRecognized() {
        for (String body : new String[] {"plain text", "{\"messageId\":\"m\"}", "{broken", "[1,2]"}) {
            InboxMessage m = auto.read(received(body, Map.of(), TRANSPORT_TRACE, 1), "q");

            assertEquals("native-1", m.id(), body);
            assertNull(m.type(), body);
            assertEquals(body, m.payloadAsString());
            assertEquals(TRANSPORT_TRACE, m.traceparent());
        }
    }

    @Test
    void recognizedEnvelopeWithBrokenFieldFailsInsteadOfTurningRaw() {
        String badTime = "{\"messageId\":\"m\",\"eventType\":\"E\",\"occurredAt\":\"yesterday\",\"payload\":{}}";
        String badVersion = "{\"messageId\":\"m\",\"eventType\":\"E\",\"aggregateVersion\":1.5,\"payload\":{}}";

        assertThrows(PayloadDeserializationException.class, () -> auto.read(received(badTime, Map.of(), null, 1), "q"));
        assertThrows(PayloadDeserializationException.class, () -> auto.read(received(badVersion, Map.of(), null, 1), "q"));
        assertThrows(PayloadDeserializationException.class, () -> auto.read(received("x",
                Map.of("ce_id", "e", "ce_type", "T", "ce_time", "soon"), null, 1), "q"));
    }

    @Test
    void explicitModesFailWhenTheirFormatIsMissing() {
        var outbox = new EnvelopeReader(EnvelopeMode.OUTBOX);
        var cloudEvents = new EnvelopeReader(EnvelopeMode.CLOUDEVENTS);

        assertThrows(PayloadDeserializationException.class, () -> outbox.read(received("{broken", Map.of(), null, 1), "q"));
        assertThrows(PayloadDeserializationException.class,
                () -> outbox.read(received("{\"messageId\":\"m\"}", Map.of(), null, 1), "q"));
        assertThrows(PayloadDeserializationException.class,
                () -> cloudEvents.read(received("x", Map.of("ce_id", "e"), null, 1), "q"));
    }

    @Test
    void rawModeIgnoresEnvelopes() {
        InboxMessage m = new EnvelopeReader(EnvelopeMode.RAW).read(received(outbox("{}"), Map.of("ce_id", "e"), null, 1), "q");

        assertEquals("native-1", m.id());
        assertEquals(outbox("{}"), m.payloadAsString());
    }

    @Test
    void deserializerBuildsTypedMessage() {
        PayloadDeserializer deserializer = (message, type) -> message.payloadAsString().toUpperCase();
        InboxMessage m = auto.read(received("hello", Map.of(), null, 1), "q");

        TypedMessage<String> typed = deserializer.typed(m, String.class);

        assertEquals("HELLO", typed.payload());
        assertSame(m, typed.message());
    }

    private static String outbox(String headers) {
        return "{\"messageId\":\"m-1\",\"eventType\":\"E\",\"headers\":" + headers + ",\"payload\":{\"a\":1}}";
    }

    private static String fixture(String name) throws IOException {
        try (InputStream in = EnvelopeReaderTest.class.getResourceAsStream("/fixtures/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).strip();
        }
    }

    private static ReceivedMessage received(String body, Map<String, String> attributes, String traceparent, int deliveryCount) {
        return new ReceivedMessage("native-1", body.getBytes(StandardCharsets.UTF_8), attributes, null, null,
                OptionalInt.of(deliveryCount), NOW, NOW, NOW.plusSeconds(30), traceparent, null, new Object());
    }
}
