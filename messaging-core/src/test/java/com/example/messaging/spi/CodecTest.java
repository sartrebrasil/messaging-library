package com.example.messaging.spi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CodecTest {

    @Test
    void attributesUpToLimitStayNative() {
        Map<String, String> user = Map.of("a", "1", "b", "2");
        assertSame(user, AttributeCodec.toNative(user, 7));
    }

    @Test
    void attributesAboveLimitAreAllPacked() {
        Map<String, String> user = new HashMap<>();
        for (int i = 0; i < 8; i++) {
            user.put("k" + i, "v\"" + i + "\\");
        }
        Map<String, String> packed = AttributeCodec.toNative(user, 7);

        assertEquals(1, packed.size());
        assertTrue(packed.containsKey(ReservedAttributes.ATTRIBUTES));
        assertEquals(user, AttributeCodec.fromNative(packed));
    }

    @Test
    void fromNativeRemovesReservedAndKeepsForeignAttributes() {
        Map<String, String> nativeAttributes = new HashMap<>();
        nativeAttributes.put(ReservedAttributes.CONTENT_TYPE, "application/json");
        nativeAttributes.put(ReservedAttributes.TRACEPARENT, "00-...");
        nativeAttributes.put("Header-Externo", "x");
        nativeAttributes.put(ReservedAttributes.ATTRIBUTES, "{\"tenant\":\"acme\"}");

        assertEquals(Map.of("Header-Externo", "x", "tenant", "acme"), AttributeCodec.fromNative(nativeAttributes));
    }

    @Test
    void packIsStableAndUnpackAcceptsStandardJson() {
        assertEquals("{\"a\":\"1\",\"b\":\"2\"}", AttributeCodec.pack(Map.of("b", "2", "a", "1")));
        assertEquals(Map.of("k", "a\"b\\c/\n\u00e9"),
                AttributeCodec.unpack(" { \"k\" : \"a\\\"b\\\\c\\/\\n\\u00e9\" } "));
        assertEquals(Map.of(), AttributeCodec.unpack("{}"));
    }

    @Test
    void malformedPackIsRejected() {
        for (String json : new String[]{"", "[]", "{\"k\":1}", "{\"k\":\"v\"", "{\"k\":\"v\"} x", "{\"k\":\"\\x\"}",
                "{\"k\":\"\\u12\"}"}) {
            assertThrows(IllegalArgumentException.class, () -> AttributeCodec.unpack(json), json);
        }
    }

    @Test
    void textualContentTypes() {
        assertTrue(BodyCodec.isTextual("text/plain; charset=utf-8"));
        assertTrue(BodyCodec.isTextual("application/json"));
        assertTrue(BodyCodec.isTextual("Application/XML"));
        assertTrue(BodyCodec.isTextual("application/cloudevents+json"));
        assertFalse(BodyCodec.isTextual("application/octet-stream"));
        assertFalse(BodyCodec.isTextual("application/avro"));
        assertFalse(BodyCodec.isTextual(null));
    }

    @Test
    void textualBodyGoesAsText() {
        byte[] body = "{\"pedido\":\"ação\"}".getBytes(StandardCharsets.UTF_8);
        BodyCodec.Encoded encoded = BodyCodec.toText(body, "application/json");

        assertEquals("{\"pedido\":\"ação\"}", encoded.text());
        assertEquals("application/json", encoded.contentType());
        assertArrayEquals(body, BodyCodec.fromText(encoded.text(), encoded.contentType()));
    }

    @Test
    void textualContentTypeWithInvalidBodyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> BodyCodec.toText(new byte[]{(byte) 0xC3, 0x28}, "text/plain"));
        assertThrows(IllegalArgumentException.class, () -> BodyCodec.toText(new byte[]{'a', 0x00}, "text/plain"));
    }

    @Test
    void binaryBodyGoesAsBase64() {
        byte[] body = {0x00, (byte) 0xFF, 0x10};
        BodyCodec.Encoded explicit = BodyCodec.toText(body, "application/avro");
        assertEquals("AP8Q", explicit.text());
        assertArrayEquals(body, BodyCodec.fromText(explicit.text(), explicit.contentType()));

        BodyCodec.Encoded inferred = BodyCodec.toText(body, null);
        assertEquals(BodyCodec.OCTET_STREAM, inferred.contentType());
        assertArrayEquals(body, BodyCodec.fromText(inferred.text(), inferred.contentType()));
    }

    @Test
    void plainTextWithoutContentTypeStaysText() {
        BodyCodec.Encoded encoded = BodyCodec.toText("olá".getBytes(StandardCharsets.UTF_8), null);
        assertEquals("olá", encoded.text());
        assertNull(encoded.contentType());
    }

    @Test
    void binaryContentTypeWithNonBase64BodyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> BodyCodec.fromText("não é base64!", "application/avro"));
    }
}
