package com.example.messaging.inbox;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OutboxJsonTest {

    @Test
    void decodesEscapesAndSurrogatePairs() {
        var body = OutboxJson.parse("{\"eventType\":\"a\\\"b\\\\c\\/d\\n\\t\\u00e9\\ud83d\\ude00\",\"payload\":1}");

        assertEquals("a\"b\\c/d\n\té\uD83D\uDE00", body.strings().get("eventType"));
        assertEquals("1", body.payload());
    }

    @Test
    void keepsPayloadRawAndSkipsUnknownMembers() {
        var body = OutboxJson.parse(" { \"extra\" : [true, null, -1.5e3, {\"x\":\"]}\"}], \"payload\" : [ {\"k\" : \"v\"} ] ,"
                + " \"headers\":{\"a\":\"1\",\"n\":2}, \"aggregateVersion\": 42 } ");

        assertEquals("[ {\"k\" : \"v\"} ]", body.payload());
        assertEquals(Map.of("a", "1"), body.headers());
        assertEquals("42", body.aggregateVersion());
        assertFalse(body.strings().containsKey("extra"));
    }

    @Test
    void rejectsMalformedInput() {
        for (String bad : new String[] {"", "[]", "{", "{\"a\":}", "{\"a\":1,}", "{\"a\":tru}", "{\"a\":\"x\"} y",
                "{\"a\":\"\\q\"}", "{\"a\":\"\\u12\"}", "{\"a\":\"line\nbreak\"}", "{\"payload\":[1,2}"}) {
            assertThrows(IllegalArgumentException.class, () -> OutboxJson.parse(bad), bad);
        }
    }

    @Test
    void deepNestingDoesNotOverflowTheStack() {
        String deep = "[".repeat(200_000) + "]".repeat(200_000);

        var body = OutboxJson.parse("{\"payload\":" + deep + "}");

        assertEquals(deep, body.payload());
        assertThrows(IllegalArgumentException.class, () -> OutboxJson.parse("{\"payload\":" + "[".repeat(200_000) + "}"));
    }
}
