package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.Delivery;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.PayloadDeserializationException;
import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JacksonPayloadDeserializerTest {

    record Order(String id, int total) {
    }

    private final JacksonPayloadDeserializer deserializer = new JacksonPayloadDeserializer(JsonMapper.builder().build());

    @Test
    void rawTypesGetThePayloadAsIs() {
        InboxMessage text = message("plain text, not json");

        assertThat(deserializer.deserialize(text, String.class)).isEqualTo("plain text, not json");
        assertThat((byte[]) deserializer.deserialize(text, byte[].class)).isEqualTo(text.payload());
    }

    @Test
    void jsonTypesGoThroughTheMapper() {
        InboxMessage json = message("{\"id\":\"o-1\",\"total\":10}");

        assertThat(deserializer.deserialize(json, Order.class)).isEqualTo(new Order("o-1", 10));
        assertThat(((JsonNode) deserializer.deserialize(json, JsonNode.class)).path("id").asString()).isEqualTo("o-1");
        assertThat(deserializer.deserialize(message("[{\"id\":\"a\",\"total\":1}]"),
                new TypeReference<List<Order>>() { }.getType())).isEqualTo(List.of(new Order("a", 1)));
    }

    @Test
    void unreadableJsonIsADeserializationFailure() {
        assertThatThrownBy(() -> deserializer.deserialize(message("not json"), Order.class))
                .isInstanceOf(PayloadDeserializationException.class);
    }

    private static InboxMessage message(String payload) {
        return new InboxMessage("m-1", null, null, null, null, null, null, null, Map.of(),
                payload.getBytes(StandardCharsets.UTF_8), "q", Delivery.first());
    }
}
