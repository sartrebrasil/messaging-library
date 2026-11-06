package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.PayloadDeserializationException;
import com.example.messaging.inbox.PayloadDeserializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.lang.reflect.Type;

/**
 * Payloads through the application's Jackson 3 mapper (ADR-0003). {@code byte[]} and {@code String} get the raw
 * payload (the outbox {@code payload} member or the whole body), with no JSON decoding; every other type, including
 * {@code JsonNode}, is read as JSON.
 */
public final class JacksonPayloadDeserializer implements PayloadDeserializer {

    private final ObjectMapper mapper;

    public JacksonPayloadDeserializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Object deserialize(InboxMessage message, Type type) {
        if (type == byte[].class) {
            return message.payload();
        }
        if (type == String.class) {
            return message.payloadAsString();
        }
        try {
            return mapper.readValue(message.payload(), mapper.getTypeFactory().constructType(type));
        } catch (JacksonException e) {
            throw new PayloadDeserializationException("payload of message " + message.id() + " is not a " + type, e);
        }
    }
}
