package com.example.messaging.aws;

import com.example.messaging.DeadLetterInfo;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.spi.AttributeCodec;
import com.example.messaging.spi.BodyCodec;
import com.example.messaging.spi.ReservedAttributes;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Mensagem no formato do SQS e do SNS (ADR-0004): corpo em texto ou Base64, até
 * {@value #NATIVE_USER_ATTRIBUTES} atributos do usuário nativos (o resto empacotado) e os
 * atributos reservados {@code content_type}, {@code traceparent} e {@code dead_letter}.
 */
final class AwsCodec {

    /** 10 atributos do SQS/SNS menos os 3 reservados que a lib pode gravar. */
    static final int NATIVE_USER_ATTRIBUTES = 7;
    static final String STRING = "String";

    private static final System.Logger LOG = System.getLogger(AwsCodec.class.getName());

    /** @param size bytes como o SQS conta: corpo + nome, tipo e valor de cada atributo */
    record Encoded(String body, Map<String, String> attributes, long size) {
    }

    record Decoded(byte[] body, Map<String, String> attributes, String contentType, String traceparent,
                   DeadLetterInfo deadLetter) {
    }

    private AwsCodec() {
    }

    static Encoded encode(OutgoingMessage message, long maxBytes) {
        BodyCodec.Encoded body = BodyCodec.toText(message.body(), message.contentType());
        Map<String, String> attributes = new HashMap<>(
                AttributeCodec.toNative(message.attributes(), NATIVE_USER_ATTRIBUTES));
        if (body.contentType() != null) {
            attributes.put(ReservedAttributes.CONTENT_TYPE, body.contentType());
        }
        if (message.traceparent() != null) {
            attributes.put(ReservedAttributes.TRACEPARENT, message.traceparent());
        }
        if (message.deadLetter() != null) {
            attributes.put(ReservedAttributes.DEAD_LETTER, message.deadLetter().toAttributeValue());
        }
        long size = utf8Length(body.text());
        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
            size += utf8Length(attribute.getKey()) + STRING.length() + utf8Length(attribute.getValue());
        }
        if (size > maxBytes) {
            throw new MessageTooLargeException(AwsErrors.PROVIDER, "Mensagem com " + size
                    + " bytes no formato do provedor; limite " + maxBytes, null);
        }
        return new Encoded(body.text(), attributes, size);
    }

    /**
     * Mensagem de outro produtor fora do formato da lib (Base64 ou pacote inválido) é entregue
     * como veio, com um aviso no log, em vez de travar o lote.
     */
    static Decoded decode(String messageId, String text, Map<String, String> nativeAttributes) {
        String contentType = nativeAttributes.get(ReservedAttributes.CONTENT_TYPE);
        byte[] body;
        try {
            body = BodyCodec.fromText(text, contentType);
        } catch (IllegalArgumentException e) {
            LOG.log(System.Logger.Level.WARNING, "Mensagem {0}: corpo entregue sem decodificar ({1})", messageId,
                    e.getMessage());
            body = text.getBytes(StandardCharsets.UTF_8);
        }
        Map<String, String> attributes;
        try {
            attributes = AttributeCodec.fromNative(nativeAttributes);
        } catch (IllegalArgumentException e) {
            LOG.log(System.Logger.Level.WARNING, "Mensagem {0}: atributos entregues sem desempacotar ({1})", messageId,
                    e.getMessage());
            attributes = new HashMap<>(nativeAttributes);
            attributes.keySet().removeIf(ReservedAttributes::isReserved);
        }
        String deadLetter = nativeAttributes.get(ReservedAttributes.DEAD_LETTER);
        return new Decoded(body, attributes, contentType, nativeAttributes.get(ReservedAttributes.TRACEPARENT),
                deadLetter == null ? null : DeadLetterInfo.fromAttributeValue(deadLetter));
    }

    static boolean isFifo(String queueUrlOrTopicArn) {
        return queueUrlOrTopicArn.endsWith(".fifo");
    }

    private static long utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
