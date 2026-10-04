package com.example.messaging.spi;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

/**
 * Corpo em bytes para provedores que só aceitam texto (SQS, SNS), decidido pelo
 * {@code contentType}, sem atributo extra (ADR-0004):
 *
 * <ul>
 *   <li>{@code contentType} textual: vai como texto; precisa ser UTF-8 com caracteres que o SQS aceita.</li>
 *   <li>{@code contentType} não textual: vai em Base64.</li>
 *   <li>Sem {@code contentType}: texto se der; senão Base64 com {@value #OCTET_STREAM}.</li>
 * </ul>
 */
public final class BodyCodec {

    public static final String OCTET_STREAM = "application/octet-stream";

    /**
     * @param text        corpo a enviar
     * @param contentType {@code contentType} a gravar; pode ter virado {@value #OCTET_STREAM}
     */
    public record Encoded(String text, String contentType) {
    }

    private BodyCodec() {
    }

    /** {@code text/*}, {@code application/json}, {@code application/xml}, {@code *+json} e {@code *+xml}, ignorando parâmetros. */
    public static boolean isTextual(String contentType) {
        if (contentType == null) {
            return false;
        }
        String type = contentType.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
        return type.startsWith("text/")
                || type.equals("application/json")
                || type.equals("application/xml")
                || type.endsWith("+json")
                || type.endsWith("+xml");
    }

    /**
     * @throws IllegalArgumentException {@code contentType} textual com corpo que não é UTF-8 ou que tem
     *                                  caracteres que o SQS recusa
     */
    public static Encoded toText(byte[] body, String contentType) {
        if (contentType != null && !isTextual(contentType)) {
            return new Encoded(Base64.getEncoder().encodeToString(body), contentType);
        }
        String text = decodeUtf8(body);
        if (text != null && isTransportable(text)) {
            return new Encoded(text, contentType);
        }
        if (contentType != null) {
            throw new IllegalArgumentException("Corpo com contentType '" + contentType
                    + "' não é UTF-8 válido ou tem caracteres de controle que o provedor recusa");
        }
        return new Encoded(Base64.getEncoder().encodeToString(body), OCTET_STREAM);
    }

    /**
     * Inverso de {@link #toText}.
     *
     * @throws IllegalArgumentException {@code contentType} não textual com corpo que não é Base64
     */
    public static byte[] fromText(String text, String contentType) {
        if (contentType == null || isTextual(contentType)) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        try {
            return Base64.getDecoder().decode(text);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Corpo com contentType '" + contentType + "' não está em Base64", e);
        }
    }

    private static String decodeUtf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(body))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    /** Caracteres aceitos pelo SQS: {@code #x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF}. */
    private static boolean isTransportable(String text) {
        return text.codePoints().allMatch(c -> c == 0x9 || c == 0xA || c == 0xD
                || (c >= 0x20 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) || (c >= 0x10000 && c <= 0x10FFFF));
    }
}
