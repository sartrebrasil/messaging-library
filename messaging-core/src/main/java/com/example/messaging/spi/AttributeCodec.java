package com.example.messaging.spi;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Atributos do usuário no formato do provedor (ADR-0004). Onde o provedor aceita poucos atributos
 * (10 no SQS/SNS), até {@code nativeLimit} vão nativos; acima disso, todos vão juntos no atributo
 * reservado {@link ReservedAttributes#ATTRIBUTES}, um objeto JSON de strings. Nunca uma mistura.
 */
public final class AttributeCodec {

    private AttributeCodec() {
    }

    /**
     * @param user        atributos já validados por {@link com.example.messaging.OutgoingMessage}
     * @param nativeLimit quantos atributos do usuário cabem como nativos
     * @return os próprios atributos, ou um mapa só com o pacote
     */
    public static Map<String, String> toNative(Map<String, String> user, int nativeLimit) {
        if (user.size() <= nativeLimit) {
            return user;
        }
        return Map.of(ReservedAttributes.ATTRIBUTES, pack(user));
    }

    /**
     * Inverso de {@link #toNative}: desfaz o pacote e remove os atributos reservados. Atributos que
     * não são da lib (de outro produtor) passam como vieram.
     *
     * @throws IllegalArgumentException pacote com JSON inválido
     */
    public static Map<String, String> fromNative(Map<String, String> nativeAttributes) {
        Map<String, String> result = new HashMap<>();
        nativeAttributes.forEach((key, value) -> {
            if (!ReservedAttributes.isReserved(key)) {
                result.put(key, value);
            }
        });
        String packed = nativeAttributes.get(ReservedAttributes.ATTRIBUTES);
        if (packed != null) {
            result.putAll(unpack(packed));
        }
        return result;
    }

    /** JSON com as chaves em ordem, para o resultado ser estável. */
    public static String pack(Map<String, String> attributes) {
        StringBuilder json = new StringBuilder("{");
        new TreeMap<>(attributes).forEach((key, value) -> {
            if (json.length() > 1) {
                json.append(',');
            }
            quote(json, key).append(':');
            quote(json, value);
        });
        return json.append('}').toString();
    }

    /**
     * Lê um objeto JSON plano de strings.
     *
     * @throws IllegalArgumentException JSON inválido ou com valor que não é string
     */
    public static Map<String, String> unpack(String json) {
        return new Parser(json).object();
    }

    private static StringBuilder quote(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"');
    }

    private static final class Parser {

        private final String json;
        private int pos;

        Parser(String json) {
            this.json = json;
        }

        Map<String, String> object() {
            Map<String, String> result = new LinkedHashMap<>();
            skipSpaces();
            expect('{');
            skipSpaces();
            if (peek() == '}') {
                pos++;
                return end(result);
            }
            while (true) {
                skipSpaces();
                String key = string();
                skipSpaces();
                expect(':');
                skipSpaces();
                result.put(key, string());
                skipSpaces();
                char c = next();
                if (c == '}') {
                    return end(result);
                }
                if (c != ',') {
                    throw error("esperado ',' ou '}'");
                }
            }
        }

        private Map<String, String> end(Map<String, String> result) {
            skipSpaces();
            if (pos != json.length()) {
                throw error("conteúdo depois do objeto");
            }
            return result;
        }

        private String string() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return out.toString();
                }
                if (c != '\\') {
                    out.append(c);
                    continue;
                }
                char escaped = next();
                switch (escaped) {
                    case '"', '\\', '/' -> out.append(escaped);
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'u' -> {
                        if (pos + 4 > json.length()) {
                            throw error("escape \\u incompleto");
                        }
                        try {
                            out.append((char) Integer.parseInt(json.substring(pos, pos + 4), 16));
                        } catch (NumberFormatException e) {
                            throw error("escape \\u inválido");
                        }
                        pos += 4;
                    }
                    default -> throw error("escape inválido");
                }
            }
        }

        private void skipSpaces() {
            while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
                pos++;
            }
        }

        private char peek() {
            if (pos >= json.length()) {
                throw error("fim inesperado");
            }
            return json.charAt(pos);
        }

        private char next() {
            char c = peek();
            pos++;
            return c;
        }

        private void expect(char expected) {
            if (next() != expected) {
                throw error("esperado '" + expected + "'");
            }
        }

        private IllegalArgumentException error(String detail) {
            return new IllegalArgumentException("Atributo '" + ReservedAttributes.ATTRIBUTES
                    + "' com JSON inválido na posição " + pos + ": " + detail);
        }
    }
}
