package com.example.messaging.inbox;

import java.util.HashMap;
import java.util.Map;

/**
 * Reads the outbox-library body (ADR-0003) without a JSON library: a top-level object whose string members are
 * decoded, {@code aggregateVersion} as its raw token, {@code headers} as a flat string object and {@code payload} kept
 * as its raw JSON text, unvalidated (the {@link PayloadDeserializer} reads it). Other members are skipped.
 *
 * <p>Skipping is iterative, so deeply nested input cannot overflow the stack.</p>
 *
 * <p>shortcut: only the outbox envelope shape; if the inbox ever needs to decode nested structures, switch to
 * jackson-core with an ADR-0003 revision.</p>
 */
final class OutboxJson {

    /**
     * Decoded envelope members. {@code aggregateVersion} is the raw token (the reader converts it, so a bad value
     * fails the recognized outbox message instead of turning it raw); {@code payload} is raw JSON. Both are
     * {@code null} when absent.
     */
    record Body(Map<String, String> strings, String aggregateVersion, Map<String, String> headers, String payload) {
    }

    private final String text;
    private int pos;

    private OutboxJson(String text) {
        this.text = text;
    }

    /** @throws IllegalArgumentException when {@code text} is not a JSON object */
    static Body parse(String text) {
        return new OutboxJson(text).body();
    }

    private Body body() {
        Map<String, String> strings = new HashMap<>();
        Map<String, String> headers = Map.of();
        String version = null;
        String payload = null;
        skipWhitespace();
        expect('{');
        skipWhitespace();
        if (peek() == '}') {
            pos++;
        } else {
            while (true) {
                skipWhitespace();
                String key = string();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                switch (key) {
                    case "payload" -> {
                        int start = pos;
                        skipValue();
                        payload = text.substring(start, pos);
                    }
                    case "headers" -> headers = flatStringObject();
                    case "aggregateVersion" -> {
                        int start = pos;
                        skipValue();
                        version = text.substring(start, pos);
                    }
                    default -> {
                        if (peek() == '"') {
                            strings.put(key, string());
                        } else {
                            skipValue();
                        }
                    }
                }
                skipWhitespace();
                char c = next();
                if (c == '}') {
                    break;
                }
                if (c != ',') {
                    throw error("expected ',' or '}'");
                }
            }
        }
        skipWhitespace();
        if (pos != text.length()) {
            throw error("content after the object");
        }
        return new Body(strings, version, headers, payload);
    }

    private Map<String, String> flatStringObject() {
        if (peek() == 'n') {
            literal("null");
            return Map.of();
        }
        expect('{');
        Map<String, String> values = new HashMap<>();
        skipWhitespace();
        if (peek() == '}') {
            pos++;
            return values;
        }
        while (true) {
            skipWhitespace();
            String key = string();
            skipWhitespace();
            expect(':');
            skipWhitespace();
            if (peek() == '"') {
                values.put(key, string());
            } else {
                skipValue();
            }
            skipWhitespace();
            char c = next();
            if (c == '}') {
                return values;
            }
            if (c != ',') {
                throw error("expected ',' or '}' in headers");
            }
        }
    }

    /** Skips any JSON value; containers are walked with a depth counter, not recursion. */
    private void skipValue() {
        char c = peek();
        if (c == '"') {
            string();
            return;
        }
        if (c != '{' && c != '[') {
            scalar();
            return;
        }
        int depth = 0;
        do {
            char d = peek();
            if (d == '"') {
                string();
                continue;
            }
            pos++;
            if (d == '{' || d == '[') {
                depth++;
            } else if (d == '}' || d == ']') {
                depth--;
            }
        } while (depth > 0);
    }

    private void scalar() {
        int start = pos;
        while (pos < text.length() && "{}[],:\" \t\r\n".indexOf(text.charAt(pos)) < 0) {
            pos++;
        }
        String token = text.substring(start, pos);
        if (!(token.equals("true") || token.equals("false") || token.equals("null")
                || token.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?"))) {
            pos = start;
            throw error("invalid value");
        }
    }

    private void literal(String word) {
        if (!text.startsWith(word, pos)) {
            throw error("expected " + word);
        }
        pos += word.length();
    }

    /** Decodes a JSON string at {@code pos}, escapes included (RFC 8259). */
    private String string() {
        expect('"');
        StringBuilder out = new StringBuilder();
        while (true) {
            char c = next();
            if (c == '"') {
                return out.toString();
            }
            if (c < 0x20) {
                throw error("control character in string");
            }
            if (c != '\\') {
                out.append(c);
                continue;
            }
            char e = next();
            switch (e) {
                case '"', '\\', '/' -> out.append(e);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (pos + 4 > text.length()) {
                        throw error("truncated \\u escape");
                    }
                    try {
                        out.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
                    } catch (NumberFormatException ex) {
                        throw error("invalid \\u escape");
                    }
                    pos += 4;
                }
                default -> throw error("invalid escape \\" + e);
            }
        }
    }

    private void skipWhitespace() {
        while (pos < text.length() && " \t\r\n".indexOf(text.charAt(pos)) >= 0) {
            pos++;
        }
    }

    private void expect(char c) {
        if (next() != c) {
            pos--;
            throw error("expected '" + c + "'");
        }
    }

    private char peek() {
        if (pos >= text.length()) {
            throw error("unexpected end");
        }
        return text.charAt(pos);
    }

    private char next() {
        char c = peek();
        pos++;
        return c;
    }

    private IllegalArgumentException error(String what) {
        return new IllegalArgumentException("invalid outbox body at offset " + pos + ": " + what);
    }
}
