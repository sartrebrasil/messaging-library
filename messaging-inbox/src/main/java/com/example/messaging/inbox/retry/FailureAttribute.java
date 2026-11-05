package com.example.messaging.inbox.retry;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Failure metadata carried in the {@value #NAME} attribute of DLQ, level 3 and {@code failed} copies
 * (ADR-0002, ADR-0017). The payload is never touched.
 *
 * <p>Format: {@code consumer=<c>;destination=<d>;exception=<class>;fingerprint=<hex>;first_failure_at=<ISO-8601>}.
 *
 * @param exception   root cause class name, or {@code none}
 * @param fingerprint {@code null} when there is no exception
 */
public record FailureAttribute(String consumer,
                               String destination,
                               String exception,
                               String fingerprint,
                               Instant firstFailureAt) {

    public static final String NAME = "inbox_failure";

    /** messaging-lib limit for an attribute value (ASCII, so characters = bytes). */
    public static final int MAX_LENGTH = 1024;

    public FailureAttribute {
        requireSafe(consumer, "consumer");
        requireSafe(destination, "destination");
        requireSafe(exception, "exception");
        Objects.requireNonNull(firstFailureAt, "firstFailureAt");
    }

    /**
     * Builds the attribute for a new copy, keeping {@code first_failure_at} from {@code previous}. When the value
     * would exceed {@value #MAX_LENGTH} characters, {@code exception} becomes the simple class name, then
     * {@code destination} and {@code consumer} are cut at the end (ADR-0002).
     */
    public static FailureAttribute next(String previous, String consumer, String destination,
                                        Throwable cause, Instant now) {
        Instant first = parse(previous).map(FailureAttribute::firstFailureAt).orElse(now);
        String exception = cause == null ? "none" : ExceptionClassifier.rootCause(cause).getClass().getName();
        var attr = new FailureAttribute(consumer, destination, exception, ErrorFingerprint.of(cause), first);
        int excess = attr.format().length() - MAX_LENGTH;
        if (excess <= 0) {
            return attr;
        }
        String simple = exception.substring(exception.lastIndexOf('.') + 1);
        excess -= exception.length() - simple.length();
        String dest = cut(destination, Math.max(0, excess));
        excess -= destination.length() - dest.length();
        String cons = cut(consumer, Math.max(0, excess));
        return new FailureAttribute(cons, dest, simple, attr.fingerprint(), first);
    }

    private static String cut(String value, int excess) {
        return value.substring(0, Math.max(1, value.length() - excess));
    }

    /** Empty when {@code value} is {@code null} or lacks a required field. */
    public static Optional<FailureAttribute> parse(String value) {
        if (value == null) {
            return Optional.empty();
        }
        var fields = new LinkedHashMap<String, String>();
        for (String part : value.split(";")) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                fields.put(part.substring(0, eq).trim(), part.substring(eq + 1).trim());
            }
        }
        String consumer = fields.get("consumer");
        String destination = fields.get("destination");
        String first = fields.get("first_failure_at");
        if (consumer == null || destination == null || first == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(new FailureAttribute(consumer, destination, fields.getOrDefault("exception", "none"),
                    fields.get("fingerprint"), Instant.parse(first)));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }

    public String format() {
        var sb = new StringBuilder()
                .append("consumer=").append(consumer)
                .append(";destination=").append(destination)
                .append(";exception=").append(exception);
        if (fingerprint != null) {
            sb.append(";fingerprint=").append(fingerprint);
        }
        return sb.append(";first_failure_at=").append(firstFailureAt).toString();
    }

    private static void requireSafe(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.indexOf(';') >= 0 || value.indexOf('=') >= 0) {
            throw new IllegalArgumentException(field + " must not contain ';' or '=': " + value);
        }
    }
}
