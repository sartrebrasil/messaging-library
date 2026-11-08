package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.RedriveRoutes;
import com.example.messaging.inbox.redrive.Filter;
import com.example.messaging.inbox.redrive.InboxRedrive;
import com.example.messaging.inbox.redrive.RedriveRequest;
import com.example.messaging.inbox.redrive.Source;
import com.example.messaging.inbox.redrive.Target;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.Access;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * {@code /actuator/inboxredrive} (ADR-0012). No access by default: it needs
 * {@code management.endpoint.inboxredrive.access=unrestricted} and {@code inboxredrive} in
 * {@code management.endpoints.web.exposure.include}. Securing it is the application's job.
 */
@Endpoint(id = "inboxredrive", defaultAccess = Access.NONE)
public class InboxRedriveEndpoint {

    static final int DEFAULT_MAX_PER_SECOND = 100;
    private static final String AVAILABLE = "available";

    private final InboxRedrive redrive;
    private final Supplier<Map<String, RedriveRoutes>> routes;

    InboxRedriveEndpoint(InboxRedrive redrive, Supplier<Map<String, RedriveRoutes>> routes) {
        this.redrive = redrive;
        this.routes = routes;
    }

    /** Receivers with their sources (available, or why not) and targets. */
    @ReadOperation
    public Map<String, Object> receivers() {
        Map<String, Object> receivers = new LinkedHashMap<>();
        routes.get().forEach((name, r) -> {
            Map<String, String> sources = new LinkedHashMap<>();
            sources.put(name(Source.FAILED), r.unavailable().getOrDefault(Source.FAILED, AVAILABLE));
            sources.put(name(Source.DEAD_LETTER), r.unavailable().getOrDefault(Source.DEAD_LETTER,
                    r.deadLetterReceivers() == null ? "not configured" : AVAILABLE));
            List<String> targets = new ArrayList<>();
            if (r.main() != null) {
                targets.add(name(Target.MAIN));
            }
            if (r.deadLetter() != null) {
                targets.add(name(Target.DEAD_LETTER));
            }
            Map<String, Object> receiver = new LinkedHashMap<>();
            receiver.put("sources", sources);
            receiver.put("targets", targets);
            receivers.put(name, receiver);
        });
        return receivers;
    }

    /** Synchronous: answers with the report when the run ends; 400 invalid, 404 unknown receiver, 409 running. */
    @WriteOperation
    public WebEndpointResponse<Object> redrive(@Selector String consumer, String source, String target, int limit,
                                               @Nullable Integer maxPerSecond, @Nullable Boolean dryRun,
                                               @Nullable String exception, @Nullable String fingerprint,
                                               @Nullable String reason, @Nullable String enqueuedBefore,
                                               @Nullable String enqueuedAfter) {
        try {
            RedriveRequest request = new RedriveRequest(consumer, parse(Source.class, "source", source),
                    parse(Target.class, "target", target), limit,
                    new Filter(exception, fingerprint, reason, instant("enqueuedBefore", enqueuedBefore),
                            instant("enqueuedAfter", enqueuedAfter)),
                    maxPerSecond == null ? DEFAULT_MAX_PER_SECOND : maxPerSecond, Boolean.TRUE.equals(dryRun));
            return new WebEndpointResponse<>(redrive.redrive(request), WebEndpointResponse.STATUS_OK);
        } catch (IllegalArgumentException e) {
            return error(e, WebEndpointResponse.STATUS_BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return error(e, WebEndpointResponse.STATUS_NOT_FOUND);
        } catch (IllegalStateException e) {
            return error(e, 409);
        }
    }

    private static <E extends Enum<E>> E parse(Class<E> type, String field, String value) {
        try {
            return Enum.valueOf(type, value.trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(field + " must be one of " + List.of(type.getEnumConstants())
                    .stream().map(InboxRedriveEndpoint::name).toList() + ": " + value);
        }
    }

    private static Instant instant(String field, String value) {
        try {
            return value == null ? null : Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(field + " must be an ISO-8601 instant: " + value);
        }
    }

    private static WebEndpointResponse<Object> error(RuntimeException e, int status) {
        return new WebEndpointResponse<>(Map.of("error", String.valueOf(e.getMessage())), status);
    }

    private static String name(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }
}
