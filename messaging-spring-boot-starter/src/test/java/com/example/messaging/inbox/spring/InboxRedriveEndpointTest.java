package com.example.messaging.inbox.spring;

import com.example.messaging.OutgoingMessage;
import com.example.messaging.inbox.PassEnd;
import com.example.messaging.inbox.redrive.InboxRedrive;
import com.example.messaging.inbox.redrive.RedriveReport;
import com.example.messaging.inbox.redrive.RedriveRequest;
import com.example.messaging.inbox.redrive.Source;
import com.example.messaging.inbox.redrive.Target;
import com.example.messaging.memory.InMemoryMessaging;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.C;
import static org.assertj.core.api.Assertions.assertThat;

/** Actuator endpoint {@code inboxredrive} (ADR-0012): off by default, on only with access and exposure. */
class InboxRedriveEndpointTest {

    private static final String ACCESS = "management.endpoint.inboxredrive.access=unrestricted";
    private static final String EXPOSURE = "management.endpoints.web.exposure.include=inboxredrive";

    // as in a SpringApplication, where access=unrestricted binds case-insensitively
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(c -> c.getEnvironment().setConversionService(new ApplicationConversionService()))
            .withConfiguration(AutoConfigurations.of(InboxAutoConfiguration.class,
                    InboxRedriveEndpointAutoConfiguration.class, EndpointAutoConfiguration.class))
            .withUserConfiguration(InboxAutoConfigurationTest.Broker.class, InboxAutoConfigurationTest.Functional.class)
            .withPropertyValues(C + "destination=orders", C + "failed=orders-failed", C + "dead-letter=orders-dlq",
                    C + "poll-wait=100ms", C + "shutdown-timeout=2s");

    private static final RedriveReport REPORT = new RedriveReport("r-1", PassEnd.EMPTY, 1, 0, 0,
            Map.of("none", 1), Map.of("none", 1));

    private static InboxRedriveEndpoint endpoint(InboxRedrive redrive) {
        return new InboxRedriveEndpoint(redrive, Map::of);
    }

    private static WebEndpointResponse<Object> post(InboxRedriveEndpoint endpoint, String source, String target) {
        return endpoint.redrive("billing-orders", source, target, 10, null, null, null, null, null, null, null);
    }

    @Test
    void offByDefault() {
        runner.run(context -> assertThat(context).doesNotHaveBean(InboxRedriveEndpoint.class));
    }

    @Test
    void needsBothKeys() {
        runner.withPropertyValues(ACCESS)
                .run(context -> assertThat(context).doesNotHaveBean(InboxRedriveEndpoint.class));
        runner.withPropertyValues(EXPOSURE)
                .run(context -> assertThat(context).doesNotHaveBean(InboxRedriveEndpoint.class));
        runner.withPropertyValues(ACCESS, EXPOSURE)
                .run(context -> assertThat(context).hasSingleBean(InboxRedriveEndpoint.class));
        // the global default does not open it: only its own key (review I5)
        runner.withPropertyValues("management.endpoints.access.default=unrestricted", EXPOSURE)
                .run(context -> assertThat(context).doesNotHaveBean(InboxRedriveEndpoint.class));
        runner.withPropertyValues("management.endpoint.inboxredrive.access=read-only", EXPOSURE)
                .run(context -> assertThat(context).doesNotHaveBean(InboxRedriveEndpoint.class));
    }

    @Test
    void listsTheReceivers() {
        runner.withPropertyValues(ACCESS, EXPOSURE).run(context -> {
            Map<String, Object> receivers = context.getBean(InboxRedriveEndpoint.class).receivers();

            assertThat(receivers).containsOnlyKeys("billing-orders");
            assertThat(receivers.get("billing-orders").toString())
                    .contains("failed=available", "dead_letter=available", "targets=[main, dead_letter]");
        });
    }

    @Test
    void redriveReturnsTheReport() {
        runner.withPropertyValues(ACCESS, EXPOSURE).run(context -> {
            context.getBean(InMemoryMessaging.class).queueSender("orders-failed")
                    .send(OutgoingMessage.ofText(InboxAutoConfigurationTest.ORDER));
            Thread.sleep(5);

            WebEndpointResponse<Object> response = context.getBean(InboxRedriveEndpoint.class).redrive(
                    "billing-orders", "failed", "main", 10, null, true, null, null, null, null, null);

            assertThat(response.getStatus()).isEqualTo(200);
            RedriveReport report = (RedriveReport) response.getBody();
            assertThat(report.skipped()).isEqualTo(1);
            assertThat(report.moved()).isZero();
        });
    }

    @Test
    void defaultsAndConversions() {
        AtomicReference<RedriveRequest> seen = new AtomicReference<>();
        InboxRedriveEndpoint endpoint = endpoint(request -> {
            seen.set(request);
            return REPORT;
        });

        WebEndpointResponse<Object> response = endpoint.redrive("billing-orders", "DEAD_LETTER", "main", 50, null,
                null, "java.lang.IllegalStateException", "abc", "rejected", "2026-11-07T10:00:00Z", null);

        assertThat(response.getStatus()).isEqualTo(200);
        RedriveRequest request = seen.get();
        assertThat(request.source()).isEqualTo(Source.DEAD_LETTER);
        assertThat(request.target()).isEqualTo(Target.MAIN);
        assertThat(request.maxPerSecond()).isEqualTo(100);
        assertThat(request.dryRun()).isFalse();
        assertThat(request.filter().exception()).isEqualTo("java.lang.IllegalStateException");
        assertThat(request.filter().enqueuedBefore()).hasToString("2026-11-07T10:00:00Z");
    }

    @Test
    void badRequest() {
        InboxRedriveEndpoint endpoint = endpoint(request -> {
            throw new IllegalArgumentException("bad");
        });

        WebEndpointResponse<Object> refused = post(endpoint, "failed", "dead_letter");
        assertThat(refused.getStatus()).isEqualTo(400);
        assertThat(refused.getBody()).isEqualTo(Map.of("error", "bad"));
        assertThat(post(endpoint, "nowhere", "main").getStatus()).isEqualTo(400);
        assertThat(endpoint.redrive("billing-orders", "failed", "main", 0, null, null, null, null, null, null, null)
                .getStatus()).isEqualTo(400);
        assertThat(endpoint.redrive("billing-orders", "failed", "main", 10, null, null, null, null, null,
                "yesterday", null).getStatus()).isEqualTo(400);
    }

    @Test
    void unknownConsumer() {
        InboxRedriveEndpoint endpoint = endpoint(request -> {
            throw new NoSuchElementException("unknown receiver: billing-orders");
        });

        assertThat(post(endpoint, "failed", "main").getStatus()).isEqualTo(404);
    }

    /** A second call while one runs: 409 (review focus 3; the core test proves the first is not affected). */
    @Test
    void concurrent() {
        InboxRedriveEndpoint endpoint = endpoint(request -> {
            throw new IllegalStateException("a redrive of failed of receiver billing-orders is in progress");
        });

        WebEndpointResponse<Object> response = post(endpoint, "failed", "main");

        assertThat(response.getStatus()).isEqualTo(409);
        assertThat(response.getBody().toString()).contains("in progress");
    }

    /** The Actuator binds the JSON body by parameter name: compiled with -parameters. */
    @Test
    void operationParametersKeepTheirNames() {
        java.lang.reflect.Method redrive = java.util.Arrays.stream(InboxRedriveEndpoint.class.getMethods())
                .filter(m -> m.getName().equals("redrive")).findFirst().orElseThrow();

        assertThat(java.util.Arrays.stream(redrive.getParameters()).map(java.lang.reflect.Parameter::getName))
                .containsExactly("consumer", "source", "target", "limit", "maxPerSecond", "dryRun", "exception",
                        "fingerprint", "reason", "enqueuedBefore", "enqueuedAfter");
    }
}
