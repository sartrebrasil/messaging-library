package com.example.messaging.inbox.spring;

import com.example.messaging.OutgoingMessage;
import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.InboxResultHandler;
import com.example.messaging.memory.InMemoryMessaging;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.C;
import static com.example.messaging.inbox.spring.InboxAutoConfigurationTest.await;
import static org.assertj.core.api.Assertions.assertThat;

/** ADR-0007: exported metric names and trace continuity. */
class ObservabilityTest {

    private static final String BODY_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String BODY_SPAN_ID = "00f067aa0ba902b7";
    private static final String TRANSPORT_TRACE = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    static final List<TraceContext> SEEN = new CopyOnWriteArrayList<>();
    static OtelTracer tracer;

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(InboxAutoConfiguration.class))
            .withUserConfiguration(InboxAutoConfigurationTest.Broker.class, Telemetry.class)
            .withPropertyValues(C + "destination=orders", C + "failed=orders-failed", C + "dead-letter=orders-dlq",
                    C + "poll-wait=100ms", C + "shutdown-timeout=2s");

    @Test
    void metricsAreExportedWithTheInboxNames() {
        runner.withUserConfiguration(Rejecting.class).run(context -> {
            InMemoryMessaging messaging = context.getBean(InMemoryMessaging.class);
            messaging.queueSender("orders").send(OutgoingMessage.ofText(InboxAutoConfigurationTest.ORDER));
            PrometheusMeterRegistry registry = context.getBean(PrometheusMeterRegistry.class);
            // delivered() runs right after the ack: wait for it rather than for the queue
            await(() -> registry.scrape().contains("inbox_failed_total"));

            String scrape = registry.scrape();
            assertThat(scrape)
                    .contains("inbox_messages_received_total{consumer=\"billing-orders\",destination=\"billing-orders\"} 1.0")
                    .contains("inbox_messages_processed_total{consumer=\"billing-orders\",destination=\"billing-orders\",outcome=\"failed\"} 1.0")
                    .contains("inbox_failed_total{consumer=\"billing-orders\",destination=\"billing-orders\",exception=\"none\",reason=\"rejected\"} 1.0")
                    .contains("inbox_processing_duration_seconds_count{consumer=\"billing-orders\",destination=\"billing-orders\",outcome=\"failed\"} 1")
                    .contains("inbox_last_receive_age_seconds{consumer=\"billing-orders\"}")
                    .doesNotContain("_total_total");
        });
    }

    @Test
    void theHandlerRunsInsideTheTraceOfTheOutboxBody() {
        SEEN.clear();
        runner.withUserConfiguration(Tracing.class).run(context -> {
            String body = InboxAutoConfigurationTest.ORDER.replace("\"payload\"",
                    "\"headers\":{\"traceparent\":\"00-" + BODY_TRACE_ID + "-" + BODY_SPAN_ID + "-01\"},\"payload\"");
            context.getBean(InMemoryMessaging.class).queueSender("orders")
                    .send(OutgoingMessage.ofText(body).withTraceparent(TRANSPORT_TRACE));

            await(() -> SEEN.size() == 1);
            TraceContext span = SEEN.getFirst();
            assertThat(span.traceId()).isEqualTo(BODY_TRACE_ID);
            assertThat(span.parentId()).isEqualTo(BODY_SPAN_ID);
            assertThat(span.spanId()).isNotEqualTo(BODY_SPAN_ID);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class Telemetry {

        @Bean
        PrometheusMeterRegistry prometheusMeterRegistry() {
            return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        }

        @Bean
        ObservationRegistry observationRegistry() {
            var otel = SdkTracerProvider.builder().build().get("inbox-test");
            tracer = new OtelTracer(otel, new OtelCurrentTraceContext(), event -> { });
            var propagator = new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel);
            ObservationRegistry registry = ObservationRegistry.create();
            registry.observationConfig().observationHandler(new PropagatingReceiverTracingObservationHandler<>(tracer, propagator));
            return registry;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Rejecting {
        @Bean
        InboxResultHandler<String> billingOrders() {
            return message -> HandlerResult.reject("not today");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Tracing {
        @Bean
        InboxResultHandler<String> billingOrders() {
            return message -> {
                SEEN.add(tracer.currentSpan().context());
                return null;
            };
        }
    }
}
