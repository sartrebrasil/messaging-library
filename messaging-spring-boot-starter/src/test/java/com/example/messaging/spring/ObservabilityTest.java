package com.example.messaging.spring;

import com.example.messaging.Capabilities;
import com.example.messaging.MessageSender;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.SenderContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservabilityTest {

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    /** Faz o papel do handler de propagação do micrometer-tracing: grava o traceparent no carrier. */
    private static final class PropagatingHandler implements ObservationHandler<SenderContext<Object>> {
        @Override
        public void onStart(SenderContext<Object> context) {
            context.getSetter().set(context.getCarrier(), "traceparent", TRACEPARENT);
        }

        @Override
        public boolean supportsContext(Observation.Context context) {
            return context instanceof SenderContext;
        }
    }

    private static final class RecordingSender implements MessageSender {
        final List<OutgoingMessage> sent = new ArrayList<>();
        boolean fail;

        @Override
        public SendResult send(OutgoingMessage message) {
            if (fail) {
                throw new IllegalStateException("falhou");
            }
            sent.add(message);
            return new SendResult("id");
        }

        @Override
        public void checkAccess() {
        }

        @Override
        public Capabilities capabilities() {
            return new Capabilities(1, 1, false, Duration.ZERO, false, false, false, false);
        }

        @Override
        public void close() {
        }
    }

    @Test
    void sendInjectsTraceparentFromObservation() {
        ObservationRegistry observations = ObservationRegistry.create();
        observations.observationConfig().observationHandler(new PropagatingHandler());
        RecordingSender delegate = new RecordingSender();
        MessageSender sender = new ObservedMessageSender(delegate, "aws", "pedidos", null, observations);

        sender.send(OutgoingMessage.ofText("x"));
        sender.sendAll(List.of(OutgoingMessage.ofText("y")));
        String own = "00-11111111111111111111111111111111-2222222222222222-01";
        sender.send(OutgoingMessage.ofText("z").withTraceparent(own));

        assertThat(delegate.sent).extracting(OutgoingMessage::traceparent).containsExactly(TRACEPARENT, TRACEPARENT, own);
    }

    @Test
    void sendRecordsTimerWithOutcome() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        RecordingSender delegate = new RecordingSender();
        MessageSender sender = new ObservedMessageSender(delegate, "aws", "pedidos", meters, null);

        sender.send(OutgoingMessage.ofText("x"));
        delegate.fail = true;
        assertThatThrownBy(() -> sender.send(OutgoingMessage.ofText("x"))).isInstanceOf(IllegalStateException.class);

        assertThat(meters.get("messaging.send").tags("provider", "aws", "destination", "pedidos", "outcome", "success")
                .timer().count()).isEqualTo(1);
        assertThat(meters.get("messaging.send").tags("outcome", "error").timer().count()).isEqualTo(1);
    }
}
