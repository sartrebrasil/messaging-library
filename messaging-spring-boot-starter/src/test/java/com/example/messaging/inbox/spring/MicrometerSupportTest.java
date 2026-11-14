package com.example.messaging.inbox.spring;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.Delivery;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.Outcome;
import com.example.messaging.spring.MessagingProperties;
import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code messaging.system} of the consumer spans (N6): the provider type, as the transport meters tag it. */
class MicrometerSupportTest {

    private static Provider provider(Type type) {
        return new Provider(type, null, null, null, null, null, "p", null, null, null, null);
    }

    @Test
    void systemIsTheTypeOfTheProviderTheReceiverUses() {
        var properties = new MessagingProperties(Map.of("main", provider(Type.AWS), "bus", provider(Type.ACTIVEMQ_CLASSIC)),
                Map.of());

        assertThat(MicrometerSupport.system("bus", properties)).isEqualTo("activemq-classic");
        assertThat(MicrometerSupport.system("main", properties)).isEqualTo("aws");
        // no provider named, two to choose from: cannot tell
        assertThat(MicrometerSupport.system(null, properties)).isEqualTo("messaging");
    }

    @Test
    void theOnlyProviderIsTheDefault() {
        var properties = new MessagingProperties(Map.of("main", provider(Type.AZURE)), Map.of());

        assertThat(MicrometerSupport.system(null, properties)).isEqualTo("azure");
        assertThat(MicrometerSupport.system(null, null)).isEqualTo("messaging");
    }

    @Test
    void theConsumerObservationCarriesIt() {
        Map<String, String> seen = new HashMap<>();
        ObservationRegistry observations = ObservationRegistry.create();
        observations.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
            @Override
            public boolean supportsContext(Observation.Context context) {
                return true;
            }

            @Override
            public void onStop(Observation.Context context) {
                context.getLowCardinalityKeyValues().forEach(kv -> seen.put(kv.getKey(), kv.getValue()));
                context.getHighCardinalityKeyValues().forEach(kv -> seen.put(kv.getKey(), kv.getValue()));
            }
        });
        var observer = new MicrometerConsumerObserver("billing-orders", "billing-orders.dead-letter", "gcp", null,
                observations, Clock.systemUTC(), true);
        var inbox = new InboxMessage("m-1", null, null, null, null, null, null, null, Map.of(), new byte[0],
                "billing-orders", Delivery.first());

        observer.execution(inbox, () -> new Outcome.Ack(Outcome.AckReason.SUCCESS));

        assertThat(seen).containsEntry("messaging.system", "gcp")
                .containsEntry("messaging.consumer.group.name", "billing-orders")
                .containsEntry("messaging.destination.name", "billing-orders.dead-letter");
    }
}
