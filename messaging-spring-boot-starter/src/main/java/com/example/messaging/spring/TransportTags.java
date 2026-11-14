package com.example.messaging.spring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** Tag values and counters shared by the transport meters. */
final class TransportTags {

    private TransportTags() {
    }

    /** The {@code outcome} of a batch call: some items failing is neither a success nor an error of the call. */
    static String batchOutcome(int failed, int total) {
        return failed == 0 ? "success" : failed >= total ? "error" : "partial";
    }

    /**
     * {@code messaging.messages.failed}: one count per message whose call failed, whether the whole call threw or
     * only some items of a batch failed.
     */
    static void failed(MeterRegistry meters, String provider, String destination, String operation, Throwable failure,
                       int messages) {
        if (messages > 0) {
            Counter.builder("messaging.messages.failed").description("messages whose send, ack, nack or dead-letter failed")
                    .tags("provider", provider, "destination", destination, "operation", operation, "exception",
                            exception(failure))
                    .register(meters).increment(messages);
        }
    }

    /**
     * The {@code exception} tag, as in the inbox meters: the simple class name of what the call threw, {@code none}
     * when it did not throw. The adapters map the SDK errors to {@code MessagingException} subclasses, so the set of
     * values stays small.
     */
    static String exception(Throwable thrown) {
        if (thrown == null) {
            return "none";
        }
        String name = thrown.getClass().getSimpleName();
        return name.isEmpty() ? thrown.getClass().getName() : name;
    }
}
