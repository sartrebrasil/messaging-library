package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.redrive.InboxRedrive;
import com.example.messaging.inbox.redrive.RedriveReport;
import com.example.messaging.inbox.redrive.RedriveRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/** {@code inbox.messages.redriven{consumer,source,target,result}} from each report (ADR-0012); no redrive id in the tags. */
final class MeteredInboxRedrive implements InboxRedrive {

    private final InboxRedrive delegate;
    private final MeterRegistry meters;

    MeteredInboxRedrive(InboxRedrive delegate, MeterRegistry meters) {
        this.delegate = delegate;
        this.meters = meters;
    }

    @Override
    public RedriveReport redrive(RedriveRequest request) {
        long start = System.nanoTime();
        RedriveReport report = delegate.redrive(request);
        Duration took = Duration.ofNanos(System.nanoTime() - start);
        count(request, "moved", report.moved());
        count(request, "skipped", report.skipped());
        count(request, "error", report.errors());
        // how the run ended: a run cut short by a failed receive is otherwise just a small one. Requests the
        // redrive rejects are not counted: the consumer in them is the caller's input, so it is not a safe tag
        Timer.builder("inbox.redrive.duration").description("one redrive run, from the request to its report")
                .tag("consumer", request.consumer())
                .tag("source", request.source().name().toLowerCase())
                .tag("target", request.target().name().toLowerCase())
                .register(meters).record(took);
        Counter.builder("inbox.redrives.ended").description("redrive runs, by how they ended")
                .tag("consumer", request.consumer())
                .tag("source", request.source().name().toLowerCase())
                .tag("target", request.target().name().toLowerCase())
                .tag("end", report.end().name().toLowerCase())
                .register(meters).increment();
        return report;
    }

    private void count(RedriveRequest request, String result, int amount) {
        // registered even at 0, so the series exists from the first run
        Counter.builder("inbox.messages.redriven")
                .description("messages read by a redrive, by result")
                .tag("consumer", request.consumer())
                .tag("source", request.source().name().toLowerCase())
                .tag("target", request.target().name().toLowerCase())
                .tag("result", result)
                .register(meters)
                .increment(amount);
    }
}
