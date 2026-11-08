package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.redrive.InboxRedrive;
import com.example.messaging.inbox.redrive.RedriveReport;
import com.example.messaging.inbox.redrive.RedriveRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** {@code inbox.redrive{consumer,source,target,result}} from each report (ADR-0012); no redrive id in the tags. */
final class MeteredInboxRedrive implements InboxRedrive {

    private final InboxRedrive delegate;
    private final MeterRegistry meters;

    MeteredInboxRedrive(InboxRedrive delegate, MeterRegistry meters) {
        this.delegate = delegate;
        this.meters = meters;
    }

    @Override
    public RedriveReport redrive(RedriveRequest request) {
        RedriveReport report = delegate.redrive(request);
        count(request, "moved", report.moved());
        count(request, "skipped", report.skipped());
        count(request, "error", report.errors());
        return report;
    }

    private void count(RedriveRequest request, String result, int amount) {
        // registered even at 0, so the series exists from the first run
        Counter.builder("inbox.redrive")
                .description("messages read by a redrive, by result")
                .tag("consumer", request.consumer())
                .tag("source", request.source().name().toLowerCase())
                .tag("target", request.target().name().toLowerCase())
                .tag("result", result)
                .register(meters)
                .increment(amount);
    }
}
