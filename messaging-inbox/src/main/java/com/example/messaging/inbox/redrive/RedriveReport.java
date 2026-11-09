package com.example.messaging.inbox.redrive;

import com.example.messaging.inbox.PassEnd;

import java.util.Map;

/**
 * @param end           why the run stopped
 * @param skipped       read but not moved (filtered out, or a dry run): back at the end of the source
 * @param errors        copy failed: the message stays in the source
 * @param byException   every message read, by {@code inbox_failure} exception ({@code none} without it)
 * @param byFingerprint every message read, by {@code inbox_failure} fingerprint ({@code none} without it)
 */
public record RedriveReport(String redriveId, PassEnd end, int moved, int skipped, int errors,
                            Map<String, Integer> byException, Map<String, Integer> byFingerprint) {

    public RedriveReport {
        byException = Map.copyOf(byException);
        byFingerprint = Map.copyOf(byFingerprint);
    }
}
