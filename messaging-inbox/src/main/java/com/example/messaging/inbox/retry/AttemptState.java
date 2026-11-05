package com.example.messaging.inbox.retry;

/**
 * Attempts already spent on the delayed level ({@code l2}) and on DLQ passes ({@code l3}), carried in the
 * {@value #ATTRIBUTE} attribute as {@code l2=<n>;l3=<n>} (ADR-0002). After a redrive the value also carries
 * {@code ;redrive=<id>}, which follows the message through every later copy (ADR-0012, ADR-0015).
 *
 * @param redrive id of the redrive that brought the message back, or {@code null}
 */
public record AttemptState(int delayed, int deadLetter, String redrive) {

    public static final String ATTRIBUTE = "inbox_attempts";

    private static final AttemptState INITIAL = new AttemptState(0, 0);

    public AttemptState {
        if (delayed < 0 || deadLetter < 0) {
            throw new IllegalArgumentException("negative attempts: l2=" + delayed + ", l3=" + deadLetter);
        }
        if (redrive != null && (redrive.isEmpty() || redrive.indexOf(';') >= 0 || redrive.indexOf('=') >= 0)) {
            throw new IllegalArgumentException("invalid redrive id: " + redrive);
        }
    }

    public AttemptState(int delayed, int deadLetter) {
        this(delayed, deadLetter, null);
    }

    public static AttemptState initial() {
        return INITIAL;
    }

    /** State of a copy sent by a redrive: attempts start over. */
    public static AttemptState redriven(String redriveId) {
        return new AttemptState(0, 0, redriveId);
    }

    /**
     * {@code l2 = l2 from the attribute + (deliveryCount - 1)}. The in-memory attempts of {@code hold} are
     * added by the caller with {@link #nextDelayed()}.
     *
     * @param attribute     value of {@value #ATTRIBUTE}, or {@code null}; unreadable parts are ignored
     * @param deliveryCount broker delivery count on the main destination; {@code 0} or less when unknown, and
     *                      when reading the DLQ, where redeliveries are not delayed attempts
     */
    public static AttemptState from(String attribute, int deliveryCount) {
        int l2 = 0;
        int l3 = 0;
        String redrive = null;
        if (attribute != null) {
            for (String part : attribute.split(";")) {
                int eq = part.indexOf('=');
                if (eq < 0) {
                    continue;
                }
                String key = part.substring(0, eq).trim();
                String value = part.substring(eq + 1).trim();
                switch (key) {
                    case "l2" -> l2 = parseCount(value);
                    case "l3" -> l3 = parseCount(value);
                    case "redrive" -> redrive = value.isEmpty() ? null : value;
                    default -> {
                    }
                }
            }
        }
        return new AttemptState(l2 + Math.max(0, deliveryCount - 1), l3, redrive);
    }

    public boolean redriven() {
        return redrive != null;
    }

    public AttemptState nextDelayed() {
        return new AttemptState(delayed + 1, deadLetter, redrive);
    }

    public AttemptState nextDeadLetter() {
        return new AttemptState(delayed, deadLetter + 1, redrive);
    }

    public String toAttribute() {
        String value = "l2=" + delayed + ";l3=" + deadLetter;
        return redrive == null ? value : value + ";redrive=" + redrive;
    }

    private static int parseCount(String v) {
        try {
            return Math.max(0, Integer.parseInt(v));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
