package com.example.messaging.inbox.retry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Groups "equal" failures for DLQ inspection and selective redrive (ADR-0017).
 *
 * <p>Uses the root cause: class plus the first {@value #FRAMES} frames as {@code class#method}, without line
 * numbers, so the fingerprint survives trivial code changes. Never use it as a metric tag.
 */
public final class ErrorFingerprint {

    static final int FRAMES = 5;
    static final int LENGTH = 12;

    private ErrorFingerprint() {
    }

    /** {@code null} when {@code error} is {@code null}. */
    public static String of(Throwable error) {
        if (error == null) {
            return null;
        }
        Throwable root = ExceptionClassifier.rootCause(error);
        var sb = new StringBuilder(root.getClass().getName());
        StackTraceElement[] st = root.getStackTrace();
        for (int i = 0; i < Math.min(FRAMES, st.length); i++) {
            sb.append('|').append(st[i].getClassName()).append('#').append(st[i].getMethodName());
        }
        return HexFormat.of().formatHex(sha1(sb.toString())).substring(0, LENGTH);
    }

    private static byte[] sha1(String s) {
        try {
            return MessageDigest.getInstance("SHA-1").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 unavailable", e);
        }
    }
}
