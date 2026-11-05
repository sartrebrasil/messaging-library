package com.example.messaging.inbox.retry;

import com.example.messaging.inbox.NonRetryable;
import com.example.messaging.inbox.RetryAfterHint;
import com.example.messaging.inbox.SkipImmediate;

import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Classifies exceptions walking the {@code cause} chain, so wrappers such as {@code CompletionException}
 * or framework exceptions do not hide the real cause (ADR-0014).
 *
 * <p>Precedence:
 * <ol>
 *   <li>{@link NonRetryable} or the {@code non-retryable} list in any link: NON_RETRYABLE</li>
 *   <li>Allowlist mode on and no link in the allowlist nor in {@code skip-immediate}: NON_RETRYABLE</li>
 *   <li>{@link SkipImmediate} or the {@code skip-immediate} list in any link: SKIP_IMMEDIATE</li>
 *   <li>Otherwise: RETRYABLE</li>
 * </ol>
 * An explicit {@code HandlerResult} wins over all of this and is handled before.
 */
public final class ExceptionClassifier {

    static final int MAX_CAUSE_DEPTH = 32;

    private final RetryPolicy policy;

    public ExceptionClassifier(RetryPolicy policy) {
        this.policy = policy;
    }

    public Classification classify(Throwable error) {
        List<Throwable> chain = causeChain(error);
        for (Throwable t : chain) {
            if (annotated(t, NonRetryable.class) || matchesAny(t, policy.nonRetryable())) {
                return Classification.NON_RETRYABLE;
            }
        }
        boolean skip = false;
        for (Throwable t : chain) {
            if (annotated(t, SkipImmediate.class) || matchesAny(t, policy.skipImmediate())) {
                skip = true;
                break;
            }
        }
        if (!policy.retryable().isEmpty() && !skip) {
            boolean allowed = false;
            for (Throwable t : chain) {
                if (matchesAny(t, policy.retryable())) {
                    allowed = true;
                    break;
                }
            }
            if (!allowed) {
                return Classification.NON_RETRYABLE;
            }
        }
        return skip ? Classification.SKIP_IMMEDIATE : Classification.RETRYABLE;
    }

    /** First valid {@link RetryAfterHint} in the chain. */
    public Optional<Duration> retryAfterHint(Throwable error) {
        if (error == null) {
            return Optional.empty();
        }
        for (Throwable t : causeChain(error)) {
            if (t instanceof RetryAfterHint hint) {
                Duration d = hint.retryAfter();
                if (d != null && !d.isNegative()) {
                    return Optional.of(d);
                }
            }
        }
        return Optional.empty();
    }

    static List<Throwable> causeChain(Throwable error) {
        var chain = new ArrayList<Throwable>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable t = error;
        while (t != null && chain.size() < MAX_CAUSE_DEPTH && seen.add(t)) {
            chain.add(t);
            t = t.getCause();
        }
        return chain;
    }

    static Throwable rootCause(Throwable error) {
        List<Throwable> chain = causeChain(error);
        return chain.get(chain.size() - 1);
    }

    private static boolean annotated(Throwable t, Class<? extends Annotation> annotation) {
        return t.getClass().isAnnotationPresent(annotation);
    }

    private static boolean matchesAny(Throwable t, List<Class<? extends Throwable>> types) {
        for (Class<? extends Throwable> type : types) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }
}
