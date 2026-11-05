package com.example.messaging.inbox.retry;

import java.util.ArrayList;
import java.util.List;

/**
 * Resolves exception class names from configuration. Fails at startup when a class does not exist:
 * a typo in an exception list is a silent bug.
 */
public final class ThrowableTypes {

    private ThrowableTypes() {
    }

    public static List<Class<? extends Throwable>> load(List<String> classNames, ClassLoader loader) {
        var out = new ArrayList<Class<? extends Throwable>>(classNames.size());
        for (String name : classNames) {
            String trimmed = name.trim();
            Class<?> c;
            try {
                c = Class.forName(trimmed, false, loader);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Exception class not found: " + trimmed, e);
            }
            if (!Throwable.class.isAssignableFrom(c)) {
                throw new IllegalArgumentException("Not a Throwable: " + trimmed);
            }
            out.add(c.asSubclass(Throwable.class));
        }
        return List.copyOf(out);
    }
}
