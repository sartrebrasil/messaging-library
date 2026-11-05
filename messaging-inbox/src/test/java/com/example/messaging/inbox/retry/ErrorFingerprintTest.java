package com.example.messaging.inbox.retry;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ErrorFingerprintTest {

    private static RuntimeException failHere(String message) {
        return new IllegalStateException(message);
    }

    @Test
    void sameOriginSameFingerprintRegardlessOfMessageAndWrapper() {
        String a = ErrorFingerprint.of(failHere("order 1"));
        String b = ErrorFingerprint.of(new RuntimeException("wrapper", failHere("order 2")));
        assertEquals(a, b);
        assertEquals(12, a.length());
        assertNotEquals(a, ErrorFingerprint.of(new IllegalArgumentException()));
        assertNull(ErrorFingerprint.of(null));
    }

    @Test
    void throwableTypesFailOnUnknownClass() {
        var loader = getClass().getClassLoader();
        assertEquals(List.of(IllegalStateException.class),
                ThrowableTypes.load(List.of(" java.lang.IllegalStateException "), loader));
        assertThrows(IllegalArgumentException.class, () -> ThrowableTypes.load(List.of("com.acme.Typo"), loader));
        assertThrows(IllegalArgumentException.class, () -> ThrowableTypes.load(List.of("java.lang.String"), loader));
    }
}
