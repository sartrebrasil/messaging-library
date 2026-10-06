package com.example.messaging.jms;

import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessagingException;
import com.example.messaging.ThrottledException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.ResourceAllocationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class JmsErrorsTest {

    @Test
    void mapsJmsExceptionsToLibraryExceptions() {
        assertInstanceOf(DestinationNotFoundException.class, JmsErrors.map("op", new InvalidDestinationException("x")));
        assertInstanceOf(AccessDeniedException.class, JmsErrors.map("op", new JMSSecurityException("x")));
        assertInstanceOf(ThrottledException.class, JmsErrors.map("op", new ResourceAllocationException("x")));
        assertEquals("jms", JmsErrors.map("op", new JMSException("x")).provider());
    }

    @Test
    void closedSessionIsLeaseExpiredOnlyWhenSettling() {
        var closed = new jakarta.jms.IllegalStateException("The Consumer is closed");
        assertInstanceOf(LeaseExpiredException.class, JmsErrors.mapSettle("ack", closed));
        assertEquals(MessagingException.class, JmsErrors.map("send", closed).getClass());
    }

    @Test
    void onlyConnectionFailuresAreRetryable() {
        JMSException lost = new JMSException("conexão caiu");
        lost.initCause(new IOException("reset"));
        assertTrue(JmsErrors.map("op", lost).retryable());
        assertFalse(JmsErrors.map("op", new JMSException("propriedade inválida")).retryable());
    }
}
