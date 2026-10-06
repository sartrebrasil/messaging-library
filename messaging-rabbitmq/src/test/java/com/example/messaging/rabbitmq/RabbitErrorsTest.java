package com.example.messaging.rabbitmq;

import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class RabbitErrorsTest {

    @Test
    void mapsReplyCodesToLibraryExceptions() {
        assertInstanceOf(DestinationNotFoundException.class, RabbitErrors.map("op", closed(404, "NOT_FOUND - no queue 'x'")));
        assertInstanceOf(AccessDeniedException.class, RabbitErrors.map("op", closed(403, "ACCESS_REFUSED")));
        assertInstanceOf(MessageTooLargeException.class, RabbitErrors.map("op",
                closed(406, "PRECONDITION_FAILED - message size 17825792 is larger than configured max size 16777216")));
        MessagingException precondition = RabbitErrors.map("op", closed(406, "PRECONDITION_FAILED - inequivalent arg"));
        assertEquals(MessagingException.class, precondition.getClass());
        assertFalse(precondition.retryable());
        assertEquals("rabbitmq", precondition.provider());
    }

    @Test
    void unknownDeliveryTagIsLeaseExpiredOnlyWhenSettling() {
        IOException unknownTag = closed(406, "PRECONDITION_FAILED - unknown delivery tag 1");
        assertInstanceOf(LeaseExpiredException.class, RabbitErrors.mapSettle("ack", unknownTag));
        assertEquals(MessagingException.class, RabbitErrors.map("send", unknownTag).getClass());
    }

    @Test
    void networkFailuresAndConfirmTimeoutsAreRetryable() {
        assertTrue(RabbitErrors.map("op", new IOException("reset")).retryable());
        assertTrue(RabbitErrors.map("op", new TimeoutException("confirm")).retryable());
    }

    /** Como o cliente informa um channel fechado pelo broker: IOException com a causa. */
    private static IOException closed(int code, String text) {
        AMQP.Channel.Close close = new AMQP.Channel.Close.Builder().replyCode(code).replyText(text).build();
        return new IOException(new ShutdownSignalException(false, false, close, null));
    }
}
