package com.example.messaging.aws;

import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ThrottledException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AwsErrorsTest {

    @Test
    void mapsErrorCodesToLibraryExceptions() {
        assertInstanceOf(DestinationNotFoundException.class, map("AWS.SimpleQueueService.NonExistentQueue", "x", 400));
        assertInstanceOf(DestinationNotFoundException.class, map("NotFound", "Topic does not exist", 404));
        assertInstanceOf(AccessDeniedException.class, map("AuthorizationError", "x", 403));
        assertInstanceOf(ThrottledException.class, map("RequestThrottled", "x", 400));
        assertInstanceOf(LeaseExpiredException.class, map("ReceiptHandleIsInvalid", "x", 400));
        assertInstanceOf(LeaseExpiredException.class, map("MessageNotInflight", "x", 400));
        assertInstanceOf(LeaseExpiredException.class,
                map("InvalidParameterValue", "Value x for parameter ReceiptHandle is invalid. Reason: The receipt handle has expired.", 400));
        assertInstanceOf(MessageTooLargeException.class, map("BatchRequestTooLong", "x", 400));
    }

    @Test
    void otherErrorsAreRetryableOnlyOnServerSide() {
        MessagingException validation = map("InvalidParameterValue", "MessageGroupId inválido", 400);
        assertEquals(MessagingException.class, validation.getClass());
        assertFalse(validation.retryable());
        assertTrue(map("InternalError", "x", 500).retryable());
        assertTrue(map("RequestThrottled", "x", 400).retryable());
        assertEquals("aws", validation.provider());
    }

    @Test
    void sizeCountsBodyAndEveryAttributePart() {
        OutgoingMessage message = OutgoingMessage.ofText("abc").withAttribute("k", "vv");
        AwsCodec.Encoded encoded = AwsCodec.encode(message, 1024);

        // corpo + ("k" + "String" + "vv") + ("content_type" + "String" + "text/plain; charset=utf-8")
        assertEquals(3 + (1 + 6 + 2) + (12 + 6 + 25), encoded.size());
        assertThrows(MessageTooLargeException.class, () -> AwsCodec.encode(message, encoded.size() - 1));
    }

    @Test
    void atMostTenNativeAttributesReachTheProvider() {
        Map<String, String> attributes = new HashMap<>();
        for (int i = 0; i < OutgoingMessage.MAX_ATTRIBUTES; i++) {
            attributes.put("k" + i, "v");
        }
        OutgoingMessage seven = OutgoingMessage.ofText("x").withTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .withAttributes(Map.of("a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "f", "6", "g", "7"))
                .withDeadLetter(new com.example.messaging.DeadLetterInfo("id", "motivo"));

        assertEquals(10, AwsCodec.encode(seven, Long.MAX_VALUE).attributes().size());
        assertEquals(2, AwsCodec.encode(OutgoingMessage.ofText("x").withAttributes(attributes), Long.MAX_VALUE)
                .attributes().size());
    }

    private static MessagingException map(String code, String text, int status) {
        return AwsErrors.map("Op", code, text, status, null);
    }
}
