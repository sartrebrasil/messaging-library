package com.example.messaging.inbox;

import com.example.messaging.MessageReceiver;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** The read loop shared by a dead-letter pass and a redrive: one message at a time, with its end conditions. */
class PassReaderTest {

    private static final String Q = "orders-dlq";

    private final InMemoryMessaging messaging = new InMemoryMessaging();
    private final StopSignal stop = new StopSignal();

    PassReaderTest() {
        messaging.createQueue(Q, QueueOptions.defaults().withLease(Duration.ofSeconds(5)));
    }

    private void send(String body) {
        messaging.queueSender(Q).send(OutgoingMessage.ofText(body));
    }

    private PassReader reader(MessageReceiver receiver, Instant runStart, int limit) {
        return new PassReader(receiver, runStart, limit, ReceivedMessage::bodyAsString, stop, Duration.ofMillis(50),
                "billing");
    }

    private PassReader reader(int limit) {
        return reader(messaging.receiver(Q), Instant.now().plusSeconds(60), limit);
    }

    @Test
    void endsWhenEmpty() {
        send("a");
        PassReader reader = reader(100);

        assertEquals("a", reader.next().orElseThrow().bodyAsString());
        assertTrue(reader.next().isEmpty());
        assertEquals(PassEnd.EMPTY, reader.end());
        assertEquals(1, reader.read());
    }

    @Test
    void endsAtANewerMessageAndGivesItBack() {
        PassReader reader = reader(messaging.receiver(Q), Instant.now().minusSeconds(60), 100);
        send("a");

        assertTrue(reader.next().isEmpty());
        assertEquals(PassEnd.NEWER_THAN_PASS, reader.end());
        assertEquals(0, reader.read());
        assertEquals(1, messaging.receiver(Q).receive(10, Duration.ZERO).size());
    }

    @Test
    void endsAtARepeatedKey() {
        send("a");
        send("a");
        PassReader reader = reader(100);

        assertTrue(reader.next().isPresent());
        assertTrue(reader.next().isEmpty());
        assertEquals(PassEnd.REPEATED_KEY, reader.end());
        assertEquals(1, reader.read());
    }

    @Test
    void endsAtTheLimitAndGivesTheNextBack() {
        send("a");
        send("b");
        send("c");
        MessageReceiver receiver = messaging.receiver(Q);
        PassReader reader = reader(receiver, Instant.now().plusSeconds(60), 2);

        Optional<ReceivedMessage> a = reader.next();
        Optional<ReceivedMessage> b = reader.next();
        assertTrue(reader.next().isEmpty());
        assertEquals(PassEnd.MAX_PER_PASS, reader.end());
        assertEquals(2, reader.read());
        receiver.ack(a.orElseThrow());
        receiver.ack(b.orElseThrow());
        List<ReceivedMessage> left = messaging.receiver(Q).receive(10, Duration.ZERO);
        assertEquals(List.of("c"), left.stream().map(ReceivedMessage::bodyAsString).toList());
    }

    @Test
    void endsWhenStopping() {
        send("a");
        stop.stop();
        PassReader reader = reader(100);

        assertTrue(reader.next().isEmpty());
        assertEquals(PassEnd.STOPPING, reader.end());
        assertEquals(1, messaging.size(Q));
    }

    @Test
    void endsOnReceiveFailureWithoutThrowing() {
        MessageReceiver broken = new InboxConsumerTest.Delegating(messaging.receiver(Q)) {
            @Override
            public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
                throw new IllegalStateException("broker down");
            }
        };
        PassReader reader = reader(broken, Instant.now(), 100);

        assertTrue(reader.next().isEmpty());
        assertEquals(PassEnd.RECEIVE_FAILED, reader.end());
    }
}
