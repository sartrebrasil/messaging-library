package com.example.messaging.jms;

import com.example.messaging.MessagingException;
import com.example.messaging.ReceivedMessage;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Sem broker: a conexão é um proxy cujo consumer entrega uma mensagem e depois falha. */
class JmsMessageReceiverTest {

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.util.function.BiFunction<String, Object[], Object> handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (p, method, args) -> {
            Object result = handler.apply(method.getName(), args);
            if (result != null || !method.getReturnType().isPrimitive()) {
                return result;
            }
            return method.getReturnType() == boolean.class ? Boolean.FALSE
                    : method.getReturnType() == long.class ? (Object) 0L : (Object) 0;
        });
    }

    private static Connection connection(AtomicInteger receives) {
        Message message = proxy(Message.class, (name, args) -> switch (name) {
            case "getPropertyNames" -> Collections.emptyEnumeration();
            case "getJMSMessageID" -> "id-1";
            case "getBody" -> "corpo".getBytes();
            default -> null;
        });
        MessageConsumer consumer = proxy(MessageConsumer.class, (name, args) -> {
            if (name.equals("receive") || name.equals("receiveNoWait")) {
                if (receives.incrementAndGet() > 1) {
                    throw new RuntimeJmsFailure();
                }
                return message;
            }
            return null;
        });
        Session session = proxy(Session.class, (name, args) -> name.equals("createConsumer") ? consumer : null);
        return proxy(Connection.class, (name, args) -> name.equals("createSession") ? session : null);
    }

    /** O proxy só pode lançar exceções declaradas; a do JMS é verificada, então o receive a vê como RuntimeException. */
    private static final class RuntimeJmsFailure extends RuntimeException {
    }

    @Test
    void aFailureAfterSomeMessagesStillDeliversTheOnesAlreadyTaken() {
        var receives = new AtomicInteger();
        try (var receiver = new JmsMessageReceiver(connection(receives), JmsDialect.ACTIVEMQ_CLASSIC, "q")) {
            List<ReceivedMessage> first = receiver.receive(5, Duration.ofSeconds(1));

            assertEquals(1, first.size());
            assertThrows(RuntimeException.class, () -> receiver.receive(5, Duration.ofSeconds(1)));
        }
    }
}
