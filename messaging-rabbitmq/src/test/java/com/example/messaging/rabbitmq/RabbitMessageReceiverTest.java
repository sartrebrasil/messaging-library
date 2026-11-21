package com.example.messaging.rabbitmq;

import com.example.messaging.MessagingException;
import com.example.messaging.ReceivedMessage;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Sem broker: o canal é um proxy que entrega uma mensagem e depois falha. */
class RabbitMessageReceiverTest {

    private static Connection connection(AtomicInteger gets) {
        Channel channel = (Channel) Proxy.newProxyInstance(Channel.class.getClassLoader(), new Class<?>[]{Channel.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "isOpen" -> {
                            return true;
                        }
                        case "basicGet" -> {
                            if (gets.incrementAndGet() > 1) {
                                throw new IOException("conexão caiu");
                            }
                            return new GetResponse(new Envelope(1, false, "", "q"),
                                    new AMQP.BasicProperties.Builder().build(), "corpo".getBytes(), 0);
                        }
                        default -> {
                            return method.getReturnType() == boolean.class ? Boolean.FALSE : null;
                        }
                    }
                });
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> method.getName().equals("createChannel") ? channel : null);
    }

    @Test
    void aFailureAfterSomeMessagesStillDeliversTheOnesAlreadyTaken() {
        var gets = new AtomicInteger();
        try (var receiver = new RabbitMessageReceiver(connection(gets), "q")) {
            List<ReceivedMessage> first = receiver.receive(5, Duration.ofSeconds(1));

            assertEquals(1, first.size());
            // o erro não se perde: volta no próximo receive, quando não há mais nada na mão
            assertThrows(MessagingException.class, () -> receiver.receive(5, Duration.ofSeconds(1)));
        }
    }
}
