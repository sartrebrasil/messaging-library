package com.example.messaging.aws;

import com.example.messaging.BatchSendResult;
import com.example.messaging.OutgoingMessage;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.lang.reflect.Proxy;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqsMessageSenderTest {

    /** Cliente que falha se for chamado: a mensagem inválida nunca deve chegar ao SQS. */
    private static final SqsClient NEVER_CALLED = (SqsClient) Proxy.newProxyInstance(
            SqsMessageSenderTest.class.getClassLoader(), new Class<?>[]{SqsClient.class}, (proxy, method, args) -> {
                if (method.getName().equals("close")) {
                    return null;
                }
                throw new AssertionError("não devia chamar " + method.getName());
            });

    @Test
    void sendAllReportsAFifoMessageWithoutOrderingKeyAsAFailureInsteadOfThrowing() {
        var sender = new SqsMessageSender(NEVER_CALLED, "https://sqs.sa-east-1.amazonaws.com/1/q.fifo");

        BatchSendResult result = sender.sendAll(List.of(OutgoingMessage.ofText("sem chave")));

        assertTrue(result.sent().isEmpty());
        assertEquals(java.util.Set.of(0), result.failures().keySet());
        assertTrue(result.failures().get(0).getCause() instanceof IllegalArgumentException);
    }
}
