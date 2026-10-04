package com.example.messaging.aws;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.testkit.MessagingContract;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

/**
 * Contrato do tópico SNS entregando numa fila SQS com {@code RawMessageDelivery=true}. Pulado
 * sem Docker. O LocalStack não aplica o limite de 10 atributos nem o {@code MaximumMessageSize}
 * (F0), então os limites do SNS são cobertos só pela validação do adapter.
 */
@Testcontainers(disabledWithoutDocker = true)
class SnsLocalStackContractTest extends MessagingContract {

    private static String topicArn;
    private static String queue;

    @BeforeAll
    static void setUp() {
        LocalStack.start();
        topicArn = LocalStack.sns().createTopic(b -> b.name("contract-topic")).topicArn();
        queue = LocalStack.queue("contract-topic-sub");
        String queueArn = LocalStack.queueArn(queue);
        LocalStack.sns().subscribe(b -> b.topicArn(topicArn).protocol("sqs").endpoint(queueArn)
                .attributes(java.util.Map.of("RawMessageDelivery", "true")));
    }

    @Override
    protected MessageSender newSender() {
        return new SnsMessageSender(LocalStack.sns(), topicArn);
    }

    @Override
    protected MessageReceiver newReceiver() {
        return new SqsMessageReceiver(LocalStack.sqs(), queue);
    }

    @Override
    protected Duration lease() {
        return LocalStack.LEASE;
    }

    @Override
    protected MessageSender newMissingDestinationSender() {
        return new SnsMessageSender(LocalStack.sns(), topicArn.replace("contract-topic", "nao-existe"));
    }
}
