package com.example.messaging.spring.aws;

import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.aws.SnsMessageSender;
import com.example.messaging.aws.SqsMessageReceiver;
import com.example.messaging.aws.SqsMessageSender;
import com.example.messaging.spring.ClientBeans;
import com.example.messaging.spring.MessagingProperties;
import com.example.messaging.spring.ProviderConnection;
import org.springframework.beans.factory.ListableBeanFactory;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQS e SNS sobre os beans {@code SqsClient} e {@code SnsClient} da aplicação (ADR-0011). Cada um é resolvido no
 * primeiro sender ou receiver que o usa: quem só publica no SNS não declara {@code SqsClient}. O starter não cria nem
 * fecha cliente algum; região, credenciais e o read timeout acima dos 20 s do long poll são da aplicação.
 */
public final class AwsProviderConnection implements ProviderConnection {

    private final String providerName;
    private final ListableBeanFactory beans;
    private volatile SqsClient sqs;
    private volatile SnsClient sns;
    /** URL de cada nome de fila, resolvida uma vez ({@code GetQueueUrl}). */
    private final Map<String, String> queueUrls = new ConcurrentHashMap<>();

    public AwsProviderConnection(String providerName, ListableBeanFactory beans) {
        this.providerName = providerName;
        this.beans = beans;
    }

    private SqsClient sqs() {
        SqsClient client = sqs;
        if (client == null) {
            sqs = client = ClientBeans.require(beans, SqsClient.class, providerName);
        }
        return client;
    }

    private SnsClient sns() {
        SnsClient client = sns;
        if (client == null) {
            sns = client = ClientBeans.require(beans, SnsClient.class, providerName);
        }
        return client;
    }

    @Override
    public MessageSender sender(String name, MessagingProperties.Destination destination) {
        if (destination.topicArn() != null) {
            long max = destination.maxMessageBytes() != null
                    ? destination.maxMessageBytes() : SnsMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
            return new SnsMessageSender(sns(), destination.topicArn(), max);
        }
        return new SqsMessageSender(sqs(), queueUrl(destination));
    }

    @Override
    public MessageReceiver receiver(String name, MessagingProperties.Destination destination,
                                    MessageSender deadLetterSender) {
        return new SqsMessageReceiver(sqs(), queueUrl(destination), deadLetterSender);
    }

    /** {@code queue-url}, ou o nome da fila resolvido no primeiro uso; exige {@code sqs:GetQueueUrl}. */
    private String queueUrl(MessagingProperties.Destination destination) {
        if (destination.queueUrl() != null) {
            return destination.queueUrl();
        }
        return queueUrls.computeIfAbsent(destination.queue(), name -> {
            try {
                return sqs().getQueueUrl(r -> r.queueName(name)).queueUrl();
            } catch (QueueDoesNotExistException e) {
                throw new DestinationNotFoundException("aws", "fila SQS '" + name + "' não existe", e);
            }
        });
    }

    @Override
    public void close() {
        // os clientes são beans da aplicação: quem os declarou os fecha
    }
}
