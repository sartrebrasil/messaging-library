package com.example.messaging.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import com.example.messaging.spring.MessagingProperties.Destination;
import com.example.messaging.spring.MessagingProperties.Provider;
import com.example.messaging.spring.MessagingProperties.Type;
import org.springframework.beans.factory.ListableBeanFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Faz o papel dos adapters nos testes do starter (que não dependem deles): aws, azure e gcp sobre
 * um broker em memória. Sem rabbitmq, para testar o erro de adapter ausente.
 */
public final class InMemoryProviderFactory implements ProviderFactory {

    @Override
    public Set<Type> types() {
        return Set.of(Type.AWS, Type.AZURE, Type.GCP);
    }

    @Override
    public ProviderConnection connect(String providerName, Provider provider, ListableBeanFactory beans) {
        InMemoryMessaging broker = new InMemoryMessaging();
        Set<String> queues = ConcurrentHashMap.newKeySet();
        return new ProviderConnection() {
            @Override
            public MessageSender sender(String name, Destination d) {
                String topic = d.topicArn() != null ? d.topicArn() : d.topic();
                if (topic != null) {
                    broker.createTopic(topic);
                    return broker.topicSender(topic);
                }
                String queue = d.queueUrl() != null ? d.queueUrl() : d.queue();
                return queue == null || d.deadLetterQueue() ? null : broker.queueSender(queue(queue));
            }

            @Override
            public MessageReceiver receiver(String name, Destination d, MessageSender deadLetterSender) {
                String queue = d.subscription() != null ? d.subscription() : d.queueUrl() != null ? d.queueUrl() : d.queue();
                return queue == null ? null : broker.receiver(queue(queue));
            }

            private String queue(String name) {
                if (queues.add(name)) {
                    broker.createQueue(name, QueueOptions.defaults());
                }
                return name;
            }

            @Override
            public void close() {
            }
        };
    }
}
