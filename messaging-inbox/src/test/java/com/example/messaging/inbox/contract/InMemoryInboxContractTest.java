package com.example.messaging.inbox.contract;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import org.junit.jupiter.api.Nested;

import java.time.Duration;
import java.util.List;

/** O contrato sobre o broker em memória: prova o próprio contrato, sem Docker. */
class InMemoryInboxContractTest extends InboxContract {

    static final Duration LEASE = Duration.ofSeconds(1);
    static final Duration WAIT = Duration.ofMillis(50);

    final InMemoryMessaging messaging = new InMemoryMessaging();

    InMemoryInboxContractTest() {
        for (String q : List.of("inbox-main", "inbox-failed", "inbox-dlq", "inbox-retry")) {
            messaging.createQueue(q, QueueOptions.defaults().withLease(LEASE));
        }
    }

    @Override
    protected MessageSender newSender() {
        return messaging.queueSender("inbox-main");
    }

    @Override
    protected MessageReceiver newReceiver() {
        return messaging.receiver("inbox-main");
    }

    @Override
    protected MessageSender failedSender() {
        return messaging.queueSender("inbox-failed");
    }

    @Override
    protected MessageReceiver failedReceiver() {
        return messaging.receiver("inbox-failed");
    }

    @Override
    protected MessageSender deadLetterSender() {
        return messaging.queueSender("inbox-dlq");
    }

    @Override
    protected MessageReceiver deadLetterReceiver() {
        return messaging.receiver("inbox-dlq");
    }

    @Override
    protected Duration lease() {
        return LEASE;
    }

    @Override
    protected List<RetryQueue> retryQueues() {
        return List.of(new RetryQueue(LEASE, messaging.queueSender("inbox-retry")));
    }

    @Override
    protected MessageReceiver retryQueueReceiver() {
        return messaging.receiver("inbox-retry");
    }

    @Override
    protected Duration settle() {
        return Duration.ofSeconds(5);
    }

    @Override
    protected Duration receiveWait() {
        return WAIT;
    }

    /** Sem bandas: o caso de retry-queues é pulado, nunca vermelho. */
    @Nested
    class WithoutRetryQueues extends InboxContract {

        @Override
        protected MessageSender newSender() {
            return InMemoryInboxContractTest.this.newSender();
        }

        @Override
        protected MessageReceiver newReceiver() {
            return InMemoryInboxContractTest.this.newReceiver();
        }

        @Override
        protected MessageSender failedSender() {
            return InMemoryInboxContractTest.this.failedSender();
        }

        @Override
        protected MessageReceiver failedReceiver() {
            return InMemoryInboxContractTest.this.failedReceiver();
        }

        @Override
        protected MessageSender deadLetterSender() {
            return InMemoryInboxContractTest.this.deadLetterSender();
        }

        @Override
        protected MessageReceiver deadLetterReceiver() {
            return InMemoryInboxContractTest.this.deadLetterReceiver();
        }

        @Override
        protected Duration lease() {
            return LEASE;
        }

        @Override
        protected Duration settle() {
            return Duration.ofSeconds(5);
        }

        @Override
        protected Duration receiveWait() {
            return WAIT;
        }
    }
}
