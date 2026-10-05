package com.example.messaging.gcp;

import com.example.messaging.AckResult;
import com.example.messaging.Capabilities;
import com.example.messaging.DeadLetterInfo;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.KeepAlive;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.ReceivedMessage;
import com.example.messaging.spi.LeaseKeeper;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Recebimento de uma subscription do Pub/Sub por pull síncrono ({@link SubscriberStub}), não pelo
 * {@code Subscriber} de streaming (ADR-0002).
 *
 * <ul>
 *   <li>{@code maxWait} é o timeout da chamada {@code Pull}; estourar vira lista vazia. O servidor
 *       pode devolver menos que {@code maxMessages} mesmo com backlog.</li>
 *   <li>O lease é o ack deadline da subscription, informado em {@link Options#ackDeadline()}:
 *       os papéis subscriber/publisher não leem a subscription.</li>
 *   <li>{@code nack} e {@code extendLease} usam {@code ModifyAckDeadline}, até 600 s a partir de agora.
 *       Com ordem, {@code nack} reentrega a mensagem e as seguintes da chave.</li>
 *   <li>{@code ack} com lease vencido só é informado com exactly-once ({@link Options#exactlyOnce()}).</li>
 *   <li>{@code deadLetter} envia uma cópia ao {@link Options#deadLetterSender()} e confirma; sem ele,
 *       {@link UnsupportedOperationException}. O dead letter topic por {@code maxDeliveryAttempts}
 *       continua sendo configuração da subscription.</li>
 * </ul>
 */
public final class PubSubMessageReceiver implements MessageReceiver {

    static final Duration MAX_ACK_DEADLINE = Duration.ofSeconds(600);
    private static final int MAX_PULL = 1000;
    private static final int MAX_ACK_IDS = 1000;
    private static final String CONSUME_PERMISSION = "pubsub.subscriptions.consume";

    /**
     * @param ackDeadline      ack deadline configurado na subscription (10–600 s)
     * @param orderedDelivery  a subscription tem {@code enable_message_ordering}
     * @param exactlyOnce      a subscription tem exactly-once delivery
     * @param deadLetterSender destino de {@code deadLetter}; {@code null} sem DLQ
     */
    public record Options(Duration ackDeadline, boolean orderedDelivery, boolean exactlyOnce,
                          MessageSender deadLetterSender) {

        public static Options ackDeadline(Duration ackDeadline) {
            return new Options(ackDeadline, false, false, null);
        }

        public Options withOrderedDelivery(boolean ordered) {
            return new Options(ackDeadline, ordered, exactlyOnce, deadLetterSender);
        }

        public Options withExactlyOnce(boolean enabled) {
            return new Options(ackDeadline, orderedDelivery, enabled, deadLetterSender);
        }

        public Options withDeadLetterSender(MessageSender sender) {
            return new Options(ackDeadline, orderedDelivery, exactlyOnce, sender);
        }
    }

    private final SubscriberStub stub;
    private final String subscription;
    private final Options options;
    private final boolean ownsStub;
    private final LeaseKeeper keeper = new LeaseKeeper(this::extendLease);
    private volatile boolean closed;

    /**
     * O stub continua de quem o criou.
     *
     * @param subscription {@code projects/{projeto}/subscriptions/{nome}}
     */
    public PubSubMessageReceiver(SubscriberStub stub, String subscription, Options options) {
        this(stub, subscription, options, false);
    }

    private PubSubMessageReceiver(SubscriberStub stub, String subscription, Options options, boolean ownsStub) {
        this.stub = stub;
        this.subscription = subscription;
        this.options = options;
        this.ownsStub = ownsStub;
    }

    /** Cria o stub gRPC a partir das settings (credenciais, canal) e o fecha no {@link #close()}. */
    public static PubSubMessageReceiver create(SubscriberStubSettings settings, String subscription, Options options) {
        try {
            return new PubSubMessageReceiver(GrpcSubscriberStub.create(settings), subscription, options, true);
        } catch (IOException e) {
            throw new MessagingException(GcpErrors.PROVIDER, "SubscriberStub: " + e.getMessage(), e, false);
        }
    }

    private record Handle(PubSubMessageReceiver owner, String ackId) {
    }

    @Override
    public List<ReceivedMessage> receive(int maxMessages, Duration maxWait) {
        ensureOpen();
        if (maxMessages < 1) {
            throw new IllegalArgumentException("maxMessages precisa ser >= 1: " + maxMessages);
        }
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait negativo: " + maxWait);
        }
        PullRequest request = PullRequest.newBuilder().setSubscription(subscription)
                .setMaxMessages(Math.min(maxMessages, MAX_PULL)).build();
        Instant receivedAt = Instant.now();
        PullResponse response;
        try {
            response = stub.pullCallable().call(request, GrpcCallContext.createDefault()
                    .withTimeoutDuration(maxWait.isZero() ? Duration.ofMillis(1) : maxWait));
        } catch (DeadlineExceededException e) {
            return List.of();
        } catch (ApiException e) {
            throw GcpErrors.map("pull " + subscription, e);
        }
        List<ReceivedMessage> result = new ArrayList<>();
        for (com.google.pubsub.v1.ReceivedMessage received : response.getReceivedMessagesList()) {
            result.add(PubSubCodec.decode(received.getMessage(), received.getDeliveryAttempt(), receivedAt,
                    options.ackDeadline(), new Handle(this, received.getAckId())));
        }
        return result;
    }

    @Override
    public void ack(ReceivedMessage message) {
        Handle handle = own(message);
        keeper.release(message);
        try {
            stub.acknowledgeCallable().call(AcknowledgeRequest.newBuilder().setSubscription(subscription)
                    .addAckIds(handle.ackId()).build());
        } catch (ApiException e) {
            throw GcpErrors.map("acknowledge " + subscription, e);
        }
    }

    /** Um {@code Acknowledge} por até 1000 ack ids; com exactly-once, falhas vêm por ack id. */
    @Override
    public AckResult ackAll(List<ReceivedMessage> messages) {
        Map<String, MessagingException> failures = new HashMap<>();
        for (int start = 0; start < messages.size(); start += MAX_ACK_IDS) {
            List<ReceivedMessage> chunk = messages.subList(start, Math.min(messages.size(), start + MAX_ACK_IDS));
            Map<String, ReceivedMessage> byAckId = new HashMap<>();
            for (ReceivedMessage message : chunk) {
                byAckId.put(own(message).ackId(), message);
                keeper.release(message);
            }
            try {
                stub.acknowledgeCallable().call(AcknowledgeRequest.newBuilder().setSubscription(subscription)
                        .addAllAckIds(byAckId.keySet()).build());
            } catch (ApiException e) {
                Map<String, String> perAckId = GcpErrors.ackFailures(e);
                MessagingException mapped = GcpErrors.map("acknowledge " + subscription, e);
                byAckId.forEach((ackId, message) -> {
                    String code = perAckId.get(ackId);
                    if (perAckId.isEmpty()) {
                        failures.put(message.messageId(), mapped);
                    } else if (GcpErrors.INVALID_ACK_ID.equals(code)) {
                        failures.put(message.messageId(), new LeaseExpiredException(GcpErrors.PROVIDER,
                                "acknowledge " + subscription + ": " + code, e));
                    } else if (code != null) {
                        failures.put(message.messageId(), new MessagingException(GcpErrors.PROVIDER,
                                "acknowledge " + subscription + ": " + code, e, code.startsWith("TRANSIENT")));
                    }
                });
            }
        }
        return new AckResult(failures);
    }

    /**
     * {@code ModifyAckDeadline(atraso)}; zero devolve na hora. A retry policy da subscription, se
     * houver, ainda soma o backoff dela. Conta como tentativa de entrega.
     *
     * @throws IllegalArgumentException atraso negativo ou acima de 600 s
     */
    @Override
    public void nack(ReceivedMessage message, Duration redeliverAfter) {
        Handle handle = own(message);
        if (redeliverAfter.isNegative() || redeliverAfter.compareTo(MAX_ACK_DEADLINE) > 0) {
            throw new IllegalArgumentException("Atraso fora de 0.." + MAX_ACK_DEADLINE + ": " + redeliverAfter);
        }
        keeper.release(message);
        modifyAckDeadline(handle, ceilSeconds(redeliverAfter));
    }

    /** {@code ModifyAckDeadline}: o novo prazo conta de agora, limitado a 600 s. */
    @Override
    public void extendLease(ReceivedMessage message, Duration lease) {
        modifyAckDeadline(own(message), (int) Math.max(1, Math.min(MAX_ACK_DEADLINE.toSeconds(), ceilSeconds(lease))));
    }

    private void modifyAckDeadline(Handle handle, int seconds) {
        try {
            stub.modifyAckDeadlineCallable().call(ModifyAckDeadlineRequest.newBuilder().setSubscription(subscription)
                    .addAckIds(handle.ackId()).setAckDeadlineSeconds(seconds).build());
        } catch (ApiException e) {
            throw GcpErrors.map("modifyAckDeadline " + subscription, e);
        }
    }

    @Override
    public KeepAlive keepAlive(ReceivedMessage message, Duration maxTotal) {
        own(message);
        return keeper.keep(message, maxTotal);
    }

    @Override
    public void deadLetter(ReceivedMessage message, String reason) {
        own(message);
        if (options.deadLetterSender() == null) {
            throw new UnsupportedOperationException("Receiver de " + subscription + " sem sender de DLQ configurado");
        }
        OutgoingMessage copy = new OutgoingMessage(message.body(), message.attributes(), message.contentType(),
                message.orderingKey(), null, message.traceparent(), new DeadLetterInfo(message.messageId(), reason));
        // Envio antes do ack: falha entre os dois gera duplicata na DLQ, nunca perda (ADR-0005)
        options.deadLetterSender().send(copy);
        ack(message);
    }

    /**
     * {@code TestIamPermissions(pubsub.subscriptions.consume)}, sem permissão extra; no emulator, que não
     * implementa IAM, cai para {@code GetSubscription}.
     */
    @Override
    public void checkAccess() {
        ensureOpen();
        try {
            List<String> granted = stub.testIamPermissionsCallable().call(TestIamPermissionsRequest.newBuilder()
                    .setResource(subscription).addPermissions(CONSUME_PERMISSION).build()).getPermissionsList();
            if (!granted.contains(CONSUME_PERMISSION)) {
                throw new DestinationNotFoundException(GcpErrors.PROVIDER,
                        "Subscription inexistente ou sem " + CONSUME_PERMISSION + ": " + subscription, null);
            }
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() != StatusCode.Code.UNIMPLEMENTED) {
                throw GcpErrors.map("testIamPermissions " + subscription, e);
            }
            try {
                stub.getSubscriptionCallable().call(GetSubscriptionRequest.newBuilder()
                        .setSubscription(subscription).build());
            } catch (ApiException getError) {
                throw GcpErrors.map("getSubscription " + subscription, getError);
            }
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(PubSubCodec.MAX_MESSAGE_BYTES, MAX_PULL, true, MAX_ACK_DEADLINE, false,
                options.orderedDelivery(), false, options.exactlyOnce());
    }

    @Override
    public void close() {
        closed = true;
        keeper.close();
        if (ownsStub) {
            stub.close();
        }
    }

    private Handle own(ReceivedMessage message) {
        ensureOpen();
        if (!(message.handle() instanceof Handle handle) || handle.owner() != this) {
            throw new IllegalArgumentException("Mensagem recebida por outro receiver: " + message.messageId());
        }
        return handle;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PubSubMessageReceiver fechado: " + subscription);
        }
    }

    private static int ceilSeconds(Duration duration) {
        long seconds = duration.getSeconds();
        return (int) (duration.getNano() > 0 ? seconds + 1 : seconds);
    }
}
