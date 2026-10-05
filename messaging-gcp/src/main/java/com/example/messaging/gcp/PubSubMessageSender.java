package com.example.messaging.gcp;

import com.example.messaging.BatchSendResult;
import com.example.messaging.Capabilities;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessageSender;
import com.example.messaging.MessagingException;
import com.example.messaging.OutgoingMessage;
import com.example.messaging.SendResult;
import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.pubsub.v1.PubsubMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Publicação num tópico do Pub/Sub. O {@link Publisher} precisa ter
 * {@code setEnableMessageOrdering(true)}: sem isso, publicar com {@code orderingKey} lança
 * {@code IllegalStateException} no SDK (ADR-0006). {@link #create} já configura assim.
 *
 * <p>Quando um envio com {@code orderingKey} falha, o publisher pausa a chave; o adapter chama
 * {@code resumePublish} e relança o erro. Para a ordem valer, todo envio da chave sai da mesma
 * região. {@code deduplicationId} é ignorado: o Pub/Sub não deduplica no envio.</p>
 *
 * <p>{@link #checkAccess()} usa {@code TestIamPermissions(pubsub.topics.publish)}, que não exige
 * permissão extra; no emulator, que não implementa IAM, cai para {@code GetTopic}.</p>
 */
public final class PubSubMessageSender implements MessageSender {

    static final int MAX_BATCH = 1000;
    private static final String PUBLISH_PERMISSION = "pubsub.topics.publish";

    private final Publisher publisher;
    private final TopicAdminClient admin;
    private final boolean ownsPublisher;
    private volatile boolean closed;

    /**
     * O publisher continua de quem o criou.
     *
     * @param admin usado só por {@link #checkAccess()}; {@code null} faz {@code checkAccess} lançar
     *              {@link UnsupportedOperationException}
     */
    public PubSubMessageSender(Publisher publisher, TopicAdminClient admin) {
        this(publisher, admin, false);
    }

    private PubSubMessageSender(Publisher publisher, TopicAdminClient admin, boolean ownsPublisher) {
        this.publisher = publisher;
        this.admin = admin;
        this.ownsPublisher = ownsPublisher;
    }

    /**
     * Constrói o publisher com ordem habilitada e lotes de até 1 MB ou 10 ms (o padrão do SDK é
     * 1 KB), e o encerra no {@link #close()}. Credenciais e canal vêm do {@code builder}.
     */
    public static PubSubMessageSender create(Publisher.Builder builder, TopicAdminClient admin) {
        try {
            Publisher publisher = builder.setEnableMessageOrdering(true)
                    .setBatchingSettings(BatchingSettings.newBuilder()
                            .setElementCountThreshold(100L)
                            .setRequestByteThreshold(1_000_000L)
                            .setDelayThresholdDuration(Duration.ofMillis(10))
                            .build())
                    .build();
            return new PubSubMessageSender(publisher, admin, true);
        } catch (java.io.IOException e) {
            throw new MessagingException(GcpErrors.PROVIDER, "Publisher: " + e.getMessage(), e, false);
        }
    }

    @Override
    public SendResult send(OutgoingMessage message) {
        ensureOpen();
        PubsubMessage wire = PubSubCodec.encode(message);
        return await(publisher.publish(wire), wire);
    }

    /** Publica todas e espera as futures juntas: o publisher agrupa em lotes. */
    @Override
    public BatchSendResult sendAll(List<OutgoingMessage> messages) {
        ensureOpen();
        Map<Integer, SendResult> sent = new HashMap<>();
        Map<Integer, MessagingException> failures = new HashMap<>();
        List<ApiFuture<String>> futures = new ArrayList<>();
        List<PubsubMessage> wires = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            try {
                PubsubMessage wire = PubSubCodec.encode(messages.get(i));
                wires.add(wire);
                futures.add(publisher.publish(wire));
            } catch (MessagingException e) {
                failures.put(i, e);
                wires.add(null);
                futures.add(null);
            }
        }
        for (int i = 0; i < futures.size(); i++) {
            if (futures.get(i) == null) {
                continue;
            }
            try {
                sent.put(i, await(futures.get(i), wires.get(i)));
            } catch (MessagingException e) {
                failures.put(i, e);
            }
        }
        return new BatchSendResult(sent, failures);
    }

    private SendResult await(ApiFuture<String> future, PubsubMessage wire) {
        try {
            return new SendResult(future.get());
        } catch (ExecutionException e) {
            if (!wire.getOrderingKey().isEmpty()) {
                publisher.resumePublish(wire.getOrderingKey());
            }
            throw GcpErrors.map("publish " + publisher.getTopicNameString(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingException(GcpErrors.PROVIDER, "publish interrompido", e, true);
        }
    }

    @Override
    public void checkAccess() {
        ensureOpen();
        if (admin == null) {
            throw new UnsupportedOperationException("Sem TopicAdminClient para checkAccess");
        }
        String topic = publisher.getTopicNameString();
        try {
            List<String> granted = admin.testIamPermissions(TestIamPermissionsRequest.newBuilder()
                    .setResource(topic).addPermissions(PUBLISH_PERMISSION).build()).getPermissionsList();
            if (!granted.contains(PUBLISH_PERMISSION)) {
                throw new DestinationNotFoundException(GcpErrors.PROVIDER,
                        "Tópico inexistente ou sem " + PUBLISH_PERMISSION + ": " + topic, null);
            }
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() != StatusCode.Code.UNIMPLEMENTED) {
                throw GcpErrors.map("testIamPermissions " + topic, e);
            }
            try {
                admin.getTopic(topic);
            } catch (ApiException getError) {
                throw GcpErrors.map("getTopic " + topic, getError);
            }
        }
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(PubSubCodec.MAX_MESSAGE_BYTES, MAX_BATCH, false, Duration.ZERO, false, true, false, false);
    }

    @Override
    public void close() {
        closed = true;
        if (ownsPublisher) {
            publisher.shutdown();
            try {
                publisher.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("PubSubMessageSender fechado: " + publisher.getTopicNameString());
        }
    }
}
