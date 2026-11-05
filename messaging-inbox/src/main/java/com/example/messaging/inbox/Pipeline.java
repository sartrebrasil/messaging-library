package com.example.messaging.inbox;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.ExceptionClassifier;
import com.example.messaging.inbox.retry.FailureResolver;
import com.example.messaging.inbox.retry.ImmediateRetrier;
import com.example.messaging.inbox.retry.RetryDecision;
import com.example.messaging.inbox.retry.RetryPolicy;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * Per-delivery processing, broker-agnostic: envelope, payload deserialization, the immediate level, exception
 * classification and the handler result (ADRs 0002, 0003, 0014). Returns an {@link Outcome}; the effect and the
 * strategy of the delayed level belong to {@link InboxConsumer}.
 */
public final class Pipeline<T> {

    private static final System.Logger LOG = System.getLogger(Pipeline.class.getName());

    private final String consumer;
    private final EnvelopeReader reader;
    private final PayloadDeserializer deserializer;
    private final Type payloadType;
    private final InboxResultHandler<T> handler;
    private final RetryPolicy policy;
    private final ImmediateRetrier retrier;
    private final FailureResolver resolver;

    /**
     * @param sleeper waits of the immediate level; use the consumer's {@link StopSignal} so shutdown cuts them
     */
    public Pipeline(String consumer, EnvelopeReader reader, PayloadDeserializer deserializer, Type payloadType,
                    InboxResultHandler<T> handler, RetryPolicy policy, ImmediateRetrier.Sleeper sleeper,
                    DoubleSupplier random) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.policy = Objects.requireNonNull(policy, "policy");
        var classifier = new ExceptionClassifier(policy);
        this.retrier = new ImmediateRetrier(policy.immediate(), classifier, sleeper, random);
        this.resolver = new FailureResolver(policy, classifier, random);
    }

    public RetryPolicy policy() {
        return policy;
    }

    /**
     * @param holdAttempts delayed attempts already spent in memory by the {@code hold} strategy for this delivery
     */
    public Outcome handle(ReceivedMessage message, String destination, int holdAttempts) {
        AttemptState read = AttemptState.from(message.attributes().get(AttemptState.ATTRIBUTE),
                message.deliveryCount().orElse(0));
        AttemptState state = new AttemptState(read.delayed() + holdAttempts, read.deadLetter(), read.redrive());
        TypedMessage<T> typed;
        try {
            InboxMessage inbox = reader.read(message, destination).withDelivery(Delivery.of(state));
            typed = typed(inbox);
        } catch (PayloadDeserializationException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: message {1} cannot be read, sent to failed: {2}",
                    consumer, message.messageId(), e.getMessage());
            return new Outcome.Failed(Outcome.FailureReason.DESERIALIZATION, state, e, null);
        }
        ImmediateRetrier.Result result;
        try {
            result = retrier.execute(() -> handler.handle(typed));
        } catch (InterruptedException e) {
            return Outcome.RELEASE;
        }
        return switch (resolver.resolve(result, state)) {
            case RetryDecision.Ack a -> new Outcome.Ack(Outcome.AckReason.SUCCESS);
            case RetryDecision.Discard d -> {
                LOG.log(System.Logger.Level.INFO, "Consumer {0}: message {1} discarded: {2}", consumer,
                        typed.message().id(), d.reason());
                yield new Outcome.Ack(Outcome.AckReason.DISCARDED);
            }
            case RetryDecision.Delay d -> new Outcome.Retry(d.delay(), d.next(), d.cause());
            case RetryDecision.DeadLetter d -> new Outcome.DeadLetter(d.next(), d.cause());
            case RetryDecision.Failed f -> {
                String detail = null;
                if (result.result() instanceof HandlerResult.Reject reject) {
                    detail = reject.reason();
                    LOG.log(System.Logger.Level.INFO, "Consumer {0}: message {1} rejected: {2}", consumer,
                            typed.message().id(), detail);
                }
                yield new Outcome.Failed(switch (f.reason()) {
                    case NON_RETRYABLE -> Outcome.FailureReason.NON_RETRYABLE;
                    case RETRIES_EXHAUSTED -> Outcome.FailureReason.RETRIES_EXHAUSTED;
                    case REJECTED -> Outcome.FailureReason.REJECTED;
                }, state, f.cause(), detail);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private TypedMessage<T> typed(InboxMessage inbox) {
        Object payload;
        try {
            payload = deserializer.deserialize(inbox, payloadType);
        } catch (PayloadDeserializationException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PayloadDeserializationException("payload of message " + inbox.id() + " is not a " + payloadType, e);
        }
        return new TypedMessage<>(inbox, (T) payload);
    }
}
