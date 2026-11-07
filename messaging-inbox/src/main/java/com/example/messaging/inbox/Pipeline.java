package com.example.messaging.inbox;

import com.example.messaging.ReceivedMessage;
import com.example.messaging.inbox.retry.AttemptState;
import com.example.messaging.inbox.retry.Classification;
import com.example.messaging.inbox.retry.ExceptionClassifier;
import com.example.messaging.inbox.retry.FailureResolver;
import com.example.messaging.inbox.retry.ImmediateRetrier;
import com.example.messaging.inbox.retry.RetryDecision;
import com.example.messaging.inbox.retry.RetryPolicy;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * Per-delivery processing, broker-agnostic: envelope, payload deserialization, idempotency, the immediate level,
 * exception classification and the handler result (ADRs 0002, 0003, 0004, 0014). Returns an {@link Outcome}; the
 * effect and the strategy of the delayed level belong to {@link InboxConsumer}.
 *
 * <p>With idempotency the order is {@code reserve -> handler -> complete} on success and
 * {@code reserve -> handler -> release} on every other outcome, before the effect on the broker (ADR-0004).</p>
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
    private final InboxIdempotency idempotency;
    private final IdempotencyGuard guard;

    /**
     * @param sleeper waits of the immediate level; use the consumer's {@link StopSignal} so shutdown cuts them
     */
    public Pipeline(String consumer, EnvelopeReader reader, PayloadDeserializer deserializer, Type payloadType,
                    InboxResultHandler<T> handler, RetryPolicy policy, ImmediateRetrier.Sleeper sleeper,
                    DoubleSupplier random) {
        this(consumer, reader, deserializer, payloadType, handler, policy, sleeper, random, null);
    }

    /** @param idempotency {@code null} without idempotency (ADR-0004: it is optional) */
    public Pipeline(String consumer, EnvelopeReader reader, PayloadDeserializer deserializer, Type payloadType,
                    InboxResultHandler<T> handler, RetryPolicy policy, ImmediateRetrier.Sleeper sleeper,
                    DoubleSupplier random, InboxIdempotency idempotency) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.deserializer = Objects.requireNonNull(deserializer, "deserializer");
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.policy = Objects.requireNonNull(policy, "policy");
        var classifier = new ExceptionClassifier(policy);
        this.retrier = new ImmediateRetrier(policy.immediate(), classifier, sleeper, random);
        this.resolver = new FailureResolver(policy, classifier, random);
        this.idempotency = idempotency;
        this.guard = idempotency == null ? null : new IdempotencyGuard(consumer, idempotency, policy, random);
        if (idempotency != null && reader.mode() == EnvelopeMode.RAW) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: idempotency with envelope raw only protects "
                    + "redeliveries of the same physical message; copies get new ids (ADR-0003)", consumer);
        }
    }

    public RetryPolicy policy() {
        return policy;
    }

    /** {@code null} without idempotency. */
    public InboxIdempotency idempotency() {
        return idempotency;
    }

    /**
     * @param holdAttempts delayed attempts already spent in memory by the {@code hold} strategy for this delivery
     */
    public Outcome handle(ReceivedMessage message, String destination, int holdAttempts) {
        return process(message, destination, holdAttempts, ConsumerObserver.NONE).outcome();
    }

    /** The outcome and the read message ({@code null} when the envelope could not be read). */
    public record Execution(Outcome outcome, InboxMessage message) {
    }

    /** {@link #handle} with an observer around the execution (ADR-0007). */
    public Execution process(ReceivedMessage message, String destination, int holdAttempts,
                             ConsumerObserver observer) {
        return process(message, destination, holdAttempts, observer, PassMode.MAIN);
    }

    /**
     * @param mode {@link PassMode#DEAD_LETTER} in a level-3 pass: decided by {@code l3} only; the delivery count on the
     *             DLQ and {@code holdAttempts} do not count on the delayed level, which a pass does not have
     */
    public Execution process(ReceivedMessage message, String destination, int holdAttempts,
                             ConsumerObserver observer, PassMode mode) {
        boolean pass = mode == PassMode.DEAD_LETTER;
        AttemptState read = AttemptState.from(message.attributes().get(AttemptState.ATTRIBUTE),
                pass ? 0 : message.deliveryCount().orElse(0));
        AttemptState state = pass ? read
                : new AttemptState(read.delayed() + holdAttempts, read.deadLetter(), read.redrive());
        Delivery delivery = pass ? new Delivery(Delivery.Level.DEAD_LETTER, state.deadLetter() + 1) : Delivery.of(state);
        TypedMessage<T> typed;
        try {
            InboxMessage inbox = reader.read(message, destination).withDelivery(delivery);
            typed = typed(inbox);
        } catch (PayloadDeserializationException e) {
            LOG.log(System.Logger.Level.WARNING, "Consumer {0}: message {1} cannot be read, sent to failed: {2}",
                    consumer, message.messageId(), e.getMessage());
            return new Execution(new Outcome.Failed(Outcome.FailureReason.DESERIALIZATION, state, e, null), null);
        }
        InboxMessage inbox = typed.message();
        Outcome outcome = observer.execution(inbox, () -> guard == null
                ? run(typed, state, observer, pass)
                : guard.apply(inbox, state, () -> run(typed, state, observer, pass), e -> storeFailure(e, state, pass)));
        return new Execution(outcome, inbox);
    }

    /** A store failure is a retryable failure that skips the immediate level: delayed level, then DLQ. */
    private Outcome storeFailure(RuntimeException e, AttemptState state, boolean pass) {
        var result = new ImmediateRetrier.Result(null, e, 1, Classification.SKIP_IMMEDIATE);
        return toOutcome(pass ? resolver.resolveDeadLetterPass(result, state) : resolver.resolve(result, state), null,
                state, null);
    }

    private Outcome run(TypedMessage<T> typed, AttemptState state, ConsumerObserver observer, boolean pass) {
        List<Throwable> failures = new ArrayList<>();
        ImmediateRetrier.Result result;
        try {
            result = retrier.execute(() -> {
                try {
                    return handler.handle(typed);
                } catch (Exception e) {
                    failures.add(e);
                    throw e;
                }
            });
        } catch (InterruptedException e) {
            return Outcome.RELEASE;
        }
        for (int i = 0; i < result.executions() - 1; i++) {
            observer.immediateRetry(typed.message(), failures.get(i));
        }
        RetryDecision decision = pass ? resolver.resolveDeadLetterPass(result, state) : resolver.resolve(result, state);
        return toOutcome(decision, result.result(), state, typed.message().id());
    }

    private Outcome toOutcome(RetryDecision decision, HandlerResult handlerResult, AttemptState state, String id) {
        return switch (decision) {
            case RetryDecision.Ack a -> new Outcome.Ack(Outcome.AckReason.SUCCESS);
            case RetryDecision.Discard d -> {
                LOG.log(System.Logger.Level.INFO, "Consumer {0}: message {1} discarded: {2}", consumer, id, d.reason());
                yield new Outcome.Ack(Outcome.AckReason.DISCARDED);
            }
            case RetryDecision.Delay d -> new Outcome.Retry(d.delay(), d.next(), d.cause());
            case RetryDecision.DeadLetter d -> new Outcome.DeadLetter(d.next(), d.cause());
            case RetryDecision.Failed f -> {
                String detail = null;
                if (handlerResult instanceof HandlerResult.Reject reject) {
                    detail = reject.reason();
                    LOG.log(System.Logger.Level.INFO, "Consumer {0}: message {1} rejected: {2}", consumer, id, detail);
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
