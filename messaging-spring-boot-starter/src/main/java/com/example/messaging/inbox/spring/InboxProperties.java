package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.EnvelopeMode;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.spring.MessagingProperties;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code messaging.receivers.<name>} and {@code messaging.receiver-defaults} (ADR-0008). Every option is nullable: {@code null} means "not set", and {@link InboxConfig} merges receiver,
 * defaults and the library defaults after binding. Unknown keys fail the startup. {@code messaging.providers} and
 * {@code messaging.senders} belong to {@link MessagingProperties}.
 */
public class InboxProperties {

    /**
     * Binds each root on its own, strictly: an unknown key under a receiver fails, while the other
     * {@code messaging.*} roots are not looked at. {@code messaging.receivers-enabled} is the condition of
     * {@link InboxAutoConfiguration}; leftover {@code inbox.*} keys fail in {@link MessagingProperties#bind}.
     */
    static InboxProperties bind(Binder binder) {
        BindHandler strict = new NoUnboundElementsBindHandler(BindHandler.DEFAULT);
        InboxProperties properties = new InboxProperties();
        binder.bind("messaging.receiver-defaults", Bindable.of(ConsumerOptions.class), strict)
                .ifBound(properties::setDefaults);
        binder.bind("messaging.receivers", Bindable.mapOf(String.class, ConsumerOptions.class), strict)
                .ifBound(properties::setConsumers);
        return properties;
    }

    /** {@code messaging.receiver-defaults}: options shared by every receiver. */
    private ConsumerOptions defaults = new ConsumerOptions();

    /** {@code messaging.receivers}. */
    private Map<String, ConsumerOptions> consumers = new LinkedHashMap<>();

    public ConsumerOptions getDefaults() {
        return defaults;
    }

    public void setDefaults(ConsumerOptions defaults) {
        this.defaults = defaults;
    }

    public Map<String, ConsumerOptions> getConsumers() {
        return consumers;
    }

    public void setConsumers(Map<String, ConsumerOptions> consumers) {
        this.consumers = consumers;
    }

    /** One receiver, or {@code receiver-defaults}. Addresses follow {@link MessagingProperties.Endpoint}. */
    public static class ConsumerOptions {

        /** {@code false}: validated at startup, never started. */
        private Boolean enabled;
        /** Name in messaging.providers; optional with a single provider. */
        private String provider;
        private String destination;
        /** Receive from a topic through this subscription. */
        private String subscription;
        private MessagingProperties.Options options;
        private String deadLetter;
        private String failed;
        private Integer concurrency;
        private Ordering ordering;
        private EnvelopeMode envelope;
        private Retry retry;
        private Idempotency idempotency;
        private Integer maxMessagesPerPoll;
        private Duration pollWait;
        private Duration maxProcessing;
        private Duration shutdownTimeout;

        // Accepted by the ADRs but not built yet: present only to fail with a clear message
        private Map<String, Object> staleCheck;
        private Map<String, Object> expiration;
        private Map<String, Object> circuitBreaker;
        private Map<String, Object> batch;
        /** Lanes processing one receive in parallel, by orderingKey (ADR-0016); 1 = in sequence. */
        private Integer keyLanes;

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public String getSubscription() {
            return subscription;
        }

        public void setSubscription(String subscription) {
            this.subscription = subscription;
        }

        public MessagingProperties.Options getOptions() {
            return options;
        }

        public void setOptions(MessagingProperties.Options options) {
            this.options = options;
        }

        public Integer getMaxMessagesPerPoll() {
            return maxMessagesPerPoll;
        }

        public void setMaxMessagesPerPoll(Integer maxMessagesPerPoll) {
            this.maxMessagesPerPoll = maxMessagesPerPoll;
        }

        public Duration getPollWait() {
            return pollWait;
        }

        public void setPollWait(Duration pollWait) {
            this.pollWait = pollWait;
        }

        public Duration getMaxProcessing() {
            return maxProcessing;
        }

        public void setMaxProcessing(Duration maxProcessing) {
            this.maxProcessing = maxProcessing;
        }

        public Duration getShutdownTimeout() {
            return shutdownTimeout;
        }

        public void setShutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public String getDeadLetter() {
            return deadLetter;
        }

        public void setDeadLetter(String deadLetter) {
            this.deadLetter = deadLetter;
        }

        public String getFailed() {
            return failed;
        }

        public void setFailed(String failed) {
            this.failed = failed;
        }

        public Integer getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(Integer concurrency) {
            this.concurrency = concurrency;
        }

        public Ordering getOrdering() {
            return ordering;
        }

        public void setOrdering(Ordering ordering) {
            this.ordering = ordering;
        }

        public EnvelopeMode getEnvelope() {
            return envelope;
        }

        public void setEnvelope(EnvelopeMode envelope) {
            this.envelope = envelope;
        }

        public Retry getRetry() {
            return retry;
        }

        public void setRetry(Retry retry) {
            this.retry = retry;
        }

        public Idempotency getIdempotency() {
            return idempotency;
        }

        public void setIdempotency(Idempotency idempotency) {
            this.idempotency = idempotency;
        }

        public Map<String, Object> getStaleCheck() {
            return staleCheck;
        }

        public void setStaleCheck(Map<String, Object> staleCheck) {
            this.staleCheck = staleCheck;
        }

        public Map<String, Object> getExpiration() {
            return expiration;
        }

        public void setExpiration(Map<String, Object> expiration) {
            this.expiration = expiration;
        }

        public Map<String, Object> getCircuitBreaker() {
            return circuitBreaker;
        }

        public void setCircuitBreaker(Map<String, Object> circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        public Map<String, Object> getBatch() {
            return batch;
        }

        public void setBatch(Map<String, Object> batch) {
            this.batch = batch;
        }

        public Integer getKeyLanes() {
            return keyLanes;
        }

        public void setKeyLanes(Integer keyLanes) {
            this.keyLanes = keyLanes;
        }
    }

    public static class Retry {

        private Backoff immediate;
        private Delayed delayed;
        /** Level 3: not built yet (F5). */
        private Map<String, Object> deadLetter;
        private List<String> nonRetryable;
        private List<String> skipImmediate;
        /** Not empty: allowlist mode (ADR-0014). */
        private List<String> retryable;
        /** Put the exception message in the dead_letter attribute of copies. */
        private Boolean includeExceptionMessage;

        public Backoff getImmediate() {
            return immediate;
        }

        public void setImmediate(Backoff immediate) {
            this.immediate = immediate;
        }

        public Delayed getDelayed() {
            return delayed;
        }

        public void setDelayed(Delayed delayed) {
            this.delayed = delayed;
        }

        public Map<String, Object> getDeadLetter() {
            return deadLetter;
        }

        public void setDeadLetter(Map<String, Object> deadLetter) {
            this.deadLetter = deadLetter;
        }

        public List<String> getNonRetryable() {
            return nonRetryable;
        }

        public void setNonRetryable(List<String> nonRetryable) {
            this.nonRetryable = nonRetryable;
        }

        public List<String> getSkipImmediate() {
            return skipImmediate;
        }

        public void setSkipImmediate(List<String> skipImmediate) {
            this.skipImmediate = skipImmediate;
        }

        public List<String> getRetryable() {
            return retryable;
        }

        public void setRetryable(List<String> retryable) {
            this.retryable = retryable;
        }

        public Boolean getIncludeExceptionMessage() {
            return includeExceptionMessage;
        }

        public void setIncludeExceptionMessage(Boolean includeExceptionMessage) {
            this.includeExceptionMessage = includeExceptionMessage;
        }
    }

    public static class Backoff {

        private Integer maxAttempts;
        private Duration initial;
        private Double multiplier;
        private Duration max;
        private Double jitter;

        public Integer getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Duration getInitial() {
            return initial;
        }

        public void setInitial(Duration initial) {
            this.initial = initial;
        }

        public Double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(Double multiplier) {
            this.multiplier = multiplier;
        }

        public Duration getMax() {
            return max;
        }

        public void setMax(Duration max) {
            this.max = max;
        }

        public Double getJitter() {
            return jitter;
        }

        public void setJitter(Double jitter) {
            this.jitter = jitter;
        }
    }

    public static class Delayed extends Backoff {

        private DelayedStrategy strategy;
        private List<Queue> queues;

        public DelayedStrategy getStrategy() {
            return strategy;
        }

        public void setStrategy(DelayedStrategy strategy) {
            this.strategy = strategy;
        }

        public List<Queue> getQueues() {
            return queues;
        }

        public void setQueues(List<Queue> queues) {
            this.queues = queues;
        }
    }

    /** A band of retry-queues: its address (in the provider of the receiver) and fixed delay. */
    public static class Queue {

        private Duration delay;
        private String destination;

        public Duration getDelay() {
            return delay;
        }

        public void setDelay(Duration delay) {
            this.delay = delay;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }
    }

    public static class Idempotency {

        private Boolean enabled;
        /** Completed records deduplicate for this long (default P7D). */
        private Duration ttl;
        /** IN_PROGRESS reservations live this long; default max-processing plus the hold delays plus 5 min. */
        private Duration reservationTtl;

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }

        public Duration getReservationTtl() {
            return reservationTtl;
        }

        public void setReservationTtl(Duration reservationTtl) {
            this.reservationTtl = reservationTtl;
        }
    }
}
