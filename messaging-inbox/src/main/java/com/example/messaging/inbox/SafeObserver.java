package com.example.messaging.inbox;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Runs a {@link ConsumerObserver} so that its failures never reach the message. */
final class SafeObserver implements ConsumerObserver {

    private static final System.Logger LOG = System.getLogger(SafeObserver.class.getName());

    private final ConsumerObserver delegate;

    SafeObserver(ConsumerObserver delegate) {
        this.delegate = delegate;
    }

    @Override
    public Outcome execution(InboxMessage message, Supplier<Outcome> execution) {
        AtomicBoolean started = new AtomicBoolean();
        AtomicReference<Outcome> result = new AtomicReference<>();
        Supplier<Outcome> once = () -> {
            if (started.compareAndSet(false, true)) {
                result.set(execution.get());
            }
            return result.get();
        };
        try {
            delegate.execution(message, once);
        } catch (RuntimeException e) {
            if (started.get() && result.get() == null) {
                throw e; // the execution itself failed: not the observer's doing
            }
            warn(e);
        }
        return started.get() ? result.get() : once.get();
    }

    @Override
    public void received(com.example.messaging.ReceivedMessage message) {
        run(() -> delegate.received(message));
    }

    @Override
    public void immediateRetry(InboxMessage message, Throwable failure) {
        run(() -> delegate.immediateRetry(message, failure));
    }

    @Override
    public void held(InboxMessage message, Outcome.Retry retry) {
        run(() -> delegate.held(message, retry));
    }

    @Override
    public void delivered(Report report) {
        run(() -> delegate.delivered(report));
    }

    @Override
    public void receiveFailed(RuntimeException failure) {
        run(() -> delegate.receiveFailed(failure));
    }

    @Override
    public void passEnded(PassEnd end, int processed) {
        run(() -> delegate.passEnded(end, processed));
    }

    private static void run(Runnable call) {
        try {
            call.run();
        } catch (RuntimeException e) {
            warn(e);
        }
    }

    private static void warn(RuntimeException e) {
        LOG.log(System.Logger.Level.WARNING, "consumer observer failed; the message is not affected", e);
    }
}
