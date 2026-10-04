package com.example.messaging.spi;

import com.example.messaging.KeepAlive;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.ReceivedMessage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LeaseKeeperTest {

    private static final Duration LEASE = Duration.ofMillis(300);

    private static ReceivedMessage received() {
        Instant now = Instant.now();
        return new ReceivedMessage("id", new byte[]{1}, Map.of(), null, null, OptionalInt.of(1), now, now,
                now.plus(LEASE), null, null, new Object());
    }

    @Test
    void renewsBeforeLeaseExpiresUntilClosed() throws InterruptedException {
        AtomicInteger renewals = new AtomicInteger();
        try (LeaseKeeper keeper = new LeaseKeeper((message, lease) -> {
            assertEquals(LEASE, lease);
            renewals.incrementAndGet();
        })) {
            KeepAlive keepAlive = keeper.keep(received(), Duration.ofMinutes(1));
            Thread.sleep(LEASE.multipliedBy(3).toMillis());
            keepAlive.close();
            int afterClose = renewals.get();
            Thread.sleep(LEASE.multipliedBy(2).toMillis());

            assertTrue(afterClose >= 3, "renovações: " + afterClose);
            assertEquals(afterClose, renewals.get());
            assertFalse(keepAlive.lost());
        }
    }

    @Test
    void stopsRenewingAtMaxTotal() throws InterruptedException {
        AtomicInteger renewals = new AtomicInteger();
        try (LeaseKeeper keeper = new LeaseKeeper((message, lease) -> renewals.incrementAndGet())) {
            keeper.keep(received(), LEASE);
            Thread.sleep(LEASE.multipliedBy(4).toMillis());

            // renova aos 2/3 do lease; a próxima cairia depois de maxTotal
            assertEquals(1, renewals.get());
        }
    }

    @Test
    void failedRenewalMarksLeaseLost() throws InterruptedException {
        AtomicInteger renewals = new AtomicInteger();
        try (LeaseKeeper keeper = new LeaseKeeper((message, lease) -> {
            renewals.incrementAndGet();
            throw new LeaseExpiredException("memory", "perdido", null);
        })) {
            KeepAlive keepAlive = keeper.keep(received(), Duration.ofMinutes(1));
            Thread.sleep(LEASE.multipliedBy(3).toMillis());

            assertTrue(keepAlive.lost());
            assertEquals(1, renewals.get());
            assertDoesNotThrow(keepAlive::close);
        }
    }

    @Test
    void releaseStopsRenewal() throws InterruptedException {
        AtomicInteger renewals = new AtomicInteger();
        try (LeaseKeeper keeper = new LeaseKeeper((message, lease) -> renewals.incrementAndGet())) {
            ReceivedMessage message = received();
            keeper.keep(message, Duration.ofMinutes(1));
            keeper.release(message);
            Thread.sleep(LEASE.multipliedBy(2).toMillis());

            assertEquals(0, renewals.get());
        }
    }

    @Test
    void rejectsInvalidArgumentsAndUseAfterClose() {
        LeaseKeeper keeper = new LeaseKeeper((message, lease) -> {
        });
        assertThrows(IllegalArgumentException.class, () -> keeper.keep(received(), Duration.ZERO));
        Instant now = Instant.now();
        ReceivedMessage noLease = new ReceivedMessage("id", new byte[]{1}, Map.of(), null, null, OptionalInt.empty(),
                now, now, now, null, null, null);
        assertThrows(IllegalArgumentException.class, () -> keeper.keep(noLease, Duration.ofMinutes(1)));

        keeper.close();
        assertThrows(IllegalStateException.class, () -> keeper.keep(received(), Duration.ofMinutes(1)));
    }
}
