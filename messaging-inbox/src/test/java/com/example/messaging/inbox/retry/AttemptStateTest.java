package com.example.messaging.inbox.retry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AttemptStateTest {

    @Test
    void attributeRoundTrip() {
        var state = new AttemptState(2, 1);
        assertEquals("l2=2;l3=1", state.toAttribute());
        assertEquals(state, AttemptState.from(state.toAttribute(), 0));
    }

    @Test
    void deliveryCountAddsToAttribute() {
        assertEquals(new AttemptState(4, 0), AttemptState.from("l2=2;l3=0", 3));
        assertEquals(new AttemptState(6, 0), AttemptState.from(null, 7));
        assertEquals(AttemptState.initial(), AttemptState.from(null, -1));
    }

    @Test
    void redriveMarkFollowsEveryLaterCopy() {
        var redriven = AttemptState.redriven("r-42");
        assertEquals("l2=0;l3=0;redrive=r-42", redriven.toAttribute());

        var later = AttemptState.from(redriven.toAttribute(), 3).nextDelayed().nextDeadLetter();
        assertEquals(new AttemptState(3, 1, "r-42"), later);
        assertEquals("l2=3;l3=1;redrive=r-42", later.toAttribute());
    }

    @Test
    void unreadableAttributeCountsAsZero() {
        assertEquals(new AttemptState(0, 1), AttemptState.from("l2=x;l3=1;garbage", 0));
    }
}
