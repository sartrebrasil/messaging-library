package com.example.messaging.inbox.spring;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;

/**
 * Where consumers find their destinations by name. The default reads {@code messaging.destinations} through the
 * messaging-lib starter; a bean of this type replaces it (tests use one over {@code InMemoryMessaging}).
 */
public interface InboxDestinations {

    /** A new receiver per call; the inbox closes it. */
    MessageReceiver receiver(String name);

    /** Shared sender; the inbox never closes it. */
    MessageSender sender(String name);
}
