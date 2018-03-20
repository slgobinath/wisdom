package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.event.Event;

/**
 * {@link StreamCallback} is provided for the users to receive the events sent to a stream.
 */
@FunctionalInterface
public interface StreamCallback {

    void receive(Event... events);
}
