package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.event.Event;

import java.util.List;

/**
 * A pattern that can optionally be empty.
 */
public interface EmptiablePattern {

    Event EMPTY_EVENT = new Event(-1);

    List<Event> getEvents(boolean isFirst);
}
