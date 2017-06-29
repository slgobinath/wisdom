package com.javahelps.wisdom.core.util;

import com.javahelps.wisdom.core.event.Event;

import java.util.Arrays;

/**
 * A utility to format and print the {@link Event}s.
 */
public class EventPrinter {

    private EventPrinter() {

    }

    public static void print(Event... events) {
        System.out.println(Arrays.toString(events));
    }
}
