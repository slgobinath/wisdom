package com.javahelps.wisdom.extensions.ml.quantizer;

import com.javahelps.wisdom.core.event.Event;

import java.util.function.Function;

public abstract class Quantizer implements Function<Event, Event> {

    public static <T extends Comparable, R extends Comparable> PredicateQuantizer<T, R> predicate(String attribute, R nullMapping) {
        return new PredicateQuantizer(attribute, nullMapping);
    }
}
