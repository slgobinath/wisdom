package com.javahelps.wisdom.extensions.ml.quantizer;

import com.javahelps.wisdom.core.event.Event;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class PredicateQuantizer<T extends Comparable, R extends Comparable> extends Quantizer {

    private final String attribute;
    private List<Predicate<T>> predicates = new LinkedList<>();
    private List<R> mappedValues = new LinkedList<>();
    private String newName;
    private boolean removeOld;

    PredicateQuantizer(String attribute, R nullMapping) {
        Objects.requireNonNull(attribute, "Attribute cannot be null");
        Objects.requireNonNull(nullMapping, "Null mapping cannot be null");
        this.attribute = attribute;
        this.addPredicate(val -> val == null, nullMapping);
    }

    public PredicateQuantizer<T, R> as(String newName, boolean removeOld) {
        if (this.newName != null) {
            throw new IllegalStateException("Rename method called twice");
        }
        this.newName = newName;
        this.removeOld = removeOld;
        return this;
    }

    public PredicateQuantizer<T, R> greaterThan(final T lowerBound, final R to) {
        this.addPredicate(val -> val.compareTo(lowerBound) >= 0, to);
        return this;
    }

    public PredicateQuantizer<T, R> lessThan(final T upperBound, final R to) {
        this.addPredicate(val -> val.compareTo(upperBound) < 0, to);
        return this;
    }

    public PredicateQuantizer<T, R> within(final T lowerBound, final T upperBound, final R to) {
        this.addPredicate(val -> val.compareTo(lowerBound) >= 0 && val.compareTo(upperBound) < 0, to);
        return this;
    }

    public PredicateQuantizer<T, R> within(final T[] values, final R to) {
        final List<T> bucket = Arrays.asList(values);
        this.addPredicate(val -> bucket.contains(val), to);
        return this;
    }

    public PredicateQuantizer<T, R> satisfies(final Predicate<T> condition, final R to) {
        Objects.requireNonNull(condition, "Condition cannot be null");
        this.addPredicate(condition, to);
        return this;
    }

    @Override
    public Event apply(Event event) {
        T object = (T) event.get(this.attribute);
        int index = 0;
        for (Predicate<T> predicate : this.predicates) {
            if (predicate.test(object)) {
                R mappedValue = this.mappedValues.get(index);
                if (this.newName == null) {
                    event.set(this.attribute, mappedValue);
                } else {
                    // Require rename
                    event.set(this.newName, mappedValue);
                    if (this.removeOld) {
                        event.remove(this.attribute);
                    }
                }
                break;
            }
            index++;
        }
        return event;
    }

    private void addPredicate(Predicate<T> predicate, R value) {
        this.predicates.add(predicate);
        this.mappedValues.add(value);
    }
}
