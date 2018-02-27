package com.javahelps.wisdom.core.operand;

import java.util.Comparator;

public class WisdomReference<T extends Comparable> {

    private T reference;

    public WisdomReference() {
        this(null);
    }

    public WisdomReference(T initialReference) {
        this.reference = initialReference;
    }

    public T set(T newReference) {
        this.reference = newReference;
        return this.reference;
    }

    public T setIfLess(Comparator<T> comparator, T newReference) {
        if (comparator.compare(newReference, this.reference) < 0) {
            this.reference = newReference;
        }
        return this.reference;
    }

    public T setIfGreater(Comparator<T> comparator, T newReference) {
        if (comparator.compare(newReference, this.reference) > 0) {
            this.reference = newReference;
        }
        return this.reference;
    }

    public T get() {
        return this.reference;
    }
}
