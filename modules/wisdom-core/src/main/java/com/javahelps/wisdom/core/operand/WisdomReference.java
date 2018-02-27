package com.javahelps.wisdom.core.operand;

import java.util.Comparator;

public class WisdomReference<T> {

    private T reference;

    public WisdomReference() {
        this(null);
    }

    public WisdomReference(T initialReference) {
        this.reference = initialReference;
    }

    public void set(T newReference) {
        this.reference = newReference;
    }

    public void setIfLess(Comparator<T> comparator, T newReference) {
        if (comparator.compare(newReference, this.reference) < 0) {
            this.reference = newReference;
        }
    }

    public void setIfGreater(Comparator<T> comparator, T newReference) {
        if (comparator.compare(newReference, this.reference) > 0) {
            this.reference = newReference;
        }
    }

    public T get() {
        return this.reference;
    }
}
