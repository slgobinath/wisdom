package com.javahelps.wisdom.core.operand;

public class WisdomLong {

    private transient long value;

    public WisdomLong() {
        this(0);
    }

    public WisdomLong(long initialValue) {
        this.value = initialValue;
    }

    public long addAndGet(long increment) {
        this.value += increment;
        return this.value;
    }

    public long incrementAndGet() {
        return ++this.value;
    }

    public long set(long val) {
        this.value = val;
        return this.value;
    }
}
