package com.javahelps.wisdom.core.operand;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LongWisdomArray extends WisdomArray {

    private long[] array;

    public LongWisdomArray(long... items) {
        this(Arrays.stream(items).boxed().collect(Collectors.toList()), items);
        this.array = items;
    }

    public LongWisdomArray(List<Object> original, long... items) {
        super(original);
        this.array = items;
    }

    @Override
    public Object toArray() {
        return this.array;
    }
}
