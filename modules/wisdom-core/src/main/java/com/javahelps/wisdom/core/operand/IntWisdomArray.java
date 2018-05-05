package com.javahelps.wisdom.core.operand;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IntWisdomArray extends WisdomArray {

    private int[] array;

    public IntWisdomArray(int... items) {
        this(Arrays.stream(items).boxed().collect(Collectors.toList()), items);
        this.array = items;
    }

    public IntWisdomArray(List<Object> original, int... items) {
        super(original);
        this.array = items;
    }

    @Override
    public Object toArray() {
        return this.array;
    }
}