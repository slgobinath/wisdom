package com.javahelps.wisdom.core.operand;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DoubleWisdomArray extends WisdomArray {

    private double[] array;

    public DoubleWisdomArray(double... items) {
        this(Arrays.stream(items).boxed().collect(Collectors.toList()), items);
        this.array = items;
    }

    public DoubleWisdomArray(List<Object> original, double... items) {
        super(original);
        this.array = items;
    }

    @Override
    public Object toArray() {
        return this.array;
    }
}
