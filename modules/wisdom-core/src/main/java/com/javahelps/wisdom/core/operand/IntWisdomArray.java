package com.javahelps.wisdom.core.operand;

import java.util.Arrays;
import java.util.stream.Collectors;

public class IntWisdomArray extends WisdomArray {

    private int[] array;

    public IntWisdomArray(int... items) {
        super(Arrays.stream(items).boxed().collect(Collectors.toList()));
        this.array = items;
    }

    @Override
    public Object toArray() {
        return this.array;
    }
}
