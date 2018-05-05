package com.javahelps.wisdom.core.operand;

import java.util.ArrayList;
import java.util.List;

public class FloatWisdomArray extends WisdomArray {

    private float[] array;

    public FloatWisdomArray(float... items) {
        this(toList(items), items);
        this.array = items;
    }

    public FloatWisdomArray(List<Object> original, float... items) {
        super(original);
        this.array = items;
    }

    @Override
    public Object toArray() {
        return this.array;
    }

    private static List<Object> toList(float... items) {
        List<Object> list = new ArrayList<>(items.length);
        for (float val : items) {
            list.add(val);
        }
        return list;
    }
}
