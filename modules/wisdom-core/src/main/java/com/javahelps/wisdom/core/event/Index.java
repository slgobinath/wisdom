package com.javahelps.wisdom.core.event;

public class Index {

    private int[] indices;

    Index(int... indices) {
        this.indices = indices;
    }

//    public static Index of(int index) {
//        if (index < 0) {
//            throw new WisdomAppValidationException("Index cannot be negative but found %d", index);
//        }
//        return new Index(index);
//    }
}
